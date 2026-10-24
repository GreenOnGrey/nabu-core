package space

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/sandboxprofile"
)

// SandboxConfig configures sandbox pods (tech §7).
type SandboxConfig struct {
	Namespace   string
	Image       string
	CPU, Memory string
	Quota       int64
	IdleTimeout time.Duration
	// RelayURL is the WebSocket of the relay inside the cluster; APIURL the
	// internal API for file sync.
	RelayURL, APIURL string
	Exclude          []string
	TokenTTL         time.Duration
	// FTR.NAB.CMN-0008: the image of the init container with the nabu binary
	// and the imagePullSecrets of sandboxes (built-in and custom registries).
	InitImage   string
	PullSecrets []string
}

// Sandboxes starts personal sandboxes on demand and puts them to sleep after
// idle (R7: the space sleeps when idle and comes up on access).
type Sandboxes struct {
	Pool   *pgxpool.Pool
	K8s    *k8s.Client
	Space  *Service
	Events events.Publisher
	Cfg    SandboxConfig
	// Profiles chooses the image of a sandbox (FTR.NAB.CMN-0008 R6); nil —
	// the single image of Cfg.Image.
	Profiles *sandboxprofile.Service
}

// profile is the profile a sandbox of the target starts from.
func (m *Sandboxes) profile(ctx context.Context, t sandboxprofile.Target) (sandboxprofile.Profile, error) {
	if m.Profiles == nil {
		return sandboxprofile.Profile{Name: "base", Kind: sandboxprofile.KindBuiltin, Image: m.Cfg.Image, Legacy: true}, nil
	}
	if t.Kind == "" {
		k, err := m.Profiles.KindOf(ctx, t.UserID)
		if err != nil {
			return sandboxprofile.Profile{}, err
		}
		t.Kind = k
	}
	return m.Profiles.Resolve(ctx, t)
}

// PodName is the pod of a user's sandbox.
func PodName(uid uuid.UUID) string {
	return "sandbox-" + strings.ReplaceAll(uid.String(), "-", "")[:20]
}

// Ensure starts the sandbox of a user unless it runs or starts (the agent
// calls the space through the relay, which waits for the connection).
func (m *Sandboxes) Ensure(ctx context.Context, uid uuid.UUID) error {
	if m == nil || m.K8s == nil {
		return nil
	}
	if _, err := m.Space.Info(ctx, uid); err != nil { // ensures the row
		return err
	}
	prof, err := m.profile(ctx, sandboxprofile.Target{UserID: uid})
	if err != nil {
		return err
	}
	tag, err := m.Pool.Exec(ctx, `UPDATE spaces SET state = 'starting', pod = $2, profile = $3, state_changed_at = now(), last_activity_at = now()
		WHERE user_id = $1 AND state IN ('sleeping','stopping')`, uid, PodName(uid), prof.Name)
	if err != nil {
		return err
	}
	_, _ = m.Pool.Exec(ctx, `UPDATE spaces SET last_activity_at = now() WHERE user_id = $1`, uid)
	if tag.RowsAffected() == 0 {
		return nil
	}
	m.publish(ctx, uid)
	// A previous pod may still be terminating; it goes away before the new one starts.
	if p, err := m.K8s.GetPod(ctx, m.Cfg.Namespace, PodName(uid)); err == nil && p != nil {
		_ = m.K8s.DeletePod(ctx, m.Cfg.Namespace, PodName(uid), 0)
		for i := 0; i < 30; i++ {
			if p, _ := m.K8s.GetPod(ctx, m.Cfg.Namespace, PodName(uid)); p == nil {
				break
			}
			time.Sleep(time.Second)
		}
	}
	err = m.K8s.CreatePod(ctx, m.Cfg.Namespace, m.manifest(uid, prof))
	if err != nil && !k8s.Conflict(err) {
		_, _ = m.Pool.Exec(ctx, `UPDATE spaces SET state = 'sleeping', pod = NULL, state_changed_at = now() WHERE user_id = $1`, uid)
		m.publish(ctx, uid)
		return err
	}
	slog.InfoContext(ctx, "sandbox starting", "user_id", uid)
	return nil
}

func (m *Sandboxes) publish(ctx context.Context, uid uuid.UUID) {
	if m.Space != nil {
		m.Space.changed(ctx, uid)
	}
}

// manifest is the pod of a sandbox: non-root, read-only root, no service
// account token, a size-limited emptyDir for the working copy, no S3
// credentials — only its own token (SPC-04). The image is the profile's; the
// nabu binary comes from the init container, so any Linux image with bash
// runs (FTR.NAB.CMN-0008 tech §5.3).
func (m *Sandboxes) manifest(uid uuid.UUID, p sandboxprofile.Profile) map[string]any {
	work := m.Cfg.Quota + (1 << 30)
	env := []map[string]any{
		{"name": "NABU_SANDBOX_USER", "value": uid.String()},
		{"name": "NABU_SANDBOX_TOKEN", "value": m.Space.SandboxToken(uid, m.Cfg.TokenTTL)},
		{"name": "NABU_WORKSPACE_TOKEN", "value": m.Space.WorkspaceToken(uid, m.Cfg.TokenTTL)},
		{"name": "NABU_RELAY_URL", "value": m.Cfg.RelayURL},
		{"name": "NABU_API_INTERNAL_URL", "value": m.Cfg.APIURL},
		{"name": "NABU_SANDBOX_EXCLUDE", "value": strings.Join(m.Cfg.Exclude, ",")},
		{"name": "HOME", "value": "/work"},
		{"name": "NABU_SANDBOX_PROFILE", "value": p.Name},
	}
	secrets := m.Cfg.PullSecrets
	if len(secrets) == 0 {
		secrets = []string{"ghcr-pull"}
	}
	c := map[string]any{
		"name": "sandbox", "image": p.Image, "args": []string{"sandbox"}, "env": env, "workingDir": "/work",
		"resources": map[string]any{
			"requests": map[string]string{"cpu": "50m", "memory": "64Mi"},
			"limits":   map[string]string{"cpu": m.Cfg.CPU, "memory": m.Cfg.Memory, "ephemeral-storage": formatBytes(work + (512 << 20))},
		},
		"securityContext": sandboxprofile.ContainerSecurity(),
		"volumeMounts":    []map[string]any{{"name": "work", "mountPath": "/work"}, {"name": "tmp", "mountPath": "/tmp"}},
	}
	spec := map[string]any{
		"automountServiceAccountToken":  false,
		"restartPolicy":                 "Always",
		"terminationGracePeriodSeconds": 90,
		"imagePullSecrets":              sandboxprofile.PullSecrets(secrets),
		"securityContext":               sandboxprofile.PodSecurity(),
		"containers":                    []map[string]any{c},
		"volumes": []map[string]any{
			{"name": "work", "emptyDir": map[string]any{"sizeLimit": formatBytes(work)}},
			{"name": "tmp", "emptyDir": map[string]any{"sizeLimit": "512Mi"}},
		},
	}
	if !p.Legacy {
		c["command"] = []string{sandboxprofile.NabuDir + "/bin/nabu"}
		c["volumeMounts"] = append(c["volumeMounts"].([]map[string]any), sandboxprofile.NabuMount())
		spec["volumes"] = append(spec["volumes"].([]map[string]any), sandboxprofile.NabuVolume())
		spec["initContainers"] = []map[string]any{sandboxprofile.InitContainer(m.Cfg.InitImage, []string{"sandbox-init"}, []map[string]any{
			{"name": "NABU_SANDBOX_PROFILE", "value": p.Name},
			{"name": "NABU_SANDBOX_TOKEN", "value": m.Space.SandboxToken(uid, m.Cfg.TokenTTL)},
			{"name": "NABU_API_INTERNAL_URL", "value": m.Cfg.APIURL},
		})}
	}
	return map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": PodName(uid), "namespace": m.Cfg.Namespace,
			"labels": map[string]string{"app.kubernetes.io/name": "nabu", "app.kubernetes.io/component": "sandbox",
				"nabu.io/user": uid.String(), "nabu.io/profile": p.Name},
			"annotations": map[string]string{"nabu.io/image": p.Image}},
		"spec": spec,
	}
}

func formatBytes(n int64) string { return itoa(n/(1<<20)) + "Mi" }

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// Reap puts idle sandboxes to sleep and repairs states (every minute).
func (m *Sandboxes) Reap(ctx context.Context) {
	if m == nil || m.K8s == nil {
		return
	}
	rows, err := m.Pool.Query(ctx, `SELECT user_id FROM spaces WHERE state IN ('running','starting')
		AND (last_activity_at IS NULL OR last_activity_at < now() - make_interval(secs => $1))`, m.Cfg.IdleTimeout.Seconds())
	if err != nil {
		slog.Error("sandbox reaper", "err", err)
		return
	}
	var idle []uuid.UUID
	for rows.Next() {
		var u uuid.UUID
		if rows.Scan(&u) == nil {
			idle = append(idle, u)
		}
	}
	rows.Close()
	for _, uid := range idle {
		_, _ = m.Pool.Exec(ctx, `UPDATE spaces SET state = 'stopping', state_changed_at = now() WHERE user_id = $1`, uid)
		if err := m.K8s.DeletePod(ctx, m.Cfg.Namespace, PodName(uid), 90); err != nil {
			slog.Warn("sandbox stop", "user_id", uid, "err", err)
			continue
		}
		slog.Info("sandbox idle, stopping", "user_id", uid)
		m.publish(ctx, uid)
	}
	// stopping → sleeping once the pod is gone; a start that failed (an image that
	// does not pull, a crashing container — at once; anything else after 5
	// minutes) → sleeping, retry later (FTR.NAB.CMN-0008 tech §7.10).
	rows, err = m.Pool.Query(ctx, `SELECT user_id, state, state_changed_at < now() - interval '5 minutes' FROM spaces
		WHERE state IN ('stopping', 'starting')`)
	if err != nil {
		return
	}
	type st struct {
		uid   uuid.UUID
		state string
		stale bool
	}
	var list []st
	for rows.Next() {
		var s st
		if rows.Scan(&s.uid, &s.state, &s.stale) == nil {
			list = append(list, s)
		}
	}
	rows.Close()
	for _, s := range list {
		p, err := m.K8s.GetPod(ctx, m.Cfg.Namespace, PodName(s.uid))
		if err != nil {
			continue
		}
		if s.state == "starting" && p != nil {
			reason := startFailure(p)
			if reason == "" && !s.stale {
				continue
			}
			if reason == "" {
				reason = "timeout"
			}
			prof, kind := p.Metadata.Labels["nabu.io/profile"], sandboxprofile.KindBuiltin
			if m.Profiles != nil && prof != "" {
				_ = m.Pool.QueryRow(ctx, `SELECT kind FROM sandbox_profiles WHERE name = $1`, prof).Scan(&kind)
			}
			metrics.SandboxStartFailures.WithLabelValues(prof, kind, reason).Inc()
			_ = m.K8s.DeletePod(ctx, m.Cfg.Namespace, PodName(s.uid), 0)
		} else if p != nil || (s.state == "starting" && !s.stale) {
			continue
		}
		_, _ = m.Pool.Exec(ctx, `UPDATE spaces SET state = 'sleeping', pod = NULL, state_changed_at = now() WHERE user_id = $1 AND state = $2`, s.uid, s.state)
		m.publish(ctx, s.uid)
	}
	m.restartOnNewImage(ctx)
	var n int
	_ = m.Pool.QueryRow(ctx, `SELECT count(*) FROM spaces WHERE state <> 'sleeping'`).Scan(&n)
	metrics.Sandboxes.Set(float64(n))
}

// startFailure is why a starting pod will not come up by itself: "pull" — the
// image does not pull, "crash" — the container fails; "" — it may still start.
func startFailure(p *k8s.Pod) string {
	for _, st := range append(append([]k8s.ContainerStatus{}, p.Status.InitContainerStatuses...), p.Status.ContainerStatuses...) {
		switch st.Waiting() {
		case "ErrImagePull", "ImagePullBackOff", "InvalidImageName":
			return "pull"
		case "CrashLoopBackOff", "CreateContainerError", "RunContainerError":
			return "crash"
		}
	}
	return ""
}

// restartOnNewImage stops running sandboxes idle for a minute whose profile
// now has another image; the next access starts them on it (R8). The
// heartbeat of a running command keeps last_activity_at fresh.
func (m *Sandboxes) restartOnNewImage(ctx context.Context) {
	if m.Profiles == nil {
		return
	}
	rows, err := m.Pool.Query(ctx, `SELECT user_id FROM spaces WHERE state = 'running' AND last_activity_at < now() - interval '1 minute'`)
	if err != nil {
		return
	}
	var idle []uuid.UUID
	for rows.Next() {
		var u uuid.UUID
		if rows.Scan(&u) == nil {
			idle = append(idle, u)
		}
	}
	rows.Close()
	for _, uid := range idle {
		p, err := m.profile(ctx, sandboxprofile.Target{UserID: uid})
		if err != nil {
			continue
		}
		pod, err := m.K8s.GetPod(ctx, m.Cfg.Namespace, PodName(uid))
		if err != nil || pod == nil || pod.Metadata.Annotations["nabu.io/image"] == p.Image {
			continue
		}
		if pod.Metadata.Annotations["nabu.io/image"] == "" && p.Legacy && pod.Image() == p.Image {
			continue
		}
		_, _ = m.Pool.Exec(ctx, `UPDATE spaces SET state = 'stopping', state_changed_at = now() WHERE user_id = $1 AND state = 'running'`, uid)
		if err := m.K8s.DeletePod(ctx, m.Cfg.Namespace, PodName(uid), 90); err != nil {
			continue
		}
		metrics.SandboxImageRestarts.WithLabelValues(p.Name).Inc()
		slog.Info("sandbox restarts on a new image", "user_id", uid, "profile", p.Name)
		m.publish(ctx, uid)
	}
}

// Stop deletes the sandbox of a user (archiving, FTR.NAB.CMN-0002 R18); the
// pod syncs the files to S3 within the grace period like an idle stop.
func (m *Sandboxes) Stop(ctx context.Context, uid uuid.UUID) error {
	if m == nil || m.K8s == nil {
		return nil
	}
	_, _ = m.Pool.Exec(ctx, `UPDATE spaces SET state = 'stopping', state_changed_at = now() WHERE user_id = $1 AND state <> 'sleeping'`, uid)
	if err := m.K8s.DeletePod(ctx, m.Cfg.Namespace, PodName(uid), 90); err != nil {
		return err
	}
	m.publish(ctx, uid)
	return nil
}

// Run reaps until ctx ends.
func (m *Sandboxes) Run(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Reap(ctx)
		}
	}
}

// StartEphemeral starts a temporary sandbox of a service agent run
// (workspace "nabu", arch §4.3): no files are restored or kept. profile is
// the agent's workspaceProfile, "" — the default of the stand
// (FTR.NAB.CMN-0008 R6).
func (m *Sandboxes) StartEphemeral(ctx context.Context, workspaceID, runID, profile string) error {
	name := ephemeralPod(workspaceID)
	prof, err := m.profile(ctx, sandboxprofile.Target{Kind: sandboxprofile.TargetService, Profile: profile})
	if err != nil {
		return err
	}
	man := m.manifest(uuid.Nil, prof)
	runToken := ""
	if m.Space != nil && m.Space.Signer != nil {
		runToken = sandboxprofile.RunToken(m.Space.Signer, runID, m.Cfg.TokenTTL)
	}
	meta := man["metadata"].(map[string]any)
	meta["name"] = name
	meta["labels"] = map[string]string{"app.kubernetes.io/name": "nabu", "app.kubernetes.io/component": "runbox", "nabu.io/profile": prof.Name}
	spec := man["spec"].(map[string]any)
	spec["restartPolicy"] = "Never"
	c := spec["containers"].([]map[string]any)[0]
	c["env"] = []map[string]any{
		{"name": "NABU_SANDBOX_EPHEMERAL", "value": "1"},
		{"name": "NABU_SANDBOX_WORKSPACE", "value": workspaceID},
		{"name": "NABU_WORKSPACE_TOKEN", "value": m.Space.Signer.Issue(jwt.Claims{Audience: jwt.AudWorkspace, Workspace: workspaceID, Kind: "sandbox"}, m.Cfg.TokenTTL)},
		{"name": "NABU_RELAY_URL", "value": m.Cfg.RelayURL},
		{"name": "NABU_API_INTERNAL_URL", "value": m.Cfg.APIURL},
		{"name": "NABU_SANDBOX_RUN_TOKEN", "value": runToken},
		{"name": "NABU_SANDBOX_PROFILE", "value": prof.Name},
		{"name": "HOME", "value": "/work"},
	}
	if inits, ok := spec["initContainers"].([]map[string]any); ok {
		inits[0]["env"] = []map[string]any{
			{"name": "NABU_SANDBOX_PROFILE", "value": prof.Name},
			{"name": "NABU_SANDBOX_RUN_TOKEN", "value": runToken},
			{"name": "NABU_API_INTERNAL_URL", "value": m.Cfg.APIURL},
		}
	}
	if err := m.K8s.CreatePod(ctx, m.Cfg.Namespace, man); err != nil && !k8s.Conflict(err) {
		return err
	}
	go m.watchEphemeral(context.WithoutCancel(ctx), name, workspaceID, prof)
	return nil
}

// watchEphemeral records how a temporary sandbox started (FTR.NAB.CMN-0008
// R20, tech §7.10): the time until it connects to the relay, or why it did
// not. A run that ends earlier deletes the pod, and the watch ends silently.
func (m *Sandboxes) watchEphemeral(ctx context.Context, pod, workspaceID string, prof sandboxprofile.Profile) {
	const every, limit = time.Second, 5 * time.Minute
	started := time.Now()
	for i := 1; time.Since(started) < limit; i++ {
		time.Sleep(every)
		var connected bool
		_ = m.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM workspace_connections WHERE workspace_id = $1)`, workspaceID).Scan(&connected)
		if connected {
			metrics.SandboxStartSeconds.WithLabelValues(prof.Name, prof.Kind, "runbox").Observe(time.Since(started).Seconds())
			return
		}
		if i%5 != 0 {
			continue
		}
		p, err := m.K8s.GetPod(ctx, m.Cfg.Namespace, pod)
		if err != nil {
			continue
		}
		if p == nil {
			return
		}
		if reason := startFailure(p); reason != "" {
			metrics.SandboxStartFailures.WithLabelValues(prof.Name, prof.Kind, reason).Inc()
			return
		}
	}
	metrics.SandboxStartFailures.WithLabelValues(prof.Name, prof.Kind, "timeout").Inc()
}

// StopEphemeral deletes a temporary sandbox.
func (m *Sandboxes) StopEphemeral(ctx context.Context, workspaceID string) {
	_ = m.K8s.DeletePod(ctx, m.Cfg.Namespace, ephemeralPod(workspaceID), 5)
}

func ephemeralPod(workspaceID string) string {
	id := strings.ReplaceAll(strings.TrimPrefix(workspaceID, "run-"), "-", "")
	if len(id) > 20 {
		id = id[:20]
	}
	return "runbox-" + id
}
