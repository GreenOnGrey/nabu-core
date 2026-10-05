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
}

// Sandboxes starts personal sandboxes on demand and puts them to sleep after
// idle (R7: the space sleeps when idle and comes up on access).
type Sandboxes struct {
	Pool   *pgxpool.Pool
	K8s    *k8s.Client
	Space  *Service
	Events events.Publisher
	Cfg    SandboxConfig
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
	tag, err := m.Pool.Exec(ctx, `UPDATE spaces SET state = 'starting', pod = $2, state_changed_at = now(), last_activity_at = now()
		WHERE user_id = $1 AND state IN ('sleeping','stopping')`, uid, PodName(uid))
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
	err = m.K8s.CreatePod(ctx, m.Cfg.Namespace, m.manifest(uid))
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
// credentials — only its own token (SPC-04).
func (m *Sandboxes) manifest(uid uuid.UUID) map[string]any {
	work := m.Cfg.Quota + (1 << 30)
	env := []map[string]any{
		{"name": "NABU_SANDBOX_USER", "value": uid.String()},
		{"name": "NABU_SANDBOX_TOKEN", "value": m.Space.SandboxToken(uid, m.Cfg.TokenTTL)},
		{"name": "NABU_WORKSPACE_TOKEN", "value": m.Space.WorkspaceToken(uid, m.Cfg.TokenTTL)},
		{"name": "NABU_RELAY_URL", "value": m.Cfg.RelayURL},
		{"name": "NABU_API_INTERNAL_URL", "value": m.Cfg.APIURL},
		{"name": "NABU_SANDBOX_EXCLUDE", "value": strings.Join(m.Cfg.Exclude, ",")},
		{"name": "HOME", "value": "/work"},
	}
	sec := map[string]any{"runAsNonRoot": true, "allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true,
		"capabilities": map[string]any{"drop": []string{"ALL"}}}
	return map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": PodName(uid), "namespace": m.Cfg.Namespace,
			"labels": map[string]string{"app.kubernetes.io/name": "nabu", "app.kubernetes.io/component": "sandbox", "nabu.io/user": uid.String()}},
		"spec": map[string]any{
			"automountServiceAccountToken":  false,
			"restartPolicy":                 "Always",
			"terminationGracePeriodSeconds": 90,
			"imagePullSecrets":              []map[string]string{{"name": "ghcr-pull"}},
			"securityContext": map[string]any{"runAsNonRoot": true, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
				"seccompProfile": map[string]string{"type": "RuntimeDefault"}},
			"containers": []map[string]any{{
				"name": "sandbox", "image": m.Cfg.Image, "args": []string{"sandbox"}, "env": env, "workingDir": "/work",
				"resources": map[string]any{
					"requests": map[string]string{"cpu": "50m", "memory": "64Mi"},
					"limits":   map[string]string{"cpu": m.Cfg.CPU, "memory": m.Cfg.Memory, "ephemeral-storage": formatBytes(work + (512 << 20))},
				},
				"securityContext": sec,
				"volumeMounts":    []map[string]string{{"name": "work", "mountPath": "/work"}, {"name": "tmp", "mountPath": "/tmp"}},
			}},
			"volumes": []map[string]any{
				{"name": "work", "emptyDir": map[string]any{"sizeLimit": formatBytes(work)}},
				{"name": "tmp", "emptyDir": map[string]any{"sizeLimit": "512Mi"}},
			},
		},
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
	// stopping → sleeping once the pod is gone; starting for too long → retry later.
	rows, err = m.Pool.Query(ctx, `SELECT user_id, state FROM spaces WHERE state = 'stopping'
		OR (state = 'starting' AND state_changed_at < now() - interval '5 minutes')`)
	if err != nil {
		return
	}
	type st struct {
		uid   uuid.UUID
		state string
	}
	var list []st
	for rows.Next() {
		var s st
		if rows.Scan(&s.uid, &s.state) == nil {
			list = append(list, s)
		}
	}
	rows.Close()
	for _, s := range list {
		p, err := m.K8s.GetPod(ctx, m.Cfg.Namespace, PodName(s.uid))
		if err != nil {
			continue
		}
		if p == nil || s.state == "starting" {
			if p != nil {
				_ = m.K8s.DeletePod(ctx, m.Cfg.Namespace, PodName(s.uid), 0)
			}
			_, _ = m.Pool.Exec(ctx, `UPDATE spaces SET state = 'sleeping', pod = NULL, state_changed_at = now() WHERE user_id = $1 AND state = $2`, s.uid, s.state)
			m.publish(ctx, s.uid)
		}
	}
	var n int
	_ = m.Pool.QueryRow(ctx, `SELECT count(*) FROM spaces WHERE state <> 'sleeping'`).Scan(&n)
	metrics.Sandboxes.Set(float64(n))
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
// (workspace "nabu", arch §4.3): no files are restored or kept.
func (m *Sandboxes) StartEphemeral(ctx context.Context, workspaceID string) error {
	name := ephemeralPod(workspaceID)
	man := m.manifest(uuid.Nil)
	meta := man["metadata"].(map[string]any)
	meta["name"] = name
	meta["labels"] = map[string]string{"app.kubernetes.io/name": "nabu", "app.kubernetes.io/component": "runbox"}
	spec := man["spec"].(map[string]any)
	spec["restartPolicy"] = "Never"
	c := spec["containers"].([]map[string]any)[0]
	c["env"] = []map[string]any{
		{"name": "NABU_SANDBOX_EPHEMERAL", "value": "1"},
		{"name": "NABU_SANDBOX_WORKSPACE", "value": workspaceID},
		{"name": "NABU_WORKSPACE_TOKEN", "value": m.Space.Signer.Issue(jwt.Claims{Audience: jwt.AudWorkspace, Workspace: workspaceID, Kind: "sandbox"}, m.Cfg.TokenTTL)},
		{"name": "NABU_RELAY_URL", "value": m.Cfg.RelayURL},
		{"name": "HOME", "value": "/work"},
	}
	if err := m.K8s.CreatePod(ctx, m.Cfg.Namespace, man); err != nil && !k8s.Conflict(err) {
		return err
	}
	return nil
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
