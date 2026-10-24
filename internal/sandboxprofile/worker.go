package sandboxprofile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// WorkerConfig is what the worker needs to run probe pods and the preloading
// DaemonSet in the namespace of sandboxes.
type WorkerConfig struct {
	Namespace    string
	InitImage    string
	PullSecrets  []string
	ProbeTimeout time.Duration
	APIURL       string // the internal API the probe reports to
}

// Worker checks custom images and preloads the images of assigned profiles
// (tech §5.6, §5.7).
type Worker struct {
	Profiles *Service
	K8s      *k8s.Client
	Signer   *jwt.Signer
	Cfg      WorkerConfig
	// OnReady is called with the inventory of a custom profile that passed its
	// check (the automatic «added» of requested packages, R16).
	OnReady func(ctx context.Context, m Manifest)
}

// Run checks probes every 10 seconds and the preloading every minute.
func (w *Worker) Run(ctx context.Context) {
	probe := time.NewTicker(10 * time.Second)
	prepull := time.NewTicker(time.Minute)
	defer probe.Stop()
	defer prepull.Stop()
	w.Prepull(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-probe.C:
			w.Probe(ctx)
		case <-prepull.C:
			w.Prepull(ctx)
		}
	}
}

// ─── Probe of custom images ─────────────────────────────────────────

// probePod is the pod of one check: sandbox-probe-<name>-<5 characters of its
// start> (tech §5.7). A new check never reads the status of a previous pod
// that is still terminating.
func probePod(profile string, started time.Time) string {
	const span = 36 * 36 * 36 * 36 * 36
	return fmt.Sprintf("sandbox-probe-%s-%05s", profile, strconv.FormatInt(started.UnixMicro()%span, 36))
}

// probeSelector finds every probe pod of a profile, whatever its check.
func probeSelector(profile string) string {
	return "app.kubernetes.io/component=sandbox-probe,nabu.io/profile=" + profile
}

// dropProbePods removes the probe pods of a profile.
func (w *Worker) dropProbePods(ctx context.Context, profile string) {
	pods, err := w.K8s.ListPods(ctx, w.Cfg.Namespace, probeSelector(profile))
	if err != nil {
		return
	}
	for _, p := range pods {
		_ = w.K8s.DeletePod(ctx, w.Cfg.Namespace, p.Metadata.Name, 0)
	}
}

type probeRow struct {
	name, ref string
	pending   *string
	digest    *string
	started   *time.Time
	report    []byte
}

// Probe starts and finishes the checks of custom profiles (R22, R24).
func (w *Worker) Probe(ctx context.Context) {
	if w == nil || w.K8s == nil {
		return
	}
	rows, err := w.Profiles.Pool.Query(ctx, `SELECT name, image_ref, pending_ref, image_digest, probe_started_at, probe_report
		FROM sandbox_profiles WHERE kind = 'custom' AND (state = 'checking' OR pending_ref IS NOT NULL)`)
	if err != nil {
		slog.Error("sandbox probe", "err", err)
		return
	}
	var list []probeRow
	for rows.Next() {
		var r probeRow
		if rows.Scan(&r.name, &r.ref, &r.pending, &r.digest, &r.started, &r.report) == nil {
			list = append(list, r)
		}
	}
	rows.Close()
	for _, r := range list {
		w.probeOne(ctx, r)
	}
}

func (w *Worker) probeOne(ctx context.Context, r probeRow) {
	ns := w.Cfg.Namespace
	image := r.ref
	if r.pending != nil {
		image = *r.pending
	}
	if r.started == nil {
		// Postgres keeps microseconds: the name is found again from the stored start.
		started := time.Now().UTC().Truncate(time.Microsecond)
		w.dropProbePods(ctx, r.name)
		if err := w.K8s.CreatePod(ctx, ns, w.probeManifest(r.name, image, started)); err != nil && !k8s.Conflict(err) {
			slog.Warn("sandbox probe pod", "profile", r.name, "err", err)
			return
		}
		_, _ = w.Profiles.Pool.Exec(ctx, `UPDATE sandbox_profiles SET probe_started_at = $2, probe_report = NULL WHERE name = $1`, r.name, started)
		return
	}
	p, err := w.K8s.GetPod(ctx, ns, probePod(r.name, *r.started))
	if err != nil {
		return
	}
	deadline := r.started.Add(w.Cfg.ProbeTimeout + time.Minute)
	switch {
	case p == nil:
		if time.Now().After(deadline) {
			w.finish(ctx, r, "", "sandbox_probe_timeout", nil)
		} else {
			// The pod vanished (node drain): start over.
			_, _ = w.Profiles.Pool.Exec(ctx, `UPDATE sandbox_profiles SET probe_started_at = NULL WHERE name = $1`, r.name)
		}
		return
	case pullFailed(p):
		w.finish(ctx, r, "", "sandbox_probe_pull", nil)
	case p.Status.Phase == "Failed" && p.Status.Reason == "DeadlineExceeded", time.Now().After(deadline):
		w.finish(ctx, r, "", "sandbox_probe_timeout", nil)
	case p.Status.Phase == "Succeeded" || p.Status.Phase == "Failed":
		var rep ProbeReport
		if len(r.report) == 0 {
			// The report may still be on its way.
			if time.Since(*r.started) < 15*time.Second {
				return
			}
			w.finish(ctx, r, "", "sandbox_probe_crash", nil)
			return
		}
		if err := json.Unmarshal(r.report, &rep); err != nil || !rep.OK || p.Status.Phase != "Succeeded" {
			code := rep.Error
			if code == "" {
				code = "sandbox_probe_crash"
			}
			w.finish(ctx, r, "", code, nil)
			return
		}
		digest := ""
		if len(p.Status.ContainerStatuses) > 0 {
			digest = digestOf(p.Status.ContainerStatuses[0].ImageID)
		}
		if digest == "" {
			w.finish(ctx, r, "", "sandbox_probe_crash", nil)
			return
		}
		w.finish(ctx, r, digest, "", rep.Inventory)
	}
}

func pullFailed(p *k8s.Pod) bool {
	for _, s := range p.Status.ContainerStatuses {
		switch s.Waiting() {
		case "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "ErrImageNeverPull":
			return true
		}
	}
	return false
}

// digestOf takes sha256:… from an imageID (docker-pullable://repo@sha256:…).
func digestOf(imageID string) string {
	if i := strings.LastIndex(imageID, "@"); i >= 0 {
		return imageID[i+1:]
	}
	if strings.HasPrefix(imageID, "sha256:") {
		return imageID
	}
	return ""
}

// finish records the result of a probe: a ready profile on its new digest, or
// the error — a profile being refreshed stays on its old digest (R24).
func (w *Worker) finish(ctx context.Context, r probeRow, digest, errCode string, inv *Manifest) {
	w.dropProbePods(ctx, r.name)
	pool := w.Profiles.Pool
	if errCode != "" {
		metrics.SandboxProbes.WithLabelValues(strings.TrimPrefix(errCode, "sandbox_probe_")).Inc()
		_, err := pool.Exec(ctx, `UPDATE sandbox_profiles SET
			state = CASE WHEN image_digest IS NULL THEN 'error' ELSE 'ready' END,
			error = $2, pending_ref = NULL, probe_started_at = NULL, probe_report = NULL, checked_at = now(), updated_at = now()
			WHERE name = $1`, r.name, errCode)
		if err != nil {
			slog.Error("sandbox probe result", "profile", r.name, "err", err)
		}
		slog.Info("sandbox profile check failed", "profile", r.name, "error", errCode)
		return
	}
	metrics.SandboxProbes.WithLabelValues("ok").Inc()
	if inv == nil {
		inv = &Manifest{}
	}
	inv.Name, inv.Kind, inv.Digest = r.name, KindCustom, digest
	b, _ := json.Marshal(inv)
	_, err := pool.Exec(ctx, `UPDATE sandbox_profiles SET state = 'ready', error = NULL, image_digest = $2,
		image_ref = coalesce(pending_ref, image_ref), pending_ref = NULL, inventory = $3, base_version = nullif($4, ''),
		probe_started_at = NULL, probe_report = NULL, checked_at = now(), updated_at = now() WHERE name = $1`,
		r.name, digest, b, inv.Version)
	if err != nil {
		slog.Error("sandbox probe result", "profile", r.name, "err", err)
		return
	}
	slog.Info("sandbox profile ready", "profile", r.name, "digest", digest)
	if w.OnReady != nil {
		w.OnReady(ctx, *inv)
	}
}

// probeManifest is the probe pod: the profile image with the nabu binary
// from the init container, the security of a sandbox, no space and no relay
// (tech §5.7).
func (w *Worker) probeManifest(profile, image string, started time.Time) map[string]any {
	ttl := w.Cfg.ProbeTimeout + 5*time.Minute
	env := []map[string]any{
		{"name": "NABU_PROBE_TOKEN", "value": ProbeToken(w.Signer, profile, ttl)},
		{"name": "NABU_API_INTERNAL_URL", "value": w.Cfg.APIURL},
	}
	return map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": probePod(profile, started), "namespace": w.Cfg.Namespace,
			"labels": map[string]string{"app.kubernetes.io/name": "nabu", "app.kubernetes.io/component": "sandbox-probe", "nabu.io/profile": profile}},
		"spec": map[string]any{
			"automountServiceAccountToken": false,
			"restartPolicy":                "Never",
			"activeDeadlineSeconds":        int(w.Cfg.ProbeTimeout.Seconds()),
			"imagePullSecrets":             PullSecrets(w.Cfg.PullSecrets),
			"securityContext":              PodSecurity(),
			"initContainers":               []map[string]any{InitContainer(w.Cfg.InitImage, []string{"sandbox-init", "--binary-only"}, nil)},
			"containers": []map[string]any{{
				"name": "probe", "image": image, "command": []string{NabuDir + "/bin/nabu"}, "args": []string{"sandbox-probe"},
				"env": env, "securityContext": ContainerSecurity(),
				"resources": map[string]any{
					"requests": map[string]string{"cpu": "50m", "memory": "64Mi"},
					"limits":   map[string]string{"cpu": "500m", "memory": "256Mi", "ephemeral-storage": "256Mi"},
				},
				"volumeMounts": []map[string]any{NabuMount(), {"name": "tmp", "mountPath": "/tmp"}},
			}},
			"volumes": []map[string]any{NabuVolume(), {"name": "tmp", "emptyDir": map[string]any{"sizeLimit": "128Mi"}}},
		},
	}
}

// ─── Preloading ─────────────────────────────────────────────────────

const prepullName = "sandbox-prepull"

// Prepull keeps the DaemonSet that pulls the images of the assigned profiles
// on every node and records how many nodes have them (R9, tech §5.6).
func (w *Worker) Prepull(ctx context.Context) {
	if w == nil || w.K8s == nil || w.Profiles.Cfg.Legacy() {
		return
	}
	names, err := w.Profiles.Assignments(ctx)
	if err != nil {
		slog.Error("sandbox prepull", "err", err)
		return
	}
	type img struct{ name, image string }
	var want []img
	for _, n := range names {
		p, err := w.Profiles.load(ctx, n)
		if err != nil {
			continue
		}
		want = append(want, img{n, p.Image})
	}
	h := sha256.New()
	var inits []map[string]any
	inits = append(inits, InitContainer(w.Cfg.InitImage, []string{"sandbox-init", "--binary-only"}, nil))
	for _, i := range want {
		h.Write([]byte(i.name + "=" + i.image + "\n"))
		inits = append(inits, map[string]any{
			"name": "p-" + i.name, "image": i.image, "command": []string{NabuDir + "/bin/nabu"}, "args": []string{"version"},
			"volumeMounts": []map[string]any{NabuMount()}, "securityContext": ContainerSecurity(),
			"resources": map[string]any{"requests": map[string]string{"cpu": "10m", "memory": "16Mi"},
				"limits": map[string]string{"cpu": "100m", "memory": "64Mi"}},
		})
	}
	hash := hex.EncodeToString(h.Sum(nil))[:16]
	cur, err := w.K8s.GetDaemonSet(ctx, w.Cfg.Namespace, prepullName)
	if err != nil {
		slog.Warn("sandbox prepull", "err", err)
		return
	}
	if annotation(cur, "nabu.io/images-hash") != hash {
		labels := map[string]string{"app.kubernetes.io/name": "nabu", "app.kubernetes.io/component": prepullName}
		ds := map[string]any{
			"apiVersion": "apps/v1", "kind": "DaemonSet",
			"metadata": map[string]any{"name": prepullName, "namespace": w.Cfg.Namespace, "labels": labels,
				"annotations": map[string]string{"nabu.io/images-hash": hash}},
			"spec": map[string]any{
				"selector": map[string]any{"matchLabels": labels},
				"template": map[string]any{
					"metadata": map[string]any{"labels": labels},
					"spec": map[string]any{
						"automountServiceAccountToken": false,
						"imagePullSecrets":             PullSecrets(w.Cfg.PullSecrets),
						"securityContext":              PodSecurity(),
						"initContainers":               inits,
						"containers": []map[string]any{{"name": "pause", "image": "registry.k8s.io/pause:3.10",
							"securityContext": ContainerSecurity(),
							"resources": map[string]any{"requests": map[string]string{"cpu": "1m", "memory": "8Mi"},
								"limits": map[string]string{"cpu": "10m", "memory": "16Mi"}}}},
						"volumes": []map[string]any{NabuVolume()},
					},
				},
			},
		}
		if err := w.K8s.PutDaemonSet(ctx, w.Cfg.Namespace, ds); err != nil {
			slog.Warn("sandbox prepull daemonset", "err", err)
			return
		}
		slog.Info("sandbox prepull updated", "profiles", len(want))
	}
	pods, err := w.K8s.ListPods(ctx, w.Cfg.Namespace, "app.kubernetes.io/component%3D"+prepullName)
	if err != nil {
		return
	}
	pool := w.Profiles.Pool
	_, _ = pool.Exec(ctx, `UPDATE sandbox_profiles SET nodes_ready = 0, nodes_total = 0 WHERE nodes_total > 0`)
	for _, i := range want {
		ready := 0
		for _, p := range pods {
			for _, s := range p.Status.InitContainerStatuses {
				if code, done := s.Exited(); s.Name == "p-"+i.name && done && code == 0 {
					ready++
				}
			}
		}
		_, _ = pool.Exec(ctx, `UPDATE sandbox_profiles SET nodes_ready = $2, nodes_total = $3 WHERE name = $1`, i.name, ready, len(pods))
		metrics.SandboxPrepullNodes.WithLabelValues(i.name, "ready").Set(float64(ready))
		metrics.SandboxPrepullNodes.WithLabelValues(i.name, "pending").Set(float64(len(pods) - ready))
	}
}

func annotation(obj map[string]any, key string) string {
	meta, _ := obj["metadata"].(map[string]any)
	ann, _ := meta["annotations"].(map[string]any)
	v, _ := ann[key].(string)
	return v
}
