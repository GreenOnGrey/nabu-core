package agentpods

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

const (
	lockKey        = "nabu:agent-pods"
	loopInterval   = time.Second
	warmInterval   = 5 * time.Minute
	statusInterval = 15 * time.Second
	quotaInterval  = time.Minute
	redispatch     = time.Minute      // a turn handed back to Kafka and not taken
	notifyAfter    = 30 * time.Second // R12: one message in a messenger
	probeMin       = 5 * time.Minute
	probeMax       = 30 * time.Minute
	learnedTTL     = 24 * time.Hour
	capacityQuiet  = 15 * time.Minute // no pods go to the reserve after a wait for capacity
	typingInterval = 4 * time.Second  // the typing mark of a messenger while a turn waits
	staleBusy      = 2 * time.Minute  // a busy mark the pod does not confirm for this long

	// States of a turn in the agent.state event.
	waitStarting = "starting"
	waitQueued   = "queued"
)

// Run is the loop of the manager: one per instance under an advisory lock
// (arch §3.2); another worker takes over when this one goes away.
func (m *Manager) Run(ctx context.Context) {
	if m == nil || m.Cfg.Local {
		return
	}
	m.wake = make(chan struct{}, 1)
	postgres.RunLocked(ctx, m.Pool, lockKey, 5*time.Second, func(ctx context.Context) {
		slog.InfoContext(ctx, "agent pods: the manager runs here")
		t := time.NewTicker(loopInterval)
		defer t.Stop()
		for {
			if err := m.Step(ctx); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "agent pods: loop", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			case <-m.wake:
			}
		}
	})
}

type podRow struct {
	owner        uuid.UUID
	generation   int64
	state        string
	stopReason   string
	image        string
	warm, probe  bool
	warmRank     *int
	busyUntil    *time.Time
	startedAt    time.Time
	lastActivity time.Time
	ip           string
}

func (p *podRow) busy(now time.Time) bool { return p.busyUntil != nil && p.busyUntil.After(now) }

func (m *Manager) podRows(ctx context.Context) ([]*podRow, error) {
	rows, err := m.Pool.Query(ctx, `SELECT owner_id, generation, state, COALESCE(stop_reason,''), image, warm, probe, warm_rank,
		busy_until, started_at, last_activity_at, COALESCE(ip,'') FROM agent_pods`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*podRow
	for rows.Next() {
		p := &podRow{}
		if err := rows.Scan(&p.owner, &p.generation, &p.state, &p.stopReason, &p.image, &p.warm, &p.probe, &p.warmRank,
			&p.busyUntil, &p.startedAt, &p.lastActivity, &p.ip); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Step is one pass of the manager (tech §4.4).
func (m *Manager) Step(ctx context.Context) error {
	now := m.now()
	// R15: the pod of a blocked or archived user, or of a group agent that is
	// off, stops at once.
	if _, err := m.Pool.Exec(ctx, `UPDATE agent_pods p SET state = 'stopping', stop_reason = $1, state_changed_at = now()
		FROM users u LEFT JOIN group_agents g ON g.data_user_id = u.id
		WHERE u.id = p.owner_id AND p.state <> 'stopping' AND (u.status <> 'active' OR (g.id IS NOT NULL AND g.status <> 'active'))`, StopLifecycle); err != nil {
		return err
	}
	pods, err := m.Kube.ListPods(ctx, m.Cfg.Namespace, "app.kubernetes.io/component%3D"+component)
	if err != nil {
		return err
	}
	rows, err := m.podRows(ctx)
	if err != nil {
		return err
	}
	rows = m.reconcile(ctx, now, rows, pods)
	ceiling, source := m.capacity(ctx, now, rows)
	if err := m.serve(ctx, now, rows, ceiling, source); err != nil {
		return err
	}
	if now.Sub(m.warmAt) >= warmInterval {
		m.warmAt = now
		if err := m.rank(ctx, now, ceiling); err != nil {
			slog.WarnContext(ctx, "agent pods: warm reserve", "err", err)
		}
	}
	m.prewarm(ctx, now, ceiling)
	if now.Sub(m.statusAt) >= statusInterval {
		m.statusAt = now
		m.poll(ctx, now, rows)
	}
	m.gauges(ctx)
	metrics.AgentManagerLoop.Set(float64(time.Now().Unix()))
	return nil
}

// reconcile brings the rows and the pods together and applies the rules of
// starting, idling and image updates; it returns the rows that stay.
func (m *Manager) reconcile(ctx context.Context, now time.Time, rows []*podRow, pods []k8s.Pod) []*podRow {
	byOwner := map[string]*k8s.Pod{}
	known := map[string]int64{}
	for _, r := range rows {
		known[r.owner.String()] = r.generation
	}
	for i := range pods {
		p := &pods[i]
		owner := p.Metadata.Labels["nabu.io/owner"]
		gen, _ := strconv.ParseInt(p.Metadata.Labels["nabu.io/generation"], 10, 64)
		if g, ok := known[owner]; !ok || g != gen {
			// LC-09: a pod without a row, or of a previous start
			if p.Metadata.DeletionTimestamp == nil {
				_ = m.Kube.DeletePod(ctx, m.Cfg.Namespace, p.Metadata.Name, 0)
			}
			continue
		}
		byOwner[owner] = p
	}
	var ready int
	for _, r := range rows {
		if r.state == StateReady {
			ready++
		}
	}
	replace := int(math.Ceil(float64(ready) * 0.1)) // LC-13: a new image comes gradually
	var keep []*podRow
	drop := func(r *podRow, reason string) {
		_, _ = m.Pool.Exec(ctx, `DELETE FROM agent_pods WHERE owner_id = $1 AND generation = $2`, r.owner, r.generation)
		metrics.AgentPodStops.WithLabelValues(reason).Inc()
		slog.InfoContext(ctx, "agent pod stopped", "owner", r.owner, "generation", r.generation, "reason", reason)
	}
	// halt stops a pod; an idle one only when no turn began in it meanwhile (R11).
	halt := func(r *podRow, reason string, idle bool) {
		if idle {
			if !m.stopIdle(ctx, r.owner, reason, now) {
				return
			}
		} else if _, err := markStopping(ctx, m.Pool, r.owner, reason, nil); err != nil {
			return
		}
		r.state, r.stopReason = StateStopping, reason
		_ = m.Kube.DeletePod(ctx, m.Cfg.Namespace, PodName(r.owner), 30)
	}
	for _, r := range rows {
		p := byOwner[r.owner.String()]
		switch r.state {
		case StateStopping:
			if p == nil {
				drop(r, orReason(r.stopReason, StopLost))
				continue
			}
			if p.Metadata.DeletionTimestamp == nil {
				_ = m.Kube.DeletePod(ctx, m.Cfg.Namespace, p.Metadata.Name, 30)
			}
		case StateStarting:
			switch {
			case p == nil || p.Metadata.DeletionTimestamp != nil:
				drop(r, StopLost)
				continue
			case p.Ready():
				_, _ = m.Pool.Exec(ctx, `UPDATE agent_pods SET state = 'ready', ip = $3, node = $4, ready_at = $5, state_changed_at = $5,
					last_activity_at = $5 WHERE owner_id = $1 AND generation = $2 AND state = 'starting'`, r.owner, r.generation, p.Status.PodIP, p.Spec.NodeName, now)
				r.state, r.ip, r.lastActivity = StateReady, p.Status.PodIP, now
				metrics.AgentPodStart.Observe(now.Sub(r.startedAt).Seconds())
				slog.InfoContext(ctx, "agent pod ready", "owner", r.owner, "generation", r.generation, "seconds", now.Sub(r.startedAt).Seconds())
				if r.probe {
					m.probed(ctx, now, true)
				}
			default:
				if since, ok := p.Unschedulable(); ok {
					if since.IsZero() {
						since = r.startedAt
					}
					if now.Sub(since) > m.Cfg.ScheduleTimeout {
						m.learn(ctx, now, rows, r)
						halt(r, StopUnschedulable, false)
					}
				} else if now.Sub(r.startedAt) > m.Cfg.StartTimeout+m.Cfg.ScheduleTimeout {
					halt(r, StopStartTimeout, false) // LC-07: the turn stays in the queue, the pod starts again
				}
			}
		case StateReady:
			idle := now.Sub(r.lastActivity)
			switch {
			case p == nil || p.Metadata.DeletionTimestamp != nil:
				drop(r, StopLost)
				continue
			case r.busy(now):
			case !r.warm && idle > m.Cfg.IdleTimeout:
				halt(r, StopIdle, true)
			case p.Image() != "" && p.Image() != m.Cfg.Image && idle > m.Cfg.MinIdle && replace > 0:
				replace--
				halt(r, StopImage, true)
			}
		}
		keep = append(keep, r)
	}
	return keep
}

func orReason(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ─── capacity (tech §4.5) ───────────────────────────────────────────

// cpuMilli and memBytes read Kubernetes quantities of the kinds a quota of
// pods uses; 0 when the value is not understood.
func cpuMilli(q string) int64 {
	q = strings.TrimSpace(q)
	if v, ok := strings.CutSuffix(q, "m"); ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	}
	f, err := strconv.ParseFloat(q, 64)
	if err != nil {
		return 0
	}
	return int64(f * 1000)
}

func memBytes(q string) int64 {
	q = strings.TrimSpace(q)
	units := []struct {
		suffix string
		mul    float64
	}{{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40}, {"k", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12}}
	for _, u := range units {
		if v, ok := strings.CutSuffix(q, u.suffix); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0
			}
			return int64(f * u.mul)
		}
	}
	n, _ := strconv.ParseInt(q, 10, 64)
	return n
}

// quotaCeiling is the number of pods the quotas of the namespace admit, nil
// without a quota on pods, CPU or memory requests (CAP-02).
func quotaCeiling(quotas []map[string]string, cpuRequest, memRequest string) *int {
	var best *int
	take := func(n int64) {
		v := int(n)
		if best == nil || v < *best {
			best = &v
		}
	}
	for _, hard := range quotas {
		if v, ok := hard["pods"]; ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				take(n)
			}
		}
		for _, k := range []string{"requests.cpu", "cpu"} {
			if v, ok := hard[k]; ok && cpuMilli(cpuRequest) > 0 {
				take(cpuMilli(v) / cpuMilli(cpuRequest))
			}
		}
		for _, k := range []string{"requests.memory", "memory"} {
			if v, ok := hard[k]; ok && memBytes(memRequest) > 0 {
				take(memBytes(v) / memBytes(memRequest))
			}
		}
	}
	return best
}

// ceilingOf picks the lowest of the configured, quota and learned ceilings
// (CAP-06); nil without any.
func ceilingOf(configured int, quota, learned *int) (*int, string) {
	var best *int
	source := "none"
	take := func(v int, s string) {
		if best == nil || v < *best {
			best, source = &v, s
		}
	}
	if configured > 0 {
		take(configured, "config")
	}
	if quota != nil {
		take(*quota, "quota")
	}
	if learned != nil {
		take(*learned, "cluster")
	}
	return best, source
}

// warmTarget is the size of the warm reserve (arch §7, WM-06); -1 means
// every owner active in the window.
func warmTarget(ceiling *int, share float64, max int) int {
	if ceiling == nil {
		if max > 0 {
			return max
		}
		return -1
	}
	w := int(math.Floor(float64(*ceiling) * share))
	if max > 0 && w > max {
		w = max
	}
	return w
}

func reserve(ceiling int) int {
	r := int(math.Ceil(float64(ceiling) * 0.1))
	if r < 2 {
		r = 2
	}
	return r
}

func (m *Manager) capacity(ctx context.Context, now time.Time, rows []*podRow) (*int, string) {
	if now.Sub(m.quotaAt) >= quotaInterval {
		m.quotaAt = now
		if qs, err := m.Kube.Quotas(ctx, m.Cfg.Namespace); err == nil {
			m.quota = quotaCeiling(qs, m.Cfg.CPURequest, m.Cfg.MemoryRequest)
		} else {
			slog.WarnContext(ctx, "agent pods: quotas", "err", err)
		}
	}
	var learned *int
	var learnedAt, peakAt *time.Time
	_ = m.Pool.QueryRow(ctx, `SELECT learned_max, learned_at, peak_at FROM agent_capacity`).Scan(&learned, &learnedAt, &peakAt)
	if learned != nil && learnedAt != nil && now.Sub(*learnedAt) > learnedTTL && (peakAt == nil || now.Sub(*peakAt) > learnedTTL) {
		// CAP-08: a day without refusals and without reaching the ceiling
		_, _ = m.Pool.Exec(ctx, `UPDATE agent_capacity SET learned_max = NULL, learned_at = NULL, probe_after = NULL, probe_interval = $1`, probeMin)
		learned = nil
	}
	ceiling, source := ceilingOf(m.Cfg.PodsMax, m.quota, learned)
	peak := ceiling != nil && len(rows) >= *ceiling
	_, _ = m.Pool.Exec(ctx, `UPDATE agent_capacity SET ceiling = $1, source = $2, warm_target = $3, updated_at = now(),
		peak_at = CASE WHEN $4 THEN $5 ELSE peak_at END`, ceiling, source, warmTarget(ceiling, m.Cfg.WarmShare, m.Cfg.WarmMax), peak, now)
	return ceiling, source
}

// learn records that the cluster placed no more pods than run now (CAP-03).
func (m *Manager) learn(ctx context.Context, now time.Time, rows []*podRow, failed *podRow) {
	n := 0
	for _, r := range rows {
		if r != failed && (r.state == StateReady || r.state == StateStopping) {
			n++
		}
	}
	if failed.probe {
		m.probed(ctx, now, false)
		return
	}
	m.learnCount(ctx, now, n)
}

// quotaExceeded reports a pod the quota of the namespace refused.
func quotaExceeded(err error) bool {
	var ae *k8s.APIError
	return errors.As(err, &ae) && ae.Status == 403 && strings.Contains(ae.Body, "exceeded quota")
}

func (m *Manager) learnCount(ctx context.Context, now time.Time, n int) {
	_, _ = m.Pool.Exec(ctx, `UPDATE agent_capacity SET learned_max = $1, learned_at = $2, probe_interval = $3, probe_after = $2::timestamptz + $3::interval`,
		n, now, probeMin)
	slog.WarnContext(ctx, "agent pods: the cluster cannot place more pods", "ceiling", n)
}

// probed moves the learned ceiling after a pod above it (CAP-04, CAP-05).
func (m *Manager) probed(ctx context.Context, now time.Time, ok bool) {
	if ok {
		_, _ = m.Pool.Exec(ctx, `UPDATE agent_capacity SET learned_max = learned_max + 1, learned_at = $1, probe_after = $1, probe_interval = $2
			WHERE learned_max IS NOT NULL`, now, probeMin)
		return
	}
	_, _ = m.Pool.Exec(ctx, `UPDATE agent_capacity SET probe_interval = LEAST(probe_interval * 2, $2::interval),
		probe_after = $1::timestamptz + LEAST(probe_interval * 2, $2::interval), learned_at = $1`, now, probeMax)
}

// ─── the queue (tech §4.6) ──────────────────────────────────────────

type queued struct {
	Item
	dispatchedAt *time.Time
	notifiedAt   *time.Time
	position     *int
}

func (m *Manager) queue(ctx context.Context) ([]*queued, error) {
	rows, err := m.Pool.Query(ctx, `SELECT q.id, q.owner_id, q.kind, q.reason, q.ref, q.topic, q.key, q.value, q.conversation_id, q.message_id,
			COALESCE(q.channel,''), q.enqueued_at, q.dispatched_at, q.notified_at, q.position, q.resume,
			CASE WHEN g.id IS NOT NULL AND g.status <> 'active' THEN 'off' ELSE u.status END
		FROM agent_queue q JOIN users u ON u.id = q.owner_id LEFT JOIN group_agents g ON g.data_user_id = u.id ORDER BY q.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*queued
	var gone []int64
	for rows.Next() {
		q := &queued{}
		var conv, msg, resume *uuid.UUID
		var status string
		if err := rows.Scan(&q.ID, &q.Owner, &q.Turn.Kind, &q.Reason, &q.Turn.Ref, &q.Turn.Topic, &q.Turn.Key, &q.Turn.Value, &conv, &msg,
			&q.Turn.Channel, &q.EnqueuedAt, &q.dispatchedAt, &q.notifiedAt, &q.position, &resume, &status); err != nil {
			return nil, err
		}
		if conv != nil {
			q.Turn.ConversationID = *conv
		}
		if msg != nil {
			q.Turn.MessageID = *msg
		}
		if resume != nil {
			q.Resume = *resume
		}
		if status != "active" {
			gone = append(gone, q.ID) // R15: the owner is blocked, archived or a group agent that is off
			continue
		}
		out = append(out, q)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(gone) > 0 {
		_, _ = m.Pool.Exec(ctx, `DELETE FROM agent_queue WHERE id = ANY($1)`, gone)
	}
	return out, nil
}

// victim picks the pod to give its place away (R11): not busy and idle for
// MinIdle; pods outside the reserve first, the longest idle first, then warm
// pods from the least active owner.
func victim(rows []*podRow, now time.Time, minIdle time.Duration) *podRow {
	var c []*podRow
	for _, r := range rows {
		if r.state == StateReady && !r.busy(now) && now.Sub(r.lastActivity) > minIdle {
			c = append(c, r)
		}
	}
	if len(c) == 0 {
		return nil
	}
	sort.SliceStable(c, func(i, j int) bool {
		a, b := c[i], c[j]
		if a.warm != b.warm {
			return !a.warm
		}
		if a.warm {
			ra, rb := math.MinInt, math.MinInt
			if a.warmRank != nil {
				ra = *a.warmRank
			}
			if b.warmRank != nil {
				rb = *b.warmRank
			}
			if ra != rb {
				return ra > rb // a larger rank is a less active owner
			}
		}
		return a.lastActivity.Before(b.lastActivity)
	})
	return c[0]
}

type waiting struct {
	owner uuid.UUID
	first time.Time
	task  bool // only runs of tasks wait: people go first (Q-09)
	items []*queued
}

// order lists the owners that wait, people before tasks, then by the time of
// their first waiting turn (R12).
func order(items []*queued) []*waiting {
	by := map[uuid.UUID]*waiting{}
	var out []*waiting
	for _, q := range items {
		w := by[q.Owner]
		if w == nil {
			w = &waiting{owner: q.Owner, first: q.EnqueuedAt, task: true}
			by[q.Owner] = w
			out = append(out, w)
		}
		if q.EnqueuedAt.Before(w.first) {
			w.first = q.EnqueuedAt
		}
		if q.Turn.Kind != KindTask {
			w.task = false
		}
		w.items = append(w.items, q)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].task != out[j].task {
			return !out[i].task
		}
		return out[i].first.Before(out[j].first)
	})
	return out
}

func (m *Manager) serve(ctx context.Context, now time.Time, rows []*podRow, ceiling *int, source string) error {
	items, err := m.queue(ctx)
	if err != nil {
		return err
	}
	// 1. Turns that waited too long (R13); the owners that are gone were
	// dropped when the queue was read.
	var live []*queued
	for _, q := range items {
		if now.Sub(q.EnqueuedAt) > m.Cfg.QueueTimeout {
			tag, err := m.Pool.Exec(ctx, `DELETE FROM agent_queue WHERE id = $1`, q.ID)
			if err == nil && tag.RowsAffected() > 0 {
				metrics.AgentQueueExpired.Inc()
				if m.Expire != nil {
					m.Expire(ctx, q.Item)
				}
			}
			continue
		}
		live = append(live, q)
	}
	pod := map[uuid.UUID]*podRow{}
	used, starting, stopping, probing := len(rows), 0, 0, false
	for _, r := range rows {
		pod[r.owner] = r
		switch r.state {
		case StateStarting:
			starting++
			probing = probing || r.probe
		case StateStopping:
			stopping++
		}
	}
	var probeAfter *time.Time
	_ = m.Pool.QueryRow(ctx, `SELECT probe_after FROM agent_capacity`).Scan(&probeAfter)
	ahead := 0
	busyNow := now.Add(time.Minute)
	typing := m.Waiting != nil && now.Sub(m.typingAt) >= typingInterval
	if typing {
		m.typingAt = now
	}
	for _, w := range order(live) {
		p := pod[w.owner]
		switch {
		case p != nil && p.state == StateReady:
			// 2. The pod is ready: the turns go back to Kafka in their order.
			for _, q := range w.items {
				if q.dispatchedAt != nil && now.Sub(*q.dispatchedAt) < redispatch {
					continue
				}
				if err := m.Bus.Publish(ctx, q.Turn.Topic, q.Turn.Key, q.Turn.Value); err != nil {
					slog.WarnContext(ctx, "agent pods: hand a turn back", "err", err)
					break
				}
				_, _ = m.Pool.Exec(ctx, `UPDATE agent_queue SET dispatched_at = $2 WHERE id = $1`, q.ID, now)
			}
			continue
		case p != nil:
			if typing {
				m.waiting(ctx, w)
			}
			m.position(ctx, w, waitStarting, 0) // starting, or stopping before a new start
			continue
		}
		// 3. No pod: start one, free a place or wait.
		if typing {
			m.waiting(ctx, w)
		}
		state := waitQueued
		switch {
		case starting >= m.Cfg.StartParallel:
		case ceiling == nil || used < *ceiling:
			if err := m.start(ctx, w.owner, false); err != nil {
				slog.WarnContext(ctx, "agent pods: start", "owner", w.owner, "err", err)
				if quotaExceeded(err) {
					// The quota of the namespace is used by more than the pods of
					// agents: what runs now is the ceiling, as with a full cluster.
					m.learnCount(ctx, now, used)
				}
				break
			}
			used, starting, state = used+1, starting+1, waitStarting
		case stopping > 0:
			stopping-- // a place is being freed for this owner
		default:
			if v := victim(rows, now, m.Cfg.MinIdle); v != nil {
				if m.stopIdle(ctx, v.owner, StopEvicted, now) {
					v.state = StateStopping
					_ = m.Kube.DeletePod(ctx, m.Cfg.Namespace, PodName(v.owner), 30)
					slog.InfoContext(ctx, "agent pod evicted", "owner", v.owner, "for", w.owner, "warm", v.warm)
				} else {
					v.busyUntil = &busyNow // a turn began in it: not a candidate in this pass
				}
			} else if source == "cluster" && !probing && probeAfter != nil && !now.Before(*probeAfter) {
				// CAP-04: one pod above the learned ceiling, to see whether the cluster grew
				if err := m.start(ctx, w.owner, true); err == nil {
					used, starting, probing, state = used+1, starting+1, true, waitStarting
				}
			}
		}
		if state == waitQueued {
			m.position(ctx, w, state, ahead)
			ahead++
			m.notify(ctx, now, w)
		} else {
			m.position(ctx, w, state, 0)
		}
	}
	return nil
}

// position tells the owner where the turn stands when that changes (Q-08).
func (m *Manager) position(ctx context.Context, w *waiting, state string, ahead int) {
	pos := -1 // starting
	if state == waitQueued {
		pos = ahead
	}
	for _, q := range w.items {
		if q.position != nil && *q.position == pos {
			continue
		}
		reason := ReasonStarting
		if state == waitQueued {
			reason = ReasonCapacity
		} else if q.Reason == ReasonOrder {
			reason = ReasonOrder
		}
		_, _ = m.Pool.Exec(ctx, `UPDATE agent_queue SET position = $2, reason = $3 WHERE id = $1`, q.ID, pos, reason)
		q.Reason = reason
		m.publishState(ctx, w.owner, q.Turn, state, ahead)
	}
}

// waiting keeps the typing mark of a messenger while a turn waits (Q-14).
func (m *Manager) waiting(ctx context.Context, w *waiting) {
	for _, q := range w.items {
		if q.Turn.Kind == KindMessage {
			m.Waiting(ctx, q.Item)
			return
		}
	}
}

// notify writes once to a messenger user who waits for capacity (Q-13).
func (m *Manager) notify(ctx context.Context, now time.Time, w *waiting) {
	if m.Notify == nil {
		return
	}
	for _, q := range w.items {
		if q.notifiedAt != nil || q.Turn.Kind != KindMessage || now.Sub(q.EnqueuedAt) < notifyAfter {
			continue
		}
		tag, err := m.Pool.Exec(ctx, `UPDATE agent_queue SET notified_at = $2 WHERE owner_id = $1 AND notified_at IS NULL`, w.owner, now)
		if err == nil && tag.RowsAffected() > 0 {
			m.Notify(ctx, q.Item)
		}
		return
	}
}

// ─── the warm reserve (tech §4.7) ───────────────────────────────────

// rank marks the pods of the most active owners of the window as warm.
func (m *Manager) rank(ctx context.Context, now time.Time, ceiling *int) error {
	w := warmTarget(ceiling, m.Cfg.WarmShare, m.Cfg.WarmMax)
	limit := w
	if w < 0 {
		limit = 100000
	}
	rows, err := m.Pool.Query(ctx, `SELECT c.user_id FROM messages m JOIN conversations c ON c.id = m.conversation_id
		JOIN users u ON u.id = c.user_id AND u.status = 'active'
		WHERE m.role = 'user' AND m.created_at > $1 AND m.channel NOT LIKE 'task:%'
		GROUP BY c.user_id ORDER BY count(*) DESC, max(m.created_at) DESC LIMIT $2`, now.Add(-m.Cfg.WarmWindow), limit)
	if err != nil {
		return err
	}
	var top []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		top = append(top, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	ranks := make([]int32, len(top))
	for i := range top {
		ranks[i] = int32(i + 1)
	}
	if _, err := m.Pool.Exec(ctx, `UPDATE agent_pods p SET warm = t.rank IS NOT NULL, warm_rank = t.rank
		FROM (SELECT a.owner_id, r.rank FROM agent_pods a
			LEFT JOIN unnest($1::uuid[], $2::int[]) AS r(owner_id, rank) ON r.owner_id = a.owner_id) t
		WHERE p.owner_id = t.owner_id AND (p.warm IS DISTINCT FROM (t.rank IS NOT NULL) OR p.warm_rank IS DISTINCT FROM t.rank)`, top, ranks); err != nil {
		return err
	}
	m.warmMissing = m.warmMissing[:0]
	have := map[uuid.UUID]bool{}
	prs, err := m.podRows(ctx)
	if err != nil {
		return err
	}
	for _, r := range prs {
		have[r.owner] = true
	}
	for _, id := range top {
		if !have[id] {
			m.warmMissing = append(m.warmMissing, id)
		}
	}
	return nil
}

// prewarm starts the lost pods of the reserve while nobody waits and the
// places for cold starts stay free (WM-04, WM-07, WM-08).
func (m *Manager) prewarm(ctx context.Context, now time.Time, ceiling *int) {
	if len(m.warmMissing) == 0 {
		return
	}
	var waitingTurns, used, starting int
	var capacityAt *time.Time
	if err := m.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM agent_queue), (SELECT count(*) FROM agent_pods),
		(SELECT count(*) FROM agent_pods WHERE state = 'starting'), (SELECT capacity_at FROM agent_capacity)`).
		Scan(&waitingTurns, &used, &starting, &capacityAt); err != nil {
		return
	}
	if waitingTurns > 0 || (capacityAt != nil && now.Sub(*capacityAt) < capacityQuiet) {
		return
	}
	for len(m.warmMissing) > 0 {
		if starting >= m.Cfg.StartParallel || (ceiling != nil && used+reserve(*ceiling) >= *ceiling) {
			return
		}
		owner := m.warmMissing[0]
		m.warmMissing = m.warmMissing[1:]
		var active bool
		_ = m.Pool.QueryRow(ctx, `SELECT status = 'active' FROM users WHERE id = $1`, owner).Scan(&active)
		if !active {
			continue
		}
		if err := m.start(ctx, owner, false); err != nil {
			slog.WarnContext(ctx, "agent pods: warm start", "owner", owner, "err", err)
			return
		}
		_, _ = m.Pool.Exec(ctx, `UPDATE agent_pods SET warm = true WHERE owner_id = $1`, owner)
		used, starting = used+1, starting+1
	}
}

// ─── observation ────────────────────────────────────────────────────

// poll asks the ready pods for their sessions (tech §3.3).
func (m *Manager) poll(ctx context.Context, now time.Time, rows []*podRow) {
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for _, r := range rows {
		if r.state != StateReady || r.ip == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(r *podRow) {
			defer wg.Done()
			defer func() { <-sem }()
			st, err := m.client(r.owner, r.generation, r.ip).Status(ctx)
			if err != nil {
				return
			}
			// The busy mark is checked against the pod: a mark left by a worker
			// that went away is cleared once the pod runs nothing for a while.
			stale := st.Busy == 0 && r.busy(now) && now.Sub(r.lastActivity) > staleBusy
			_, _ = m.Pool.Exec(ctx, `UPDATE agent_pods SET sessions = $3,
				busy = CASE WHEN $4 THEN 0 ELSE busy END, busy_until = CASE WHEN $4 THEN NULL ELSE busy_until END
				WHERE owner_id = $1 AND generation = $2 AND last_activity_at = $5`, r.owner, r.generation, st.Sessions, stale, r.lastActivity)
		}(r)
	}
	wg.Wait()
}

func (m *Manager) gauges(ctx context.Context) {
	states := map[string]float64{StateStarting: 0, StateReady: 0, StateStopping: 0}
	var warm float64
	if rows, err := m.Pool.Query(ctx, `SELECT state, count(*), count(*) FILTER (WHERE warm) FROM agent_pods GROUP BY state`); err == nil {
		for rows.Next() {
			var s string
			var n, w float64
			if rows.Scan(&s, &n, &w) == nil {
				states[s] = n
				if s == StateReady {
					warm = w
				}
			}
		}
		rows.Close()
	}
	for s, n := range states {
		metrics.AgentPods.WithLabelValues(s).Set(n)
	}
	metrics.AgentPodsWarm.Set(warm)
	reasons := map[string]float64{ReasonStarting: 0, ReasonCapacity: 0, ReasonOrder: 0}
	if rows, err := m.Pool.Query(ctx, `SELECT reason, count(*) FROM agent_queue GROUP BY reason`); err == nil {
		for rows.Next() {
			var s string
			var n float64
			if rows.Scan(&s, &n) == nil {
				reasons[s] = n
			}
		}
		rows.Close()
	}
	for s, n := range reasons {
		metrics.AgentQueueLength.WithLabelValues(s).Set(n)
	}
	var ceiling *int
	var source string
	var target int
	if m.Pool.QueryRow(ctx, `SELECT ceiling, source, warm_target FROM agent_capacity`).Scan(&ceiling, &source, &target) == nil {
		metrics.AgentPodsCapacity.Reset()
		v := 0.0
		if ceiling != nil {
			v = float64(*ceiling)
		}
		metrics.AgentPodsCapacity.WithLabelValues(source).Set(v)
		metrics.AgentPodsWarmTarget.Set(float64(max(target, 0)))
	}
}
