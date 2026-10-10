// Package agentpods runs the agent of every owner — a user or the technical
// user of a group — in its own pod (FTR.NAB.CMN-0004): the engine asks for the
// pod of an owner and either gets its operator or leaves the turn in the
// queue; one manager per instance starts and stops pods, keeps the capacity
// and the warm reserve and hands waiting turns back to Kafka.
package agentpods

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Operator is the client of an agent operator: the pool or the pod of an owner.
type Operator interface {
	Open(ctx context.Context, req agent.SessionRequest, bundle func(ctx context.Context) ([]byte, error)) (agent.SessionResponse, error)
	Prompt(ctx context.Context, sessionID string, p agent.PromptRequest, onEvent func(agent.Event)) error
	Abort(ctx context.Context, sessionID string) error
	Patch(ctx context.Context, sessionID string, p agent.PatchRequest) error
	Snapshot(ctx context.Context, sessionID string) ([]byte, error)
	Close(ctx context.Context, sessionID string) error
}

// Kube is the part of the Kubernetes API the manager uses.
type Kube interface {
	CreatePod(ctx context.Context, ns string, manifest map[string]any) error
	DeletePod(ctx context.Context, ns, name string, grace int) error
	ListPods(ctx context.Context, ns, selector string) ([]k8s.Pod, error)
	Quotas(ctx context.Context, ns string) ([]map[string]string, error)
}

// AgentState is the SSE event of a turn waiting for its pod (tech §2).
const AgentState = "agent.state"

// Reasons a turn waits.
const (
	ReasonStarting = "starting"
	ReasonCapacity = "capacity"
	ReasonOrder    = "order"
)

// Kinds of turns.
const (
	KindMessage = "message"
	KindTask    = "task"
)

// Reasons a pod stops (the label of nabu_agent_pod_stops_total).
const (
	StopIdle          = "idle"
	StopEvicted       = "evicted"
	StopLifecycle     = "lifecycle"
	StopAdmin         = "admin"
	StopImage         = "image"
	StopLost          = "lost"
	StopUnschedulable = "unschedulable"
	StopStartTimeout  = "start_timeout"
)

// States of a pod in agent_pods.
const (
	StateStarting = "starting"
	StateReady    = "ready"
	StateStopping = "stopping"
)

const (
	component = "agent-pod"
	podPort   = 8090 // the operator in the pod
)

// ErrQueued means the turn waits for the pod of its owner: the manager hands
// the event back to Kafka when the pod is ready.
var ErrQueued = errors.New("agentpods: the turn waits for the pod of its owner")

// Config configures the pods (tech §7.1).
type Config struct {
	// Local runs every session in one operator and never queues
	// (AGENT_EXECUTOR=local: development and tests without Kubernetes).
	Local bool

	Namespace, Image       string
	CPURequest, CPU        string
	MemoryRequest, Memory  string
	Work                   int64
	MaxSessions            int
	SessionIdle            time.Duration // AGENT_IDLE_TIMEOUT of the operator in the pod
	IdleTimeout, MinIdle   time.Duration
	PodsMax, StartParallel int
	StartTimeout           time.Duration
	ScheduleTimeout        time.Duration
	QueueTimeout           time.Duration
	TurnTimeout            time.Duration
	WarmWindow             time.Duration
	WarmShare              float64
	WarmMax                int
}

// Turn is the Kafka event of a turn: it is published again when the pod is ready.
type Turn struct {
	Kind           string // message | task
	Ref            string // the message or the task run: one row in the queue
	Topic, Key     string
	Value          []byte
	ConversationID uuid.UUID
	MessageID      uuid.UUID
	Channel        string
}

// Item is a turn in the queue, for the hooks of the manager.
type Item struct {
	ID         int64
	Owner      uuid.UUID
	Turn       Turn
	Reason     string
	EnqueuedAt time.Time
	// Resume is the unfinished answer of a turn that lost its pod and waits
	// for a new one (R5).
	Resume uuid.UUID
}

// Manager owns the pods of agents.
type Manager struct {
	Cfg    Config
	Pool   *pgxpool.Pool
	Kube   Kube
	Signer *jwt.Signer
	Bus    kafka.Publisher
	Events events.Publisher
	// Shared is the operator of the local executor.
	Shared Operator
	// Expire answers a turn that did not get a pod in time (R13); Notify
	// tells a messenger user once that the agent is busy, Waiting is called
	// every few seconds for a turn that waits — the typing mark (R12).
	Expire  func(ctx context.Context, it Item)
	Notify  func(ctx context.Context, it Item)
	Waiting func(ctx context.Context, it Item)
	// Now and Dial are replaced in tests.
	Now  func() time.Time
	Dial func(ip string) string

	wake        chan struct{}
	typingAt    time.Time
	warmAt      time.Time
	warmMissing []uuid.UUID
	statusAt    time.Time
	quotaAt     time.Time
	quota       *int
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Manager) poke() {
	if m.wake == nil {
		return
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// PodName is the pod of an owner.
func PodName(owner uuid.UUID) string {
	return "agent-" + strings.ReplaceAll(owner.String(), "-", "")[:20]
}

// ─── leases ─────────────────────────────────────────────────────────

// Lease is the right to run one turn in the pod of an owner.
type Lease struct {
	m          *Manager
	owner      uuid.UUID
	op         Operator
	generation int64
	// Start tells how the turn got its pod: cold after waiting for it to
	// start, otherwise empty (the engine tells warm from hot).
	Start string
	// Attempts counts the pods this turn lost; Resume is the unfinished
	// answer it continues (R5).
	Attempts int
	Resume   uuid.UUID
}

// Operator is the operator of the pod.
func (l *Lease) Operator() Operator { return l.op }

// Generation is the start of the pod; sessions of another generation are gone.
func (l *Lease) Generation() int64 { return l.generation }

// Release ends the turn: the pod may sleep or give its place away again.
func (l *Lease) Release(ctx context.Context) {
	if l == nil || l.m == nil || l.m.Cfg.Local {
		return
	}
	_, _ = l.m.Pool.Exec(context.WithoutCancel(ctx), `UPDATE agent_pods SET busy = GREATEST(busy - 1, 0),
		busy_until = CASE WHEN busy <= 1 THEN NULL ELSE busy_until END, last_activity_at = $3
		WHERE owner_id = $1 AND generation = $2`, l.owner, l.generation, l.m.now())
}

func (m *Manager) client(owner uuid.UUID, generation int64, ip string) *agent.Client {
	base := "http://" + ip + ":" + fmt.Sprint(podPort)
	if m.Dial != nil {
		base = m.Dial(ip)
	}
	sub := owner.String()
	return &agent.Client{BaseURL: base, TokenFunc: func() string {
		// TOK-01: a token of this owner and this start of the pod; a prompt
		// stream is checked once, when it opens.
		return m.Signer.Issue(jwt.Claims{Audience: jwt.AudAgent, Subject: sub, Generation: generation}, 5*time.Minute)
	}}
}

// Acquire returns the pod of the owner for one turn, or ErrQueued after
// putting the turn into the queue (tech §4.1).
func (m *Manager) Acquire(ctx context.Context, owner uuid.UUID, t Turn) (*Lease, error) {
	if m == nil || m.Cfg.Local {
		var op Operator
		if m != nil {
			op = m.Shared
		}
		return &Lease{op: op}, nil
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // a no-op after Commit
	var (
		gen       int64
		state, ip string
		hasPod    = true
	)
	err = tx.QueryRow(ctx, `SELECT generation, state, COALESCE(ip,'') FROM agent_pods WHERE owner_id = $1 FOR UPDATE`, owner).Scan(&gen, &state, &ip)
	if postgres.IsNoRows(err) {
		hasPod, err = false, nil
	}
	if err != nil {
		return nil, err
	}
	var (
		mine       int64
		mineReason string
		enqueued   time.Time
		attempts   int
		resume     *uuid.UUID
	)
	err = tx.QueryRow(ctx, `SELECT id, reason, enqueued_at, attempts, resume FROM agent_queue WHERE owner_id = $1 AND ref = $2`, owner, t.Ref).
		Scan(&mine, &mineReason, &enqueued, &attempts, &resume)
	if err != nil && !postgres.IsNoRows(err) {
		return nil, err
	}
	var earlier int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM agent_queue WHERE owner_id = $1 AND ref <> $2 AND ($3 = 0 OR id < $3)`, owner, t.Ref, mine).Scan(&earlier); err != nil {
		return nil, err
	}
	if hasPod && state == StateReady && ip != "" && earlier == 0 {
		l := &Lease{m: m, owner: owner, generation: gen, op: m.client(owner, gen, ip), Attempts: attempts}
		if resume != nil {
			l.Resume = *resume
		}
		if mine != 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM agent_queue WHERE id = $1`, mine); err != nil {
				return nil, err
			}
			metrics.AgentQueueWait.Observe(m.now().Sub(enqueued).Seconds())
			if mineReason != ReasonOrder {
				l.Start = "cold"
			}
		}
		until := m.now().Add(m.Cfg.TurnTimeout + time.Minute)
		if _, err := tx.Exec(ctx, `UPDATE agent_pods SET busy = CASE WHEN busy_until IS NULL OR busy_until < $3 THEN 1 ELSE busy + 1 END,
			busy_until = GREATEST(COALESCE(busy_until, $2), $2), last_activity_at = $3 WHERE owner_id = $1`, owner, until, m.now()); err != nil {
			return nil, err
		}
		return l, tx.Commit(ctx)
	}
	reason := ReasonStarting
	switch {
	case hasPod && state == StateReady:
		reason = ReasonOrder // Q-10: behind the waiting turns of the same owner
	case !hasPod:
		var ceiling *int
		var used int
		if err := tx.QueryRow(ctx, `SELECT (SELECT ceiling FROM agent_capacity), (SELECT count(*) FROM agent_pods)`).Scan(&ceiling, &used); err != nil {
			return nil, err
		}
		if ceiling != nil && used >= *ceiling {
			reason = ReasonCapacity
		}
	}
	if mine != 0 {
		// Q-12: back in the queue with the time it first got there
		_, err = tx.Exec(ctx, `UPDATE agent_queue SET dispatched_at = NULL WHERE id = $1`, mine)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO agent_queue (owner_id, kind, reason, ref, topic, key, value, conversation_id, message_id, channel, enqueued_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (owner_id, ref) DO NOTHING`, owner, t.Kind, reason, t.Ref, t.Topic, t.Key, t.Value,
			nullUUID(t.ConversationID), nullUUID(t.MessageID), t.Channel, m.now())
		metrics.AgentQueueEnqueued.WithLabelValues(reason).Inc()
	}
	if err != nil {
		return nil, err
	}
	if reason == ReasonCapacity {
		if _, err := tx.Exec(ctx, `UPDATE agent_capacity SET capacity_at = $1`, m.now()); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if mine == 0 && reason != ReasonCapacity {
		m.publishState(ctx, owner, t, "starting", 0)
	}
	m.poke()
	return nil, ErrQueued
}

func nullUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

func (m *Manager) publishState(ctx context.Context, owner uuid.UUID, t Turn, state string, position int) {
	if m.Events == nil || t.Kind != KindMessage || t.ConversationID == uuid.Nil {
		return
	}
	data := map[string]any{"conversationId": t.ConversationID, "messageId": t.MessageID, "state": state}
	if state == "queued" {
		data["position"] = position
	}
	m.Events.Publish(ctx, events.Event{Type: AgentState, UserID: &owner, Data: data})
}

// Peek returns the operator of the owner's pod without a lease — to close
// sessions; false when the owner has no ready pod.
func (m *Manager) Peek(ctx context.Context, owner uuid.UUID) (Operator, bool) {
	if m == nil || m.Cfg.Local {
		if m == nil || m.Shared == nil {
			return nil, false
		}
		return m.Shared, true
	}
	var gen int64
	var ip string
	err := m.Pool.QueryRow(ctx, `SELECT generation, COALESCE(ip,'') FROM agent_pods WHERE owner_id = $1 AND state = 'ready'`, owner).Scan(&gen, &ip)
	if err != nil || ip == "" {
		return nil, false
	}
	return m.client(owner, gen, ip), true
}

// Requeue puts back a turn whose pod went away before the answer began
// (R5, LC-04): the pod of the lease is stopped, a new one starts and the turn
// comes back as the same Kafka event. resume is the answer it continues.
func (m *Manager) Requeue(ctx context.Context, l *Lease, t Turn, resume uuid.UUID) error {
	ctx = context.WithoutCancel(ctx)
	now := m.now()
	if _, err := m.Pool.Exec(ctx, `UPDATE agent_pods SET state = $3, stop_reason = $4, state_changed_at = $5, busy = 0, busy_until = NULL
		WHERE owner_id = $1 AND generation = $2 AND state <> $3`, l.owner, l.generation, StateStopping, StopLost, now); err != nil {
		return err
	}
	_, err := m.Pool.Exec(ctx, `INSERT INTO agent_queue (owner_id, kind, reason, ref, topic, key, value, conversation_id, message_id, channel, enqueued_at, attempts, resume)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (owner_id, ref) DO UPDATE SET dispatched_at = NULL, attempts = EXCLUDED.attempts, resume = EXCLUDED.resume`,
		l.owner, t.Kind, ReasonStarting, t.Ref, t.Topic, t.Key, t.Value, nullUUID(t.ConversationID), nullUUID(t.MessageID), t.Channel, now,
		l.Attempts+1, nullUUID(resume))
	if err != nil {
		return err
	}
	metrics.AgentQueueEnqueued.WithLabelValues(ReasonStarting).Inc()
	m.publishState(ctx, l.owner, t, "starting", 0)
	m.poke()
	return nil
}

// Drop removes a turn nobody will run (its message or run is gone or done),
// so it does not hold the later turns of the owner back (R12).
func (m *Manager) Drop(ctx context.Context, owner uuid.UUID, ref string) {
	if m == nil || m.Cfg.Local {
		return
	}
	_, _ = m.Pool.Exec(ctx, `DELETE FROM agent_queue WHERE owner_id = $1 AND ref = $2`, owner, ref)
}

// markStopping moves the pod of an owner to stopping. With idleAt the pod is
// left alone when a turn runs in it at that time (R11): the caller decided
// on rows read earlier.
func markStopping(ctx context.Context, pool *pgxpool.Pool, owner uuid.UUID, reason string, idleAt *time.Time) (bool, error) {
	tag, err := pool.Exec(ctx, `UPDATE agent_pods SET state = $2, stop_reason = $3, state_changed_at = now()
		WHERE owner_id = $1 AND state <> $2 AND ($4::timestamptz IS NULL OR busy_until IS NULL OR busy_until < $4)`,
		owner, StateStopping, reason, idleAt)
	return err == nil && tag.RowsAffected() > 0, err
}

// Stop stops the pod of an owner; with the lifecycle reason the waiting
// turns of the owner are dropped (R15). The manager deletes the pod.
func (m *Manager) Stop(ctx context.Context, owner uuid.UUID, reason string) (bool, error) {
	if m == nil || m.Cfg.Local {
		return false, nil
	}
	if reason == StopLifecycle {
		if _, err := m.Pool.Exec(ctx, `DELETE FROM agent_queue WHERE owner_id = $1`, owner); err != nil {
			return false, err
		}
	}
	ok, err := markStopping(ctx, m.Pool, owner, reason, nil)
	m.poke()
	return ok, err
}

// ─── the pod ────────────────────────────────────────────────────────

// mebibytes writes a size as a Kubernetes quantity.
func mebibytes(n int64) string { return fmt.Sprint(n/(1<<20)) + "Mi" }

// manifest is the pod of an owner: non-root, a read-only root, no service
// account token and no secret in the environment — only the owner, the
// generation and the public key the tokens are checked with (POD-06).
func (m *Manager) manifest(owner uuid.UUID, generation int64) map[string]any {
	env := []map[string]any{
		{"name": "AGENT_MODE", "value": "owner"},
		{"name": "AGENT_OWNER", "value": owner.String()},
		{"name": "AGENT_GENERATION", "value": fmt.Sprint(generation)},
		{"name": "AGENT_JWT_PUBLIC_KEY", "value": m.Signer.PublicKey()},
		{"name": "AGENT_MAX_SESSIONS", "value": fmt.Sprint(m.Cfg.MaxSessions)},
		{"name": "AGENT_IDLE_TIMEOUT", "value": m.Cfg.SessionIdle.String()},
		{"name": "AGENT_WORKDIR", "value": "/work"},
	}
	sec := map[string]any{"runAsNonRoot": true, "allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true,
		"capabilities": map[string]any{"drop": []string{"ALL"}}}
	return map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": PodName(owner), "namespace": m.Cfg.Namespace,
			"labels": map[string]string{"app.kubernetes.io/name": "nabu", "app.kubernetes.io/component": component,
				"nabu.io/owner": owner.String(), "nabu.io/generation": fmt.Sprint(generation)},
			"annotations": map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "9100"}},
		"spec": map[string]any{
			"automountServiceAccountToken":  false,
			"enableServiceLinks":            false,
			"restartPolicy":                 "Always",
			"terminationGracePeriodSeconds": 30,
			"priorityClassName":             "nabu-agent-pod",
			"imagePullSecrets":              []map[string]string{{"name": "ghcr-pull"}},
			"securityContext": map[string]any{"runAsNonRoot": true, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
				"seccompProfile": map[string]string{"type": "RuntimeDefault"}},
			"containers": []map[string]any{{
				"name": "agent", "image": m.Cfg.Image, "args": []string{"agent"}, "env": env,
				"ports":          []map[string]any{{"name": "agent", "containerPort": podPort}, {"name": "service", "containerPort": 9100}},
				"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/readyz", "port": 9100}, "periodSeconds": 1, "failureThreshold": 30},
				"resources": map[string]any{
					"requests": map[string]string{"cpu": m.Cfg.CPURequest, "memory": m.Cfg.MemoryRequest},
					"limits":   map[string]string{"cpu": m.Cfg.CPU, "memory": m.Cfg.Memory, "ephemeral-storage": mebibytes(m.Cfg.Work + (512 << 20))},
				},
				"securityContext": sec,
				"volumeMounts": []map[string]string{{"name": "work", "mountPath": "/work"}, {"name": "tmp", "mountPath": "/tmp"},
					{"name": "home", "mountPath": "/home/node"}},
			}},
			"volumes": []map[string]any{
				{"name": "work", "emptyDir": map[string]any{"sizeLimit": mebibytes(m.Cfg.Work)}},
				{"name": "tmp", "emptyDir": map[string]any{"sizeLimit": "256Mi"}},
				{"name": "home", "emptyDir": map[string]any{"sizeLimit": "64Mi"}},
			},
		},
	}
}

// start creates the pod of an owner: the row first, so every worker sees it.
func (m *Manager) start(ctx context.Context, owner uuid.UUID, probe bool) error {
	var gen int64
	err := m.Pool.QueryRow(ctx, `INSERT INTO agent_pods (owner_id, generation, pod, state, image, probe, started_at, last_activity_at, state_changed_at)
		VALUES ($1, nextval('agent_pod_generation'), $2, 'starting', $3, $4, $5, $5, $5) ON CONFLICT (owner_id) DO NOTHING RETURNING generation`,
		owner, PodName(owner), m.Cfg.Image, probe, m.now()).Scan(&gen)
	if postgres.IsNoRows(err) {
		return nil // the owner has a pod already
	}
	if err != nil {
		return err
	}
	if err := m.Kube.CreatePod(ctx, m.Cfg.Namespace, m.manifest(owner, gen)); err != nil {
		_, _ = m.Pool.Exec(ctx, `DELETE FROM agent_pods WHERE owner_id = $1 AND generation = $2`, owner, gen)
		return err
	}
	return nil
}

// stopIdle stops a pod no turn runs in; false when a turn began meanwhile.
func (m *Manager) stopIdle(ctx context.Context, owner uuid.UUID, reason string, now time.Time) bool {
	ok, _ := markStopping(ctx, m.Pool, owner, reason, &now)
	return ok
}

// ─── the state of the administration API ────────────────────────────

// PodInfo is a pod in the Agents section (tech §5).
type PodInfo struct {
	OwnerID        uuid.UUID `json:"ownerId"`
	OwnerKind      string    `json:"ownerKind"` // user | group
	Title          string    `json:"title"`
	Channel        string    `json:"channel,omitempty"`
	State          string    `json:"state"`
	StopReason     string    `json:"stopReason,omitempty"`
	Warm           bool      `json:"warm"`
	Busy           bool      `json:"busy"`
	Sessions       int       `json:"sessions"`
	StartedAt      time.Time `json:"startedAt"`
	LastActivityAt time.Time `json:"lastActivityAt"`
	Node           string    `json:"node,omitempty"`
	Image          string    `json:"image"`
}

// Overview is the answer of GET /admin/api/v1/agent-pods.
type Overview struct {
	Enabled  bool `json:"enabled"`
	Capacity struct {
		Max        *int       `json:"max"`
		Source     string     `json:"source"`
		Used       int        `json:"used"`
		Starting   int        `json:"starting"`
		LearnedAt  *time.Time `json:"learnedAt"`
		ProbeAfter *time.Time `json:"probeAfter"`
	} `json:"capacity"`
	Warm struct {
		Now int `json:"now"`
		// Target is nil without a ceiling and a limit: every active owner is kept warm.
		Target *int `json:"target"`
	} `json:"warm"`
	Queue struct {
		Length        int `json:"length"`
		OldestSeconds int `json:"oldestSeconds"`
	} `json:"queue"`
	Pods []PodInfo `json:"pods"`
}

// Read loads the capacity, the queue and the pods; any process may call it.
func Read(ctx context.Context, pool *pgxpool.Pool, local bool) (*Overview, error) {
	o := &Overview{Enabled: !local, Pods: []PodInfo{}}
	o.Capacity.Source = "none"
	if local {
		return o, nil
	}
	var learnedAt, probeAfter *time.Time
	var target int
	if err := pool.QueryRow(ctx, `SELECT ceiling, source, warm_target, learned_at, probe_after FROM agent_capacity`).
		Scan(&o.Capacity.Max, &o.Capacity.Source, &target, &learnedAt, &probeAfter); err != nil && !postgres.IsNoRows(err) {
		return nil, err
	}
	if target >= 0 {
		o.Warm.Target = &target
	}
	if o.Capacity.Source == "cluster" {
		o.Capacity.LearnedAt, o.Capacity.ProbeAfter = learnedAt, probeAfter
	}
	if err := pool.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(enqueued_at)), 0)::int FROM agent_queue`).
		Scan(&o.Queue.Length, &o.Queue.OldestSeconds); err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `SELECT p.owner_id, u.created_via = 'group', COALESCE(g.chat_title, u.email), COALESCE(g.channel, ''),
			p.state, COALESCE(p.stop_reason, ''), p.warm, p.busy_until IS NOT NULL AND p.busy_until > now(), p.sessions,
			p.started_at, p.last_activity_at, COALESCE(p.node, ''), p.image
		FROM agent_pods p JOIN users u ON u.id = p.owner_id
		LEFT JOIN group_agents g ON g.data_user_id = p.owner_id
		ORDER BY p.last_activity_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p PodInfo
		var group bool
		if err := rows.Scan(&p.OwnerID, &group, &p.Title, &p.Channel, &p.State, &p.StopReason, &p.Warm, &p.Busy, &p.Sessions,
			&p.StartedAt, &p.LastActivityAt, &p.Node, &p.Image); err != nil {
			return nil, err
		}
		p.OwnerKind = "user"
		if group {
			p.OwnerKind = "group"
		}
		o.Capacity.Used++
		if p.State == StateStarting {
			o.Capacity.Starting++
		}
		if p.Warm && p.State == StateReady {
			o.Warm.Now++
		}
		o.Pods = append(o.Pods, p)
	}
	return o, rows.Err()
}

// Errors of RequestStop.
var (
	ErrNoPod = errors.New("agentpods: the owner has no pod")
	ErrBusy  = errors.New("agentpods: the pod runs a turn")
)

// RequestStop marks the pod of an owner for stopping (the administrator's
// «Stop»); a pod with a running turn stops only with force (ADM-03).
func RequestStop(ctx context.Context, pool *pgxpool.Pool, owner uuid.UUID, force bool) error {
	var busy bool
	var state string
	err := pool.QueryRow(ctx, `SELECT state, busy_until IS NOT NULL AND busy_until > now() FROM agent_pods WHERE owner_id = $1`, owner).Scan(&state, &busy)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoPod
	}
	if err != nil {
		return err
	}
	if busy && !force && state != StateStopping {
		return ErrBusy
	}
	_, err = markStopping(ctx, pool, owner, StopAdmin, nil)
	return err
}

// Local is the lease of an engine without pods: every turn runs in op.
func Local(op Operator) *Lease { return &Lease{op: op} }
