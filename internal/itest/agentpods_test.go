//go:build integration

package itest

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/agentpods"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/operator"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
)

// kube is the Kubernetes API of the tests: pods with a phase, conditions and
// an address the test sets.
type kube struct {
	mu     sync.Mutex
	pods   map[string]*k8s.Pod
	quotas []map[string]string
	n      int
	// limit is a quota on pods the namespace enforces; other — the pods in
	// it that are not pods of agents.
	limit, other int
	onQuotas     func()
}

func (k *kube) CreatePod(_ context.Context, _ string, m map[string]any) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.limit > 0 && len(k.pods)+k.other >= k.limit {
		return &k8s.APIError{Status: 403, Body: `pods "agent" is forbidden: exceeded quota: agents, requested: pods=1`}
	}
	meta := m["metadata"].(map[string]any)
	p := &k8s.Pod{}
	p.Metadata.Name = meta["name"].(string)
	p.Metadata.Labels = meta["labels"].(map[string]string)
	now := time.Now()
	p.Metadata.CreationTimestamp = &now
	c := m["spec"].(map[string]any)["containers"].([]map[string]any)[0]
	p.Spec.Containers = append(p.Spec.Containers, struct {
		Image string `json:"image"`
	}{c["image"].(string)})
	p.Status.Phase = "Pending"
	if _, ok := k.pods[p.Metadata.Name]; ok {
		return &k8s.APIError{Status: 409, Body: "exists"}
	}
	k.pods[p.Metadata.Name] = p
	return nil
}

func (k *kube) DeletePod(_ context.Context, _, name string, _ int) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.pods, name)
	return nil
}

func (k *kube) ListPods(context.Context, string, string) ([]k8s.Pod, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]k8s.Pod, 0, len(k.pods))
	for _, p := range k.pods {
		out = append(out, *p)
	}
	return out, nil
}

func (k *kube) Quotas(context.Context, string) ([]map[string]string, error) {
	if k.onQuotas != nil {
		k.onQuotas()
		k.onQuotas = nil
	}
	return k.quotas, nil
}

type condition = struct {
	Type               string     `json:"type"`
	Status             string     `json:"status"`
	Reason             string     `json:"reason"`
	LastTransitionTime *time.Time `json:"lastTransitionTime"`
}

// ready makes the pod of an owner Ready on a node with an address.
func (k *kube) ready(owner uuid.UUID) {
	k.mu.Lock()
	defer k.mu.Unlock()
	p := k.pods[agentpods.PodName(owner)]
	k.n++
	p.Status.Phase, p.Status.PodIP, p.Spec.NodeName = "Running", "10.0.0."+itoa(int64(k.n)), "node-1"
	p.Status.Conditions = []condition{{Type: "Ready", Status: "True"}}
}

func (k *kube) unschedulable(owner uuid.UUID, since time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.pods[agentpods.PodName(owner)].Status.Conditions = []condition{{Type: "PodScheduled", Status: "False", Reason: "Unschedulable", LastTransitionTime: &since}}
}

func (k *kube) has(owner uuid.UUID) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	_, ok := k.pods[agentpods.PodName(owner)]
	return ok
}

func (k *kube) count() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.pods)
}

type podsEnv struct {
	t       *testing.T
	m       *agentpods.Manager
	k       *kube
	bus     *bus
	events  *capture
	now     time.Time
	mu      sync.Mutex
	expired []agentpods.Item
	told    []agentpods.Item
	typing  []agentpods.Item
}

func newPods(t *testing.T, cfg agentpods.Config) *podsEnv {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{`DELETE FROM agent_queue`, `DELETE FROM agent_pods`,
		`UPDATE agent_capacity SET learned_max = NULL, learned_at = NULL, probe_after = NULL, peak_at = NULL, capacity_at = NULL, ceiling = NULL, source = 'none'`} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	e := &podsEnv{t: t, k: &kube{pods: map[string]*k8s.Pod{}}, bus: &bus{}, events: &capture{}, now: time.Now()}
	def := agentpods.Config{Namespace: "nabu-agents", Image: "img:1", CPURequest: "100m", CPU: "1", MemoryRequest: "256Mi", Memory: "1Gi",
		Work: 1 << 30, MaxSessions: 8, SessionIdle: 15 * time.Minute, IdleTimeout: 15 * time.Minute, MinIdle: time.Minute, StartParallel: 10,
		StartTimeout: time.Minute, ScheduleTimeout: 15 * time.Second, QueueTimeout: 10 * time.Minute, TurnTimeout: 30 * time.Minute,
		WarmWindow: 3 * time.Hour, WarmShare: 0.5}
	def.PodsMax, def.WarmMax = cfg.PodsMax, cfg.WarmMax
	if cfg.WarmShare != 0 {
		def.WarmShare = cfg.WarmShare
	}
	e.m = &agentpods.Manager{Cfg: def, Pool: pool, Kube: e.k, Signer: jwt.NewSigner("https://nabu.test", key), Bus: e.bus, Events: e.events,
		Now:  func() time.Time { return e.now },
		Dial: func(string) string { return "http://127.0.0.1:1" }, // no pods behind the addresses of the tests
		Expire: func(_ context.Context, it agentpods.Item) {
			e.mu.Lock()
			e.expired = append(e.expired, it)
			e.mu.Unlock()
		},
		Notify: func(_ context.Context, it agentpods.Item) {
			e.mu.Lock()
			e.told = append(e.told, it)
			e.mu.Unlock()
		},
		Waiting: func(_ context.Context, it agentpods.Item) {
			e.mu.Lock()
			e.typing = append(e.typing, it)
			e.mu.Unlock()
		}}
	return e
}

func (e *podsEnv) step() {
	e.t.Helper()
	if err := e.m.Step(context.Background()); err != nil {
		e.t.Fatal(err)
	}
}

func (e *podsEnv) after(d time.Duration) { e.now = e.now.Add(d) }

func (e *podsEnv) turn(owner uuid.UUID, ref string) agentpods.Turn {
	return agentpods.Turn{Kind: agentpods.KindMessage, Ref: ref, Topic: "nabu.inbound", Key: owner.String(), Value: []byte(ref),
		ConversationID: uuid.New(), MessageID: uuid.New(), Channel: "web"}
}

// up brings the pod of an owner up and returns a lease of it.
func (e *podsEnv) up(owner uuid.UUID, ref string) *agentpods.Lease {
	e.t.Helper()
	ctx := context.Background()
	t := e.turn(owner, ref)
	if _, err := e.m.Acquire(ctx, owner, t); !errors.Is(err, agentpods.ErrQueued) {
		e.t.Fatalf("acquire before the pod: %v", err)
	}
	e.step()
	if !e.k.has(owner) {
		e.t.Fatal("the pod was not created")
	}
	e.k.ready(owner)
	e.step()
	l, err := e.m.Acquire(ctx, owner, t)
	if err != nil {
		e.t.Fatalf("acquire with a ready pod: %v", err)
	}
	return l
}

func (e *podsEnv) state(owner uuid.UUID) string {
	var s string
	_ = pool.QueryRow(context.Background(), `SELECT state FROM agent_pods WHERE owner_id = $1`, owner).Scan(&s)
	return s
}

func (e *podsEnv) queued(owner uuid.UUID) (n int, reason string) {
	_ = pool.QueryRow(context.Background(), `SELECT count(*), COALESCE(max(reason),'') FROM agent_queue WHERE owner_id = $1`, owner).Scan(&n, &reason)
	return
}

func (e *podsEnv) handedBack(ref string) int {
	e.bus.mu.Lock()
	defer e.bus.mu.Unlock()
	n := 0
	for _, m := range e.bus.msgs {
		if m == "nabu.inbound "+ref {
			n++
		}
	}
	return n
}

func (e *podsEnv) lastState(owner uuid.UUID) (string, any) {
	e.events.mu.Lock()
	defer e.events.mu.Unlock()
	for i := len(e.events.evs) - 1; i >= 0; i-- {
		ev := e.events.evs[i]
		if ev.Type == agentpods.AgentState && ev.UserID != nil && *ev.UserID == owner {
			d := ev.Data.(map[string]any)
			return d["state"].(string), d["position"]
		}
	}
	return "", nil
}

// LC-01, CS-02, Q-06, Q-07, Q-10, Q-12, POD-01: a turn waits for the pod of
// its owner and comes back when the pod is ready, in the order of arrival.
func TestAgentPodsColdStart(t *testing.T) {
	ctx := context.Background()
	e := newPods(t, agentpods.Config{})
	a, b := newUser(t, "pod-a@x.org"), newUser(t, "pod-b@x.org")
	m1, m2 := e.turn(a, "a-1"), e.turn(a, "a-2")
	if _, err := e.m.Acquire(ctx, a, m1); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	if n, reason := e.queued(a); n != 1 || reason != "starting" {
		t.Fatal(n, reason)
	}
	if s, _ := e.lastState(a); s != "starting" {
		t.Fatal("no agent.state: starting", s)
	}
	if _, err := e.m.Acquire(ctx, a, m2); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	e.step()
	if e.state(a) != "starting" || !e.k.has(a) || e.handedBack("a-1") != 0 {
		t.Fatal("the pod starts, nothing is handed back yet")
	}
	e.k.ready(a)
	e.step()
	if e.state(a) != "ready" || e.handedBack("a-1") != 1 || e.handedBack("a-2") != 1 {
		t.Fatal("ready: both turns go back to Kafka", e.state(a))
	}
	e.step() // Q-11: not published twice while the worker takes them
	if e.handedBack("a-1") != 1 {
		t.Fatal("published again at once")
	}
	// Q-10: the second message does not overtake the first
	if _, err := e.m.Acquire(ctx, a, m2); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal("the second turn overtook the first", err)
	}
	l1, err := e.m.Acquire(ctx, a, m1)
	if err != nil || l1.Start != "cold" || l1.Generation() == 0 || l1.Operator() == nil {
		t.Fatalf("%+v %v", l1, err)
	}
	e.step() // the second turn is handed back again
	if e.handedBack("a-2") != 2 {
		t.Fatal(e.handedBack("a-2"))
	}
	l2, err := e.m.Acquire(ctx, a, m2)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := e.queued(a); n != 0 {
		t.Fatal("the queue of the owner is not empty")
	}
	// POD-01: another owner gets another pod
	lb := e.up(b, "b-1")
	if e.k.count() != 2 || lb.Generation() == l1.Generation() {
		t.Fatal("one pod per owner")
	}
	// a turn of an owner with a ready pod runs at once: not cold
	l3, err := e.m.Acquire(ctx, b, e.turn(b, "b-2"))
	if err != nil || l3.Start != "" {
		t.Fatal(l3, err)
	}
	// LC-10 and two turns in one pod: the pod is busy until the last one ends
	l1.Release(ctx)
	var busy int
	_ = pool.QueryRow(ctx, `SELECT busy FROM agent_pods WHERE owner_id = $1`, a).Scan(&busy)
	if busy != 1 {
		t.Fatal(busy)
	}
	l2.Release(ctx)
	// LC-03, LC-09: the pod is lost — the row goes, the next turn starts a new pod of a new generation
	_ = e.k.DeletePod(ctx, "", agentpods.PodName(a), 0)
	e.step()
	if e.state(a) != "" {
		t.Fatal("the row of a lost pod stays")
	}
	l4 := e.up(a, "a-3")
	if l4.Generation() <= l1.Generation() {
		t.Fatal("the generation did not grow")
	}
	// LC-09: a pod without a row is deleted
	stray := uuid.New()
	_ = e.k.CreatePod(ctx, "", map[string]any{"metadata": map[string]any{"name": agentpods.PodName(stray),
		"labels": map[string]string{"nabu.io/owner": stray.String(), "nabu.io/generation": "1"}},
		"spec": map[string]any{"containers": []map[string]any{{"image": "img:1"}}}})
	e.step()
	if e.k.has(stray) {
		t.Fatal("a pod without a row stays")
	}
	// LC-02: 15 minutes without turns
	lb.Release(ctx)
	l3.Release(ctx)
	l4.Release(ctx)
	e.after(16 * time.Minute)
	e.step()
	e.step()
	if e.k.count() != 0 || e.state(a) != "" || e.state(b) != "" {
		t.Fatal("idle pods stay", e.k.count())
	}
}

// CAP-01, Q-01, Q-03…Q-05, Q-08, Q-13…Q-15: at the ceiling an idle pod gives
// its place away, a busy one does not, and the turn waits in the queue.
func TestAgentPodsCapacityAndQueue(t *testing.T) {
	ctx := context.Background()
	e := newPods(t, agentpods.Config{PodsMax: 2})
	a, b, c, d := newUser(t, "cap-a@x.org"), newUser(t, "cap-b@x.org"), newUser(t, "cap-c@x.org"), newUser(t, "cap-d@x.org")
	la := e.up(a, "a-1")
	lb := e.up(b, "b-1")
	e.step()
	tc := e.turn(c, "c-1")
	if _, err := e.m.Acquire(ctx, c, tc); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	if _, reason := e.queued(c); reason != "capacity" {
		t.Fatal(reason)
	}
	td := e.turn(d, "d-1")
	td.Channel = "telegram"
	if _, err := e.m.Acquire(ctx, d, td); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	e.step()
	if e.k.count() != 2 || e.state(a) != "ready" || e.state(b) != "ready" {
		t.Fatal("a busy pod was touched") // Q-04
	}
	if s, pos := e.lastState(c); s != "queued" || pos != 0 {
		t.Fatal(s, pos)
	}
	if s, pos := e.lastState(d); s != "queued" || pos != 1 { // Q-08
		t.Fatal(s, pos)
	}
	// Q-03: a pod that just finished a turn is not evicted yet
	la.Release(ctx)
	e.after(30 * time.Second)
	e.step()
	if e.state(a) != "ready" {
		t.Fatal("evicted before AGENT_POD_MIN_IDLE")
	}
	// Q-13: one message to the messenger after 30 seconds of waiting for capacity
	e.after(5 * time.Second)
	e.step()
	e.step()
	var told int
	for _, it := range e.told { // the hook of the engine writes to messengers only
		if it.Owner == d {
			told++
		}
	}
	if told != 1 {
		t.Fatalf("notified %d times", told)
	}
	// Q-01: after a minute of idling the pod of a gives its place to c
	e.after(40 * time.Second)
	e.step()
	if e.state(a) != "stopping" || e.state(b) != "ready" {
		t.Fatal(e.state(a), e.state(b))
	}
	e.step() // the pod is gone, the place goes to the first in the queue
	if e.state(a) != "" || e.state(c) != "starting" || e.state(d) != "" {
		t.Fatal(e.state(a), e.state(c), e.state(d))
	}
	if s, pos := e.lastState(d); s != "queued" || pos != 0 {
		t.Fatal(s, pos)
	}
	e.k.ready(c)
	e.step()
	lc, err := e.m.Acquire(ctx, c, tc)
	if err != nil || lc.Start != "cold" {
		t.Fatal(lc, err)
	}
	// Q-15: d never gets a place
	e.after(10 * time.Minute)
	e.step()
	if len(e.expired) != 1 || e.expired[0].Owner != d || e.expired[0].Turn.Ref != "d-1" {
		t.Fatalf("expired %+v", e.expired)
	}
	if n, _ := e.queued(d); n != 0 {
		t.Fatal("an expired turn stays in the queue")
	}
	// ADM-01…ADM-04
	o, err := agentpods.Read(ctx, pool, false)
	if err != nil || o.Capacity.Max == nil || *o.Capacity.Max != 2 || o.Capacity.Source != "config" || o.Capacity.Used != 2 || len(o.Pods) != 2 {
		t.Fatalf("%+v %v", o, err)
	}
	if err := agentpods.RequestStop(ctx, pool, c, false); !errors.Is(err, agentpods.ErrBusy) {
		t.Fatal(err)
	}
	if err := agentpods.RequestStop(ctx, pool, d, false); !errors.Is(err, agentpods.ErrNoPod) {
		t.Fatal(err)
	}
	if err := agentpods.RequestStop(ctx, pool, c, true); err != nil {
		t.Fatal(err)
	}
	e.step()
	e.step()
	if e.k.has(c) || e.state(c) != "" {
		t.Fatal("the stopped pod stays")
	}
	lb.Release(ctx)
	lc.Release(ctx)
	// LC-11: a blocked owner loses the pod and the waiting turns at once
	if _, err := e.m.Acquire(ctx, c, e.turn(c, "c-2")); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE users SET status = 'blocked' WHERE id = ANY($1)`, []uuid.UUID{b, c}); err != nil {
		t.Fatal(err)
	}
	e.step()
	e.step()
	if n, _ := e.queued(c); n != 0 || e.state(b) != "" || e.k.count() != 0 {
		t.Fatal("a blocked owner keeps a pod or a turn", n, e.state(b), e.k.count())
	}
}

// CAP-02…CAP-05, CAP-07: the ceiling from a quota and from the cluster.
func TestAgentPodsLearnedCeiling(t *testing.T) {
	ctx := context.Background()
	e := newPods(t, agentpods.Config{})
	a, b := newUser(t, "learn-a@x.org"), newUser(t, "learn-b@x.org")
	la := e.up(a, "a-1") // CAP-07: no ceiling — pods start
	if o, _ := agentpods.Read(ctx, pool, false); o.Capacity.Max != nil || o.Capacity.Source != "none" {
		t.Fatalf("%+v", o.Capacity)
	}
	tb := e.turn(b, "b-1")
	if _, err := e.m.Acquire(ctx, b, tb); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	e.step()
	e.k.unschedulable(b, e.now)
	e.after(10 * time.Second)
	e.step()
	if e.state(b) != "starting" {
		t.Fatal("given up before AGENT_SCHEDULE_TIMEOUT")
	}
	e.after(10 * time.Second)
	e.step() // CAP-03
	e.step()
	o, _ := agentpods.Read(ctx, pool, false)
	if o.Capacity.Max == nil || *o.Capacity.Max != 1 || o.Capacity.Source != "cluster" || o.Capacity.ProbeAfter == nil || e.k.has(b) {
		t.Fatalf("%+v", o.Capacity)
	}
	e.step()
	if e.state(b) != "" {
		t.Fatal("a pod above the learned ceiling before the probe time")
	}
	// CAP-05: a failed probe doubles the interval
	e.after(5*time.Minute + time.Second)
	e.step()
	if e.state(b) != "starting" {
		t.Fatal("no probe after 5 minutes")
	}
	e.k.unschedulable(b, e.now)
	e.after(16 * time.Second)
	e.step()
	e.step()
	var interval time.Duration
	_ = pool.QueryRow(ctx, `SELECT probe_interval FROM agent_capacity`).Scan(&interval)
	if interval != 10*time.Minute || e.state(b) != "" {
		t.Fatal(interval, e.state(b))
	}
	// a new message of b after the first one expired; CAP-04: the probe succeeds
	e.after(10*time.Minute + time.Second)
	e.step()
	tb = e.turn(b, "b-2")
	if _, err := e.m.Acquire(ctx, b, tb); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	e.step()
	if e.state(b) != "starting" {
		t.Fatal("no second probe")
	}
	e.k.ready(b)
	e.step()
	e.step()
	if o, _ := agentpods.Read(ctx, pool, false); *o.Capacity.Max != 2 {
		t.Fatalf("%+v", o.Capacity)
	}
	la.Release(ctx)
	// CAP-02: a quota wins when it is lower
	e2 := newPods(t, agentpods.Config{PodsMax: 50})
	e2.k.quotas = []map[string]string{{"pods": "10", "requests.memory": "2Gi"}}
	e2.step()
	if o, _ := agentpods.Read(ctx, pool, false); *o.Capacity.Max != 8 || o.Capacity.Source != "quota" || o.Warm.Target == nil || *o.Warm.Target != 4 {
		t.Fatalf("%+v %+v", o.Capacity, o.Warm)
	}
	// R10: the quota is used by other pods too (the image pre-pull) — a
	// refused pod sets the ceiling, and an idle pod gives its place away
	e3 := newPods(t, agentpods.Config{})
	e3.k.quotas = []map[string]string{{"pods": "3"}}
	e3.k.limit, e3.k.other = 3, 1
	c, d, f := newUser(t, "quota-c@x.org"), newUser(t, "quota-d@x.org"), newUser(t, "quota-f@x.org")
	lc, ld := e3.up(c, "c-1"), e3.up(d, "d-1")
	lc.Release(ctx)
	if _, err := e3.m.Acquire(ctx, f, e3.turn(f, "f-1")); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	e3.step() // the quota refuses the third pod
	if o, _ := agentpods.Read(ctx, pool, false); e3.state(f) != "" || o.Capacity.Max == nil || *o.Capacity.Max != 3 {
		t.Fatalf("%q %+v", e3.state(f), o.Capacity)
	}
	e3.step()
	if o, _ := agentpods.Read(ctx, pool, false); *o.Capacity.Max != 2 || o.Capacity.Source != "cluster" {
		t.Fatalf("%+v", o.Capacity)
	}
	e3.after(2 * time.Minute)
	e3.step()
	e3.step()
	if e3.state(c) != "" || e3.state(d) != "ready" || e3.state(f) != "starting" {
		t.Fatal("the idle pod did not give its place away", e3.state(c), e3.state(d), e3.state(f))
	}
	ld.Release(ctx)
	// WM-06 without a ceiling: every active owner is kept warm, no number to show
	e4 := newPods(t, agentpods.Config{})
	e4.step()
	if o, _ := agentpods.Read(ctx, pool, false); o.Warm.Target != nil {
		t.Fatal(*o.Warm.Target)
	}
}

// R5, LC-04: a turn whose pod went away before the answer began waits for a
// new pod and continues the same answer; R12: a turn nobody will run does
// not hold the owner back; R11: a pod that became busy is not stopped.
func TestAgentPodsRequeueAndDrop(t *testing.T) {
	ctx := context.Background()
	e := newPods(t, agentpods.Config{PodsMax: 1})
	a, b := newUser(t, "lost-a@x.org"), newUser(t, "lost-b@x.org")
	ta := e.turn(a, "a-1")
	ta.Channel = "telegram"
	if _, err := e.m.Acquire(ctx, a, ta); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	e.step()
	e.after(5 * time.Second)
	e.step() // Q-14: the typing mark while the pod starts
	if len(e.typing) == 0 || e.typing[0].Owner != a {
		t.Fatal("no typing mark while the turn waits")
	}
	e.k.ready(a)
	e.step()
	la, err := e.m.Acquire(ctx, a, ta)
	if err != nil {
		t.Fatal(err)
	}
	answer := uuid.New()
	if err := e.m.Requeue(ctx, la, ta, answer); err != nil {
		t.Fatal(err)
	}
	la.Release(ctx)
	if e.state(a) != "stopping" {
		t.Fatal("the lost pod stays", e.state(a))
	}
	e.step()
	e.step()
	if e.state(a) != "starting" {
		t.Fatal("no new pod for the turn", e.state(a))
	}
	e.k.ready(a)
	e.step()
	la2, err := e.m.Acquire(ctx, a, ta)
	if err != nil || la2.Resume != answer || la2.Attempts != 1 || la2.Generation() <= la.Generation() {
		t.Fatalf("%+v %v", la2, err)
	}
	la2.Release(ctx)
	// R12: a turn of a deleted message; the next one runs after Drop
	if _, err := pool.Exec(ctx, `INSERT INTO agent_queue (owner_id, kind, reason, ref, topic, key, value) VALUES ($1,'message','order','gone','nabu.inbound','k','v')`, a); err != nil {
		t.Fatal(err)
	}
	t2 := e.turn(a, "a-2")
	if _, err := e.m.Acquire(ctx, a, t2); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal("the turn overtook a waiting one", err)
	}
	e.m.Drop(ctx, a, "gone")
	l2, err := e.m.Acquire(ctx, a, t2)
	if err != nil {
		t.Fatal(err)
	}
	l2.Release(ctx)
	// R11: the pod idles in the rows the manager read, but a turn began in it
	// before the stop — the pod stays
	e.after(2 * time.Minute)
	if _, err := e.m.Acquire(ctx, b, e.turn(b, "b-1")); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	e.k.onQuotas = func() { // between reading the rows and choosing whom to stop
		if _, err := e.m.Acquire(ctx, a, e.turn(a, "a-3")); err != nil {
			t.Error(err)
		}
	}
	e.step()
	if e.state(a) != "ready" || !e.k.has(a) {
		t.Fatal("a pod that became busy was stopped", e.state(a))
	}
}

// R13: a run that got no capacity fails without counting towards the pause.
func TestTaskSkip(t *testing.T) {
	ctx := context.Background()
	uid := newUser(t, "skip@x.org")
	var task, run uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO scheduled_tasks (user_id, title, instruction, schedule_kind, timezone, channel, status, failures)
		VALUES ($1,'t','i','once','UTC','web','active',2) RETURNING id`, uid).Scan(&task); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO task_runs (task_id, status) VALUES ($1,'running') RETURNING id`, task).Scan(&run); err != nil {
		t.Fatal(err)
	}
	svc := &tasks.Service{Pool: pool, Events: &capture{}, MaxFailures: 3}
	if err := svc.Skip(ctx, run, "no_capacity", "no free capacity for the agent"); err != nil {
		t.Fatal(err)
	}
	var status, class, taskStatus string
	var failures int
	_ = pool.QueryRow(ctx, `SELECT r.status, r.error_class, t.status, t.failures FROM task_runs r JOIN scheduled_tasks t ON t.id = r.task_id WHERE r.id = $1`, run).
		Scan(&status, &class, &taskStatus, &failures)
	if status != "failed" || class != "no_capacity" || taskStatus != "active" || failures != 2 {
		t.Fatal(status, class, taskStatus, failures)
	}
}

// WM-01…WM-05, WM-07: the most active owners of the window keep their pods.
func TestAgentPodsWarmReserve(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `UPDATE messages SET created_at = created_at - interval '1 day'`); err != nil {
		t.Fatal(err)
	}
	e := newPods(t, agentpods.Config{PodsMax: 4}) // the reserve is 2 pods
	store := &chat.Store{Pool: pool}
	a, b, c := newUser(t, "warm-a@x.org"), newUser(t, "warm-b@x.org"), newUser(t, "warm-c@x.org")
	say := func(uid uuid.UUID, n int, ch string) {
		main, err := store.Main(ctx, uid)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			if _, err := store.AddUser(ctx, main.ID, "hi", ch, nil, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	say(a, 3, "web")
	say(b, 2, "telegram")
	say(c, 1, "web")
	say(c, 5, "task:"+uuid.NewString()) // WM-05: runs of tasks are not activity
	leases := []*agentpods.Lease{e.up(a, "a-1"), e.up(b, "b-1"), e.up(c, "c-1")}
	for _, l := range leases {
		l.Release(ctx)
	}
	e.after(5*time.Minute + time.Second)
	e.step() // the reserve is ranked
	warm := func(uid uuid.UUID) bool {
		var w bool
		_ = pool.QueryRow(ctx, `SELECT warm FROM agent_pods WHERE owner_id = $1`, uid).Scan(&w)
		return w
	}
	if !warm(a) || !warm(b) || warm(c) { // WM-01
		t.Fatal(warm(a), warm(b), warm(c))
	}
	e.after(16 * time.Minute)
	e.step()
	e.step()
	if e.state(a) != "ready" || e.state(b) != "ready" || e.state(c) != "" { // WM-02
		t.Fatal(e.state(a), e.state(b), e.state(c))
	}
	// WM-04: a lost warm pod comes back without a message
	_ = e.k.DeletePod(ctx, "", agentpods.PodName(a), 0)
	e.step()
	if e.state(a) != "" {
		t.Fatal("the row of the lost pod stays")
	}
	e.after(5*time.Minute + time.Second)
	e.step()
	if e.state(a) != "starting" || !e.k.has(a) {
		t.Fatal("the warm pod was not started again", e.state(a))
	}
	// WM-07: nothing goes to the reserve while somebody waited for capacity
	_ = e.k.DeletePod(ctx, "", agentpods.PodName(a), 0)
	e.step()
	if _, err := pool.Exec(ctx, `UPDATE agent_capacity SET capacity_at = $1`, e.now); err != nil {
		t.Fatal(err)
	}
	e.after(5*time.Minute + time.Second)
	e.step()
	if e.state(a) != "" {
		t.Fatal("a pod went to the reserve right after a wait for capacity")
	}
	// Q-02: at the ceiling the warm pod of the least active owner gives its place away
	e3 := newPods(t, agentpods.Config{PodsMax: 2, WarmShare: 1})
	e3.now = e.now
	la, lb := e3.up(a, "a-9"), e3.up(b, "b-9")
	la.Release(ctx)
	lb.Release(ctx)
	e3.after(5*time.Minute + time.Second)
	e3.step()
	if !warm(a) || !warm(b) {
		t.Fatal("both pods are warm here")
	}
	if _, err := e3.m.Acquire(ctx, c, e3.turn(c, "c-9")); !errors.Is(err, agentpods.ErrQueued) {
		t.Fatal(err)
	}
	e3.step()
	if e3.state(a) != "ready" || e3.state(b) != "stopping" {
		t.Fatal(e3.state(a), e3.state(b))
	}
}

// TOK-02, POD-05 with a real operator in the owner mode: the lease of an
// owner works only against the pod of that owner and that start.
func TestAgentPodsToken(t *testing.T) {
	ctx := context.Background()
	e := newPods(t, agentpods.Config{})
	a, b := newUser(t, "tok-a@x.org"), newUser(t, "tok-b@x.org")
	la, lb := e.up(a, "a-1"), e.up(b, "b-1")
	pub, _ := jwt.ParsePublicKey(e.m.Signer.PublicKey())
	op, err := operator.New(operator.Config{Runtime: pi.Runtime{Command: []string{"/bin/false"}}, WorkDir: t.TempDir(),
		Mode: operator.ModeOwner, Owner: a.String(), Generation: la.Generation(), PublicKey: pub})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(op.Handler())
	defer srv.Close()
	// the address of every pod leads to the pod of a, as after an address was reused
	e.m.Dial = func(string) string { return srv.URL }
	la2, err := e.m.Acquire(ctx, a, e.turn(a, "a-2"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := la2.Operator().(*agent.Client).Status(ctx)
	if err != nil || st.Owner != a.String() || st.Generation != la.Generation() {
		t.Fatalf("%+v %v", st, err)
	}
	lb2, err := e.m.Acquire(ctx, b, e.turn(b, "b-2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lb2.Operator().(*agent.Client).Status(ctx); !errors.Is(err, agent.ErrSessionGone) {
		t.Fatalf("the pod of another owner answered: %v", err)
	}
	if err := lb2.Operator().Close(ctx, "any"); err != nil { // a session that went with its pod is not an error to close
		t.Fatal(err)
	}
	if _, err := lb2.Operator().Open(ctx, agent.SessionRequest{}, nil); !errors.Is(err, agent.ErrSessionGone) || !strings.Contains(err.Error(), "token") {
		t.Fatal(err)
	}
	for _, l := range []*agentpods.Lease{la, lb, la2, lb2} {
		l.Release(ctx)
	}
}
