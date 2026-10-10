package agentpods

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
)

func ip(n int) *int { return &n }

// CAP-02: the quota of the namespace gives the ceiling.
func TestQuotaCeiling(t *testing.T) {
	q := []map[string]string{{"pods": "10", "requests.memory": "2Gi"}}
	if c := quotaCeiling(q, "100m", "256Mi"); c == nil || *c != 8 {
		t.Fatal(c)
	}
	if c := quotaCeiling([]map[string]string{{"requests.cpu": "1500m"}, {"pods": "40"}}, "100m", "256Mi"); c == nil || *c != 15 {
		t.Fatal(c)
	}
	if c := quotaCeiling([]map[string]string{{"services": "3"}}, "100m", "256Mi"); c != nil {
		t.Fatal(*c)
	}
	if cpuMilli("2") != 2000 || cpuMilli("250m") != 250 || memBytes("1Gi") != 1<<30 || memBytes("500M") != 500_000_000 {
		t.Fatal("quantities")
	}
}

// CAP-06, CAP-07: the lowest of the configured, quota and learned ceilings.
func TestCeilingOf(t *testing.T) {
	if c, s := ceilingOf(0, nil, nil); c != nil || s != "none" {
		t.Fatal(c, s)
	}
	if c, s := ceilingOf(5, nil, nil); *c != 5 || s != "config" {
		t.Fatal(*c, s)
	}
	if c, s := ceilingOf(50, ip(40), ip(18)); *c != 18 || s != "cluster" {
		t.Fatal(*c, s)
	}
	if c, s := ceilingOf(50, ip(8), ip(18)); *c != 8 || s != "quota" {
		t.Fatal(*c, s)
	}
}

// WM-06, WM-08: the size of the warm reserve and the places kept for cold starts.
func TestWarmTarget(t *testing.T) {
	if w := warmTarget(ip(40), 0.3, 0); w != 12 {
		t.Fatal(w)
	}
	if w := warmTarget(ip(40), 0.3, 5); w != 5 {
		t.Fatal(w)
	}
	if w := warmTarget(nil, 0.3, 7); w != 7 {
		t.Fatal(w)
	}
	if w := warmTarget(nil, 0.3, 0); w != -1 {
		t.Fatal(w)
	}
	if reserve(40) != 4 || reserve(5) != 2 {
		t.Fatal(reserve(40), reserve(5))
	}
}

// Q-01…Q-04, LC-10: who gives its place away.
func TestVictim(t *testing.T) {
	now := time.Now()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	future, past := now.Add(time.Minute), now.Add(-time.Minute)
	id := func() uuid.UUID { return uuid.New() }
	cold5 := &podRow{owner: id(), state: "ready", lastActivity: ago(5 * time.Minute)}
	cold9 := &podRow{owner: id(), state: "ready", lastActivity: ago(9 * time.Minute)}
	warm1 := &podRow{owner: id(), state: "ready", warm: true, warmRank: ip(1), lastActivity: ago(40 * time.Minute)}
	warm3 := &podRow{owner: id(), state: "ready", warm: true, warmRank: ip(3), lastActivity: ago(10 * time.Minute)}
	busy := &podRow{owner: id(), state: "ready", lastActivity: ago(30 * time.Minute), busyUntil: &future}
	fresh := &podRow{owner: id(), state: "ready", lastActivity: ago(20 * time.Second)}
	starting := &podRow{owner: id(), state: "starting", lastActivity: ago(time.Hour)}

	if v := victim([]*podRow{warm1, cold5, cold9, busy, fresh, starting}, now, time.Minute); v != cold9 {
		t.Fatal("the longest idle pod outside the reserve goes first")
	}
	if v := victim([]*podRow{warm1, warm3, busy, fresh}, now, time.Minute); v != warm3 {
		t.Fatal("then the warm pod of the least active owner")
	}
	if v := victim([]*podRow{busy, fresh, starting}, now, time.Minute); v != nil {
		t.Fatal("a busy, a fresh and a starting pod stay")
	}
	crashed := &podRow{owner: id(), state: "ready", lastActivity: ago(5 * time.Minute), busyUntil: &past}
	if v := victim([]*podRow{crashed}, now, time.Minute); v != crashed {
		t.Fatal("a busy mark in the past does not hold the pod")
	}
}

// Q-08, Q-09: owners by their first waiting turn, people before tasks.
func TestOrder(t *testing.T) {
	now := time.Now()
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	item := func(o uuid.UUID, kind string, ago time.Duration) *queued {
		return &queued{Item: Item{Owner: o, Turn: Turn{Kind: kind}, EnqueuedAt: now.Add(-ago)}}
	}
	ws := order([]*queued{item(a, KindTask, 9*time.Minute), item(b, KindMessage, 2*time.Minute), item(c, KindMessage, 5*time.Minute),
		item(b, KindMessage, time.Minute)})
	if len(ws) != 3 || ws[0].owner != c || ws[1].owner != b || ws[2].owner != a || len(ws[1].items) != 2 {
		t.Fatalf("%+v", ws)
	}
}

// POD-06: the pod carries no secret and no rights.
func TestManifest(t *testing.T) {
	m := &Manager{Signer: jwt.NewSigner("https://nabu.test", []byte("0123456789abcdef0123456789abcdef")),
		Cfg: Config{Namespace: "nabu-agents", Image: "img:1", CPURequest: "100m", CPU: "1", MemoryRequest: "256Mi", Memory: "1Gi",
			Work: 1 << 30, MaxSessions: 8, SessionIdle: 15 * time.Minute}}
	owner := uuid.New()
	b, _ := json.Marshal(m.manifest(owner, 42))
	s := string(b)
	for _, want := range []string{`"automountServiceAccountToken":false`, `"readOnlyRootFilesystem":true`, `"runAsNonRoot":true`,
		`"allowPrivilegeEscalation":false`, `"drop":["ALL"]`, `"name":"AGENT_MODE","value":"owner"`, `"value":"42"`, owner.String(),
		`"priorityClassName":"nabu-agent-pod"`, `"memory":"1Gi"`, `"cpu":"100m"`, `"namespace":"nabu-agents"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("%s not in %s", want, s)
		}
	}
	for _, secret := range []string{"TOKEN", "SECRET", "PASSWORD", "LLM"} {
		if strings.Contains(s, secret) {
			t.Fatalf("the manifest mentions %s", secret)
		}
	}
	if !strings.HasPrefix(PodName(owner), "agent-") || len(PodName(owner)) != 26 {
		t.Fatal(PodName(owner))
	}
}
