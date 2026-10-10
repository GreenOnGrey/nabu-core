//go:build integration

// Package itest runs the repositories against a real Postgres: a ready server
// from NABU_TEST_DATABASE_URL, or a dockertest container.
// Run with: go test -tags integration -count=1 ./internal/itest/
package itest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	neturl "net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v4"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/clients"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/space"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	url := os.Getenv("NABU_TEST_DATABASE_URL")
	var stop func()
	if url == "" {
		dp, err := dockertest.NewPool(ctx, "", dockertest.WithMaxWait(2*time.Minute))
		if err != nil {
			fmt.Println("docker unavailable:", err)
			os.Exit(1)
		}
		res, err := dp.Run(ctx, "postgres", dockertest.WithTag("16"), dockertest.WithoutReuse(),
			dockertest.WithEnv([]string{"POSTGRES_PASSWORD=pw", "POSTGRES_DB=nabu"}))
		if err != nil {
			fmt.Println("start postgres:", err)
			os.Exit(1)
		}
		url = fmt.Sprintf("postgres://postgres:pw@%s/nabu?sslmode=disable", res.GetHostPort("5432/tcp"))
		stop = func() { _ = dp.Close(ctx) }
		if err := dp.Retry(ctx, time.Minute, func() error {
			p, err := postgres.Connect(ctx, url)
			if err != nil {
				return err
			}
			defer p.Close()
			return p.Ping(ctx)
		}); err != nil {
			fmt.Println("postgres:", err)
			stop()
			os.Exit(1)
		}
	}
	// Every run gets a fresh database on the server.
	admin, err := postgres.Connect(ctx, url)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	name := "nabu_itest_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	u, _ := neturl.Parse(url)
	u.Path = "/" + name
	url = u.String()
	if err := postgres.Migrate(ctx, url); err != nil {
		fmt.Println("migrate:", err)
		os.Exit(1)
	}
	if pool, err = postgres.Connect(ctx, url); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	code := m.Run()
	pool.Close()
	_, _ = admin.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
	admin.Close()
	if stop != nil {
		stop()
	}
	os.Exit(code)
}

type bus struct {
	mu   sync.Mutex
	msgs []string
}

func (b *bus) Publish(_ context.Context, topic, key string, v []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.msgs = append(b.msgs, topic+" "+string(v))
	return nil
}

type mem struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (s *mem) Put(_ context.Context, k string, r io.Reader, _ int64, _ string) error {
	b, err := io.ReadAll(r)
	s.mu.Lock()
	s.m[k] = b
	s.mu.Unlock()
	return err
}
func (s *mem) Get(_ context.Context, k string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[k]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return io.NopCloser(strings.NewReader(string(b))), nil
}
func (s *mem) Delete(_ context.Context, k string) error {
	s.mu.Lock()
	delete(s.m, k)
	s.mu.Unlock()
	return nil
}
func (s *mem) List(context.Context, string) ([]storage.Object, error) { return nil, nil }

var key = []byte("0123456789abcdef0123456789abcdef")

func repo() *users.Repo {
	return &users.Repo{Pool: pool, DefaultLanguage: "en", DefaultTimezone: "Europe/Moscow", SpaceQuota: 1000}
}

// AUTH-01, AUTH-05, AUTH-06, AUTH-07
func TestSignIn(t *testing.T) {
	ctx := context.Background()
	r := repo()
	u, created, err := r.SignIn(ctx, users.Identity{Issuer: "github", Subject: "1", Email: "A@x.org", Name: "Ann"}, false)
	if err != nil || !created || u.Email != "a@x.org" || u.IsAdmin {
		t.Fatalf("%+v %v %v", u, created, err)
	}
	ag, err := r.GetAgent(ctx, u.ID)
	if err != nil || ag.Name != "Nabu" || ag.Tone != "business" {
		t.Fatalf("agent %+v %v", ag, err)
	}
	u2, created, err := r.SignIn(ctx, users.Identity{Issuer: "github", Subject: "1", Email: "ann@new.org"}, true)
	if err != nil || created || u2.ID != u.ID || u2.Email != "ann@new.org" || !u2.IsAdmin {
		t.Fatalf("relogin %+v %v %v", u2, created, err)
	}
	inv, err := r.Invite(ctx, "boss@x.org", true)
	if err != nil || inv.Status != "invited" {
		t.Fatal(inv, err)
	}
	u3, _, err := r.SignIn(ctx, users.Identity{Issuer: "https://kc", Subject: "s9", Email: "boss@x.org"}, false)
	if err != nil || u3.ID != inv.ID || !u3.IsAdmin || u3.Status != "active" {
		t.Fatalf("invite %+v %v", u3, err)
	}
	d, err := r.EnsureByEmail(ctx, "new@x.org")
	if err != nil || d.CreatedVia != "delegation" {
		t.Fatal(d, err)
	}
	list, err := r.List(ctx, "x.org", "", 0, 500)
	n := 0
	for _, u := range list {
		if u.Email == "ann@new.org" || u.Email == "boss@x.org" || u.Email == "new@x.org" {
			n++
		}
	}
	if err != nil || n != 2 {
		t.Fatal(len(list), err)
	}
	if _, err := r.AdminPatch(ctx, u.ID, d.ID, nil, ptr(true)); err != nil {
		t.Fatal(err)
	}
	if r.Allowed(ctx, d.ID) {
		t.Fatal("blocked user allowed")
	}
}

func ptr[T any](v T) *T { return &v }

func newUser(t *testing.T, email string) uuid.UUID {
	u, _, err := repo().SignIn(context.Background(), users.Identity{Issuer: "t", Subject: email, Email: email}, false)
	if err != nil {
		t.Fatal(err)
	}
	return u.ID
}

// MEM-01, MEM-03, MEM-04
func TestMemory(t *testing.T) {
	ctx := context.Background()
	m := &memory.Service{Pool: pool}
	a, b := newUser(t, "m1@x.org"), newUser(t, "m2@x.org")
	if _, err := m.Add(ctx, a, "Prefers reports on Mondays", "agent", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add(ctx, b, "Reports go to CFO", "user", false, nil); err != nil {
		t.Fatal(err)
	}
	l, err := m.List(ctx, a, "reports", httpx.Page{Limit: 10})
	if err != nil || len(l.Items) != 1 || l.Items[0].Source != "agent" {
		t.Fatalf("%+v %v", l, err)
	}
	if err := m.Clear(ctx, a, ""); err == nil {
		t.Fatal("clear without confirmation")
	}
	if err := m.Clear(ctx, a, "all"); err != nil {
		t.Fatal(err)
	}
	l, _ = m.List(ctx, a, "", httpx.Page{Limit: 10})
	if len(l.Items) != 0 {
		t.Fatal("not cleared")
	}
}

// TSK-03, TSK-05, TSK-08, TSK-11, TSK-12
func TestTasks(t *testing.T) {
	ctx := context.Background()
	b := &bus{}
	s := &tasks.Service{Pool: pool, Bus: b, Rules: tasks.Rules{MinInterval: 15 * time.Minute, MaxActive: 3}, Catchup: time.Hour,
		Tick: time.Second, MaxFailures: 3, DefaultTimezone: "Europe/Moscow"}
	box, _ := crypto.NewBox(key)
	s.Deliverable = (&engine.Engine{Pool: pool, Users: repo(), Registry: &channels.Registry{Pool: pool, Box: box},
		Keys: &channels.Keys{Pool: pool, Pepper: key}}).Deliverable
	uid := newUser(t, "t1@x.org")
	in := tasks.CreateInput{Title: "Metrics", Instruction: "Send a metrics summary"}
	in.Schedule.Cron = "*/5 * * * *"
	if _, err := s.Create(ctx, uid, in, "web", nil); err == nil || !strings.Contains(err.Error(), "minimum") {
		t.Fatalf("5 minutes: %v", err)
	}
	in.Schedule.Cron = "0 10 * * 1"
	tk, err := s.Create(ctx, uid, in, "web", nil)
	if err != nil || tk.NextRunAt == nil || tk.Schedule.Human != "Every Monday at 10:00" {
		t.Fatalf("%+v %v", tk, err)
	}
	in2 := tasks.CreateInput{Title: "Review", Instruction: "Remind about the review"}
	in2.Schedule.Once = "2020-01-01T10:00"
	if _, err := s.Create(ctx, uid, in2, "web", nil); err == nil {
		t.Fatal("past time accepted")
	}
	in2.Schedule.Once = time.Now().Add(time.Hour).Format("2006-01-02T15:04")
	// a channel that is switched off is not available; switched on — not linked yet
	reg := &channels.Registry{Pool: pool, Box: box}
	on, off := true, false
	if _, err := reg.Update(ctx, "telegram", channels.Patch{Enabled: &off}, nil); err != nil {
		t.Fatal(err)
	}
	s.Deliverable = (&engine.Engine{Pool: pool, Users: repo(), Registry: reg, Keys: &channels.Keys{Pool: pool, Pepper: key}}).Deliverable
	if _, err := s.Create(ctx, uid, in2, "telegram", nil); err == nil || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("telegram off: %v", err)
	}
	if _, err := reg.Update(ctx, "telegram", channels.Patch{Enabled: &on, AllUsers: &on}, nil); err != nil {
		t.Fatal(err)
	}
	s.Deliverable = (&engine.Engine{Pool: pool, Users: repo(), Registry: reg, Keys: &channels.Keys{Pool: pool, Pepper: key}}).Deliverable
	if _, err := s.Create(ctx, uid, in2, "telegram", nil); err == nil || !strings.Contains(err.Error(), "not linked") {
		t.Fatalf("telegram unlinked: %v", err)
	}
	// make it due 3 hours ago: only the latest occurrence inside the window runs
	_, _ = pool.Exec(ctx, `UPDATE scheduled_tasks SET cron = '0 * * * *', next_run_at = now() - interval '3 hours' WHERE id = $1`, tk.ID)
	var wg sync.WaitGroup
	counts := make([]int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); counts[i], _ = s.Due(ctx) }(i)
	}
	wg.Wait()
	if counts[0]+counts[1] != 1 {
		t.Fatalf("runs planned: %v", counts)
	}
	var runID uuid.UUID
	_ = pool.QueryRow(ctx, `SELECT id FROM task_runs WHERE task_id = $1`, tk.ID).Scan(&runID)
	for i := 0; i < 3; i++ {
		if i > 0 {
			_ = pool.QueryRow(ctx, `INSERT INTO task_runs (task_id, scheduled_for, status) VALUES ($1, now() + make_interval(mins => $2), 'running') RETURNING id`, tk.ID, i).Scan(&runID)
		}
		if err := s.Finish(ctx, runID, false, "", "unavailable", "confluence is down", nil); err != nil {
			t.Fatal(err)
		}
	}
	if p, reason := s.PausedNow(ctx, tk.ID); !p || reason != "confluence is down" {
		t.Fatalf("paused %v %q", p, reason)
	}
	got, err := s.Resume(ctx, uid, tk.ID)
	if err != nil || got.Status != "active" || got.Failures != 0 {
		t.Fatalf("%+v %v", got, err)
	}
	if got, err = s.Cancel(ctx, uid, tk.ID); err != nil || got.Status != "cancelled" {
		t.Fatal(got, err)
	}
	l, err := s.List(ctx, uid, "cancelled", httpx.Page{Limit: 10})
	if err != nil || len(l.Items) != 1 || l.Items[0].LastRun == nil {
		t.Fatalf("%+v %v", l, err)
	}
}

// SPC-05, file manifest
func TestSpace(t *testing.T) {
	ctx := context.Background()
	s3 := &mem{m: map[string][]byte{}}
	sp := &space.Service{Pool: pool, S3: s3, Signer: jwt.NewSigner("https://n", key), Quota: 1000, MaxFile: 1 << 20}
	uid := newUser(t, "s1@x.org")
	if _, err := sp.Put(ctx, uid, "a/b.txt", strings.NewReader("hello"), 5, "agent"); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Put(ctx, uid, "../x", strings.NewReader("x"), 1, "agent"); err == nil {
		t.Fatal("escape accepted")
	}
	if _, err := sp.Put(ctx, uid, "big", strings.NewReader(strings.Repeat("x", 2000)), -1, "user"); err == nil || !strings.Contains(err.Error(), "quota") {
		t.Fatalf("quota: %v", err)
	}
	info, _ := sp.Info(ctx, uid)
	if info.UsedBytes != 5 || info.Files != 1 {
		t.Fatalf("%+v", info)
	}
	if err := sp.Delete(ctx, uid, "a/b.txt"); err != nil {
		t.Fatal(err)
	}
	info, _ = sp.Info(ctx, uid)
	if info.UsedBytes != 0 {
		t.Fatal(info)
	}
}

// SVC-02/03/04/09, catalog, import-like flow, ledger
func TestClientsAndRuns(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://n", key)
	ms := &models.Service{Pool: pool, Box: box}
	conn, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "LiteLLM", BaseURL: "http://litellm:4000", APIKey: "sk-123456789", Models: []string{"gpt-x"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.PutPersonal(ctx, models.PersonalModels{Default: &models.Choice{ConnectionID: conn.ID, Model: "gpt-x"}}); err != nil {
		t.Fatal(err)
	}
	spec, k, err := ms.ResolvePersonal(ctx, nil, nil)
	if err != nil || spec.ModelID != "gpt-x" || k != "sk-123456789" {
		t.Fatal(spec, err)
	}
	cs := &clients.Service{Pool: pool, Signer: signer}
	cl, err := cs.Create(ctx, clients.Input{Name: "hammurapi", CanDelegate: ptr(true)})
	if err != nil {
		t.Fatal(err)
	}
	b := &bus{}
	ss := &services.Service{Pool: pool, Models: ms, Signer: signer, Bus: b, Harnesses: []services.Harness{{Name: "pi"}}, WorkspaceTTL: time.Hour}
	if _, err := ss.Save(ctx, nil, "", services.Config{Name: "hammurapi-codegen", Model: models.Choice{ConnectionID: conn.ID, Model: "gpt-x"},
		Workspace: "external", Clients: []string{"hammurapi"}}, true); err != nil {
		t.Fatal(err)
	}
	hc, _ := cs.Load(ctx, cl.ID)
	if len(hc.Agents) != 1 {
		t.Fatalf("agents %v", hc.Agents)
	}
	if _, err := ss.Start(ctx, hc, "other", services.RunInput{Input: "x"}); err == nil {
		t.Fatal("not allowed agent started")
	}
	st, err := ss.Start(ctx, hc, "hammurapi-codegen", services.RunInput{Input: "do it", IdempotencyKey: "task:1:1"})
	if err != nil || st.WorkspaceToken == "" {
		t.Fatal(st, err)
	}
	st2, err := ss.Start(ctx, hc, "hammurapi-codegen", services.RunInput{Input: "do it", IdempotencyKey: "task:1:1"})
	if err != nil || st2.RunID != st.RunID || len(b.msgs) != 1 {
		t.Fatalf("idempotency %v %v %d", st2, err, len(b.msgs))
	}
	ss.AppendEvent(ctx, st.RunID, "started", map[string]any{})
	ss.AppendEvent(ctx, st.RunID, "completed", map[string]any{"status": "succeeded"})
	// secret reissue keeps the old secret for a day
	re, err := cs.Reissue(ctx, cl.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Authenticate(ctx, "hammurapi", cl.Secret); err != nil {
		t.Fatal("old secret refused")
	}
	if _, err := cs.Authenticate(ctx, "hammurapi", re.Secret); err != nil {
		t.Fatal("new secret refused")
	}
	// catalog
	cat := &catalog.Service{Pool: pool, Box: box, Signer: signer, InternalURL: "http://api:8081"}
	mode := "platform"
	headers := map[string]string{"Authorization": "Bearer grafana-token"}
	it, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "grafana", Source: &catalog.Source{Kind: "url", URL: "https://g/mcp"},
		Mode: &mode, Published: ptr(true), PlatformAuth: &headers})
	if err != nil || it.PlatformAuth["Authorization"] != "…oken" {
		t.Fatalf("%+v %v", it, err)
	}
	if err := cat.StarterCatalog(ctx); err != nil {
		t.Fatal(err)
	}
	uid := newUser(t, "c1@x.org")
	if _, err := cat.Connect(ctx, uid, it.ID, ""); err != nil {
		t.Fatal(err)
	}
	sm, err := cat.ForSession(ctx, uid, "web", "", time.Hour)
	if err != nil || len(sm.Servers) != 1 || !strings.Contains(sm.Servers[0].URL, "/internal/v1/mcp-proxy/") {
		t.Fatalf("%+v %v", sm, err)
	}
	items, _ := cat.ForUser(ctx, uid)
	for _, i := range items {
		if strings.Contains(strings.ToLower(i.Name+i.Title), "hammurapi") {
			t.Fatal("HMR-09: Hammurapi appears in the catalog")
		}
	}
	// a delegation item is checked with a Nabu JWT for the administrator
	var sent map[string]string
	cat.CheckMCP = func(_ context.Context, req agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
		sent = req.Headers
		return &agent.MCPCheckResponse{OK: true}, nil
	}
	pmode := "personal"
	di, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "product", Source: &catalog.Source{Kind: "url", URL: "https://p/mcp"},
		Mode: &pmode, PersonalAuth: &catalog.PersonalAuth{Kind: "delegation", Audience: "hammurapi"}})
	if err != nil {
		t.Fatal(err)
	}
	if di.ReadOnly { // the product applies the rights of the user: its tools that change data are allowed by default
		t.Fatal("a delegation item is read-only by default")
	}
	if di, err = cat.Check(ctx, di.ID, "admin@x.org"); err != nil || di.Status == nil || *di.Status != "ok" {
		t.Fatalf("%+v %v", di, err)
	}
	if c, err := signer.Verify(strings.TrimPrefix(sent["Authorization"], "Bearer "), "hammurapi"); err != nil || c.Email != "admin@x.org" || sent[catalog.DelegationHeader] != "admin@x.org" {
		t.Fatalf("delegation check headers %v: %+v %v", sent, c, err)
	}
	// ledger
	l := &ledger.Ledger{Pool: pool}
	if err := l.Partitions(ctx, 365*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	srv := "clickhouse"
	l.Audit(ctx, ledger.Entry{AgentKind: "personal", Agent: "Nabu", UserID: &uid, Channel: "web", Server: &srv, Tool: "query", Result: "ok"}, `{"q":"select","t":"grafana-token"}`, []string{"grafana-token"})
	list, err := l.List(ctx, ledger.Filter{From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour), User: &uid}, httpx.Page{Limit: 10})
	if err != nil || len(list.Items) != 1 || strings.Contains(*list.Items[0].ArgsSummary, "grafana-token") {
		t.Fatalf("%+v %v", list, err)
	}
	if err := l.Partitions(ctx, 365*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if g, err := l.UsageReport(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), "user"); err != nil {
		t.Fatal(g, err)
	}
}

// CONV-05/06 and the chat store
func TestChat(t *testing.T) {
	ctx := context.Background()
	st := &chat.Store{Pool: pool}
	uid := newUser(t, "ch@x.org")
	main, err := st.Main(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	topic, err := st.CreateTopic(ctx, uid, "Quarterly report")
	if err != nil {
		t.Fatal(err)
	}
	m, err := st.AddUser(ctx, main.ID, "hello", "telegram", nil, json.RawMessage(`{"type":"feature"}`))
	if err != nil {
		t.Fatal(err)
	}
	a, err := st.AddAssistant(ctx, main.ID, "telegram", &m.ID, "gpt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Finish(ctx, a.ID, "hi", "done", []chat.ToolStep{{ID: "1", Server: "jira", Tool: "create", Status: "done"}}, "", ""); err != nil {
		t.Fatal(err)
	}
	h, _ := st.History(ctx, main.ID, 10)
	if len(h) != 2 || h[0].Role != "user" || h[1].ToolSteps[0].Server != "jira" {
		t.Fatalf("%+v", h)
	}
	if st.LastUserChannel(ctx, main.ID) != "telegram" {
		t.Fatal("channel")
	}
	_, _ = pool.Exec(ctx, `UPDATE conversations SET created_at = now() - interval '20 days' WHERE id = $1`, topic.ID)
	ids, err := st.ArchiveIdle(ctx, 14*24*time.Hour)
	if err != nil || len(ids) == 0 {
		t.Fatal(ids, err)
	}
	l, _ := st.List(ctx, uid, true)
	if len(l) != 1 || l[0].ID != topic.ID {
		t.Fatalf("%+v", l)
	}
	if _, err := st.Patch(ctx, uid, main.ID, nil, ptr(true)); err == nil {
		t.Fatal("main archived")
	}
}
