//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/clients"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/fakellm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/operator"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/space"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
)

type capture struct {
	mu  sync.Mutex
	evs []events.Event
}

func (c *capture) Publish(_ context.Context, e events.Event) {
	c.mu.Lock()
	c.evs = append(c.evs, e)
	c.mu.Unlock()
}

func (c *capture) types() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, e := range c.evs {
		out = append(out, e.Type)
	}
	return out
}

func toolNamed(r fakellm.Request, suffix string) string {
	for _, n := range r.ToolNames() {
		if strings.HasSuffix(n, suffix) {
			return n
		}
	}
	return ""
}

// The whole path with a real Pi (NABU_PI_CMD): engine → operator → pi → fake
// model → built-in MCP of Nabu (MEM-01, MEM-02, CONV-07, AUD-01, USE-01, TSK-07,
// SVC-02, SVC-07).
func TestEngineWithPi(t *testing.T) {
	cmd := strings.Fields(os.Getenv("NABU_PI_CMD"))
	if len(cmd) == 0 {
		t.Skip("NABU_PI_CMD is not set")
	}
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://nabu.test", key)
	hub := &capture{}
	b := &bus{}
	s3 := &mem{m: map[string][]byte{}}

	var mu sync.Mutex
	var prompts []string
	llm := &fakellm.Server{Script: func(r fakellm.Request) *fakellm.Reply {
		mu.Lock()
		prompts = append(prompts, r.LastUser())
		mu.Unlock()
		last := r.LastUser()
		if strings.Contains(last, "remember") && r.ToolResults() == 0 {
			if n := toolNamed(r, "memory_save"); n != "" {
				return &fakellm.Reply{ToolCalls: []fakellm.ToolCall{{Name: n, Args: map[string]any{"text": "Prefers green tea"}}}}
			}
			return &fakellm.Reply{Text: "no memory tool: " + strings.Join(r.ToolNames(), ",")}
		}
		if strings.Contains(last, "remember") {
			return &fakellm.Reply{Text: "Saved to memory."}
		}
		return nil // echo
	}}
	lsrv := httptest.NewServer(llm)
	defer lsrv.Close()

	op, err := operator.New(operator.Config{Runtime: pi.Runtime{Command: cmd, Options: pi.Options{Path: os.Getenv("PATH")}},
		WorkDir: t.TempDir(), ServiceToken: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithCancel(context.Background())
		cancel()
		op.Run(c)
	})
	osrv := httptest.NewServer(op.Handler())
	defer osrv.Close()

	ms := &models.Service{Pool: pool, Box: box}
	conn, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Fake", BaseURL: lsrv.URL, APIKey: "sk-fake-0001", Models: []string{"fake-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.PutPersonal(ctx, models.PersonalModels{Default: &models.Choice{ConnectionID: conn.ID, Model: "fake-1"}}); err != nil {
		t.Fatal(err)
	}
	memSvc := &memory.Service{Pool: pool, Events: hub}
	taskSvc := &tasks.Service{Pool: pool, Events: hub, Bus: b, Rules: tasks.Rules{MinInterval: 15 * time.Minute, MaxActive: 20},
		Catchup: time.Hour, Tick: time.Second, MaxFailures: 3, DefaultTimezone: "UTC"}
	spaceSvc := &space.Service{Pool: pool, S3: s3, Signer: signer, Events: hub, Quota: 1 << 20, MaxFile: 1 << 20}
	store := &chat.Store{Pool: pool}

	builtin := mcp.NewServer(signer)
	builtin.Register(memSvc.Tools()...)
	builtin.Register(spaceSvc.Tool())
	builtin.Register(taskSvc.Tools()...)
	builtin.ChannelOf = store.LastUserChannel
	ir := chi.NewRouter()
	ir.Handle("/internal/v1/mcp", builtin)
	isrv := httptest.NewServer(ir)
	defer isrv.Close()

	svcSvc := &services.Service{Pool: pool, Models: ms, Signer: signer, Bus: b, Events: hub, Harnesses: []services.Harness{{Name: "pi"}}, WorkspaceTTL: time.Hour}
	eng := &engine.Engine{
		Cfg:  engine.Config{InternalURL: isrv.URL, TurnTimeout: 2 * time.Minute, TaskRunTimeout: 2 * time.Minute, RunWorkspaceWait: time.Second, SessionMaxAge: time.Hour},
		Pool: pool, Op: &agent.Client{BaseURL: osrv.URL, Token: "svc"}, Users: repo(), Chat: store, Models: ms,
		Catalog: &catalog.Service{Pool: pool, Box: box, Signer: signer, InternalURL: isrv.URL, S3: s3},
		Memory:  memSvc, Tasks: taskSvc, Space: spaceSvc, Services: svcSvc, Ledger: &ledger.Ledger{Pool: pool},
		Signer: signer, S3: s3, Events: hub, Bus: b, Adapters: map[string]channels.Adapter{},
		Registry: &channels.Registry{Pool: pool, Box: box}, Keys: &channels.Keys{Pool: pool, Pepper: key},
	}
	uid := newUser(t, "pi@x.org")
	main, err := store.Main(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	turn := func(text string) *chat.Message {
		t.Helper()
		m, err := store.AddUser(ctx, main.ID, text, "web", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		in, _ := json.Marshal(chat.Inbound{UserID: uid, ConversationID: main.ID, MessageID: m.ID, Channel: "web"})
		if err := eng.HandleInbound(ctx, nil, in); err != nil {
			t.Fatal(err)
		}
		h, _ := store.History(ctx, main.ID, 1)
		return &h[0]
	}

	// 1. MEM-01, AUD-01: the agent saves a fact with memory_save; the step is in the answer.
	a := turn("please remember that I like green tea")
	if a.Status != "done" || a.Text != "Saved to memory." {
		t.Fatalf("answer %+v", a)
	}
	if len(a.ToolSteps) != 1 || a.ToolSteps[0].Server != "nabu" || a.ToolSteps[0].Tool != "memory_save" || a.ToolSteps[0].Status != "done" {
		t.Fatalf("steps %+v", a.ToolSteps)
	}
	recs, _ := memSvc.ForInstructions(ctx, uid)
	if len(recs) != 1 || recs[0].Source != "agent" || recs[0].Text != "Prefers green tea" {
		t.Fatalf("memory %+v", recs)
	}
	var audits, usage int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM audit WHERE user_id = $1 AND tool = 'memory_save' AND result = 'ok'`, uid).Scan(&audits)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM usage WHERE user_id = $1`, uid).Scan(&usage)
	if audits != 1 || usage == 0 {
		t.Fatalf("audit %d usage %d", audits, usage)
	}
	// CONV-07: the snapshot of the session is in S3.
	if _, ok := s3.m["sessions/"+main.ID.String()+".jsonl"]; !ok {
		t.Fatal("no session snapshot")
	}
	ts := strings.Join(hub.types(), ",")
	for _, want := range []string{"message.created", "message.delta", "tool.step", "message.done"} {
		if !strings.Contains(ts, want) {
			t.Fatalf("event %s missing: %s", want, ts)
		}
	}

	// 2. MEM-02: memory changed on the site reaches the open session with the
	// next message; a deleted record is no longer in the instructions.
	lastPrompt := func() string {
		mu.Lock()
		defer mu.Unlock()
		return prompts[len(prompts)-1]
	}
	if _, err := memSvc.Add(ctx, uid, "Works in Berlin", "user", false, nil); err != nil {
		t.Fatal(err)
	}
	a = turn("where do I work")
	if a.Status != "done" || !strings.HasPrefix(a.Text, "echo:") {
		t.Fatalf("answer %+v", a)
	}
	if p := lastPrompt(); !strings.Contains(p, "memory changed") || !strings.Contains(p, "Works in Berlin") {
		t.Fatalf("update not in the prompt: %q", p)
	}
	all, _ := memSvc.ForInstructions(ctx, uid)
	for _, r := range all {
		if r.Text == "Works in Berlin" {
			_ = memSvc.Delete(ctx, uid, r.ID)
		}
	}
	turn("and now")
	if p := lastPrompt(); !strings.Contains(p, "memory changed") || strings.Contains(p, "Works in Berlin") {
		t.Fatalf("deleted record still in the prompt: %q", p)
	}
	turn("again")
	if p := lastPrompt(); strings.Contains(p, "memory changed") {
		t.Fatalf("an unchanged memory is sent again: %q", p)
	}

	// 3. TSK-07: a task run is a separate session; only the result goes to the main conversation.
	in := tasks.CreateInput{Title: "Tea", Instruction: "say tea"}
	in.Schedule.Once = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	tk, err := taskSvc.Create(ctx, uid, in, "web", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `UPDATE scheduled_tasks SET next_run_at = now() - interval '1 minute' WHERE id = $1`, tk.ID)
	if n, err := taskSvc.Due(ctx); err != nil || n != 1 {
		t.Fatalf("due %d %v", n, err)
	}
	var runID uuid.UUID
	_ = pool.QueryRow(ctx, `SELECT id FROM task_runs WHERE task_id = $1`, tk.ID).Scan(&runID)
	payload, _ := json.Marshal(tasks.RunMessage{RunID: runID, TaskID: tk.ID, UserID: uid})
	if err := eng.HandleTaskRun(ctx, nil, payload); err != nil {
		t.Fatal(err)
	}
	var st string
	_ = pool.QueryRow(ctx, `SELECT status FROM task_runs WHERE id = $1`, runID).Scan(&st)
	h, _ := store.History(ctx, main.ID, 1)
	if st != "succeeded" || h[0].Channel != "task:"+tk.ID.String() || !strings.Contains(h[0].Text, "say tea") {
		t.Fatalf("task run %s, last message %+v", st, h[0])
	}

	// 4. SVC-02, SVC-07: a client runs a service agent; the events and the result go to the client.
	cs := &clients.Service{Pool: pool, Signer: signer}
	cl, err := cs.Create(ctx, clients.Input{Name: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svcSvc.Save(ctx, nil, "", services.Config{Name: "summarizer", Instructions: "Summarize.", Model: models.Choice{ConnectionID: conn.ID, Model: "fake-1"},
		Clients: []string{"billing"}}, true); err != nil {
		t.Fatal(err)
	}
	hc, _ := cs.Load(ctx, cl.ID)
	started, err := svcSvc.Start(ctx, hc, "summarizer", services.RunInput{Input: "invoice 42", Initiator: "Ann@x.org"})
	if err != nil {
		t.Fatal(err)
	}
	rp, _ := json.Marshal(services.RunMessage{RunID: started.RunID})
	if err := eng.HandleRun(ctx, nil, rp); err != nil {
		t.Fatal(err)
	}
	run, err := svcSvc.GetRun(ctx, started.RunID)
	if err != nil || run.Status != "succeeded" || run.Summary == nil || !strings.Contains(*run.Summary, "invoice 42") || *run.InitiatorEmail != "ann@x.org" {
		t.Fatalf("run %+v %v", run, err)
	}
	var kinds []string
	rows, _ := pool.Query(ctx, `SELECT type FROM run_events WHERE run_id = $1 ORDER BY seq`, started.RunID)
	for rows.Next() {
		var k string
		_ = rows.Scan(&k)
		kinds = append(kinds, k)
	}
	rows.Close()
	if kinds[0] != "started" || kinds[len(kinds)-1] != "completed" {
		t.Fatalf("run events %v", kinds)
	}
}
