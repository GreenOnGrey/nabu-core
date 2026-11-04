//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/clients"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/fakellm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/operator"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/k8s"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/space"
	"github.com/GreenOnGrey/nabu-core/internal/workspace"
)

// FTR.HMR.CMN-0008 (CHK-11…CHK-28, CHK-40) and FTR.NAB.CMN-0010 (STL-09, STL-10,
// STL-11, STL-16): the engine, a real Pi with a fake model, Postgres, a real
// workspace server behind a fake relay and a fake Kubernetes API.
func TestRunCheckWithPi(t *testing.T) {
	cmd := strings.Fields(os.Getenv("NABU_PI_CMD"))
	if len(cmd) == 0 {
		t.Skip("NABU_PI_CMD is not set")
	}
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://nabu.test", key)
	root := t.TempDir()

	llm := &fakellm.Server{Script: func(r fakellm.Request) *fakellm.Reply {
		first, last := r.FirstUser(), r.LastUser()
		switch {
		case strings.Contains(last, "Stop: you have repeated"):
			if strings.Contains(first, "spin-tool") {
				return &fakellm.Reply{ToolCalls: []fakellm.ToolCall{{Name: "ls", Args: map[string]any{"path": "."}}}}
			}
			return &fakellm.Reply{Text: "wrapped up"}
		case strings.Contains(first, "spin"):
			return &fakellm.Reply{ToolCalls: []fakellm.ToolCall{{Name: "ls", Args: map[string]any{"path": "."}}}}
		case strings.Contains(last, "readiness check"):
			if strings.Contains(first, "fix-me") {
				_ = os.WriteFile(filepath.Join(root, "fixed"), nil, 0o644)
			}
			return &fakellm.Reply{Text: "repaired"}
		case strings.Contains(first, "probe") && r.ToolResults() == 0:
			return &fakellm.Reply{ToolCalls: []fakellm.ToolCall{{Name: "bash", Args: map[string]any{"command": "exit 3"}}}}
		case strings.Contains(first, "probe") && r.ToolResults() == 1:
			return &fakellm.Reply{ToolCalls: []fakellm.ToolCall{{Name: "grep", Args: map[string]any{"pattern": "no-such-text-anywhere"}}}}
		}
		return &fakellm.Reply{Text: "done. Implemented: R1"}
	}}
	lsrv := httptest.NewServer(llm)
	defer lsrv.Close()
	ext, _ := filepath.Abs(filepath.Join("..", "..", "pi-extensions", "nabu-workspace"))
	op, err := operator.New(operator.Config{Runtime: pi.Runtime{Command: cmd, Options: pi.Options{Path: os.Getenv("PATH"), ExtensionDir: ext}},
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

	// The sandbox: a real workspace server; only what the run needs of the relay.
	var changed atomic.Bool
	changed.Store(true)
	ws := &workspace.Workspace{Root: root, Token: "ws", Env: os.Environ(),
		Changed: func() (bool, error) { return changed.Load(), nil },
		Changes: func(context.Context) (any, error) { return map[string]int{"files": 1}, nil }}
	handler := ws.Handler()
	relay := chi.NewRouter()
	relay.Post("/internal/v1/workspaces/{id}/v1/*", func(w http.ResponseWriter, r *http.Request) {
		if _, err := signer.Verify(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), jwt.AudCall); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		r.URL.Path = "/v1/" + chi.URLParam(r, "*")
		r.Header.Set("Authorization", "Bearer ws")
		handler.ServeHTTP(w, r)
	})
	rsrv := httptest.NewServer(relay)
	defer rsrv.Close()
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer kube.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tokenFile, []byte("t"), 0o600)
	kc := k8s.NewInCluster()
	kc.APIServer, kc.TokenFile = kube.URL, tokenFile

	ms := &models.Service{Pool: pool, Box: box}
	conn, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Check LLM", BaseURL: lsrv.URL, APIKey: "sk-fake-check", Models: models.Refs("fake-1")})
	if err != nil {
		t.Fatal(err)
	}
	store := &mem{m: map[string][]byte{}}
	ss := &services.Service{Pool: pool, Models: ms, Signer: signer, Bus: &bus{}, Harnesses: []services.Harness{{Name: "pi"}},
		Files: &services.Files{Store: store, Cfg: services.FilesConfig{MaxFiles: 10, MaxBytes: 1 << 20, UploadTTL: time.Hour}}}
	spaceSvc := &space.Service{Pool: pool, S3: store, Signer: signer, Quota: 1 << 20, MaxFile: 1 << 20, Enabled: true}
	eng := &engine.Engine{
		Cfg: engine.Config{RelayInternalURL: rsrv.URL, TurnTimeout: time.Minute, TaskRunTimeout: time.Minute, SessionMaxAge: time.Hour, RunChangesTimeout: 10 * time.Second,
			StallDetector: true, StallRepeatLimit: 5, StallErrorLimit: 5, StallWrapupTimeout: 20 * time.Second},
		Pool: pool, Op: &agent.Client{BaseURL: osrv.URL, Token: "svc"}, Users: repo(), Models: ms,
		Catalog: &catalog.Service{Pool: pool, Box: box, Signer: signer, S3: store}, Space: spaceSvc, Services: ss, Ledger: &ledger.Ledger{Pool: pool},
		Signer: signer, S3: store, Events: &capture{}, Bus: &bus{},
		Sandboxes: &space.Sandboxes{Pool: pool, K8s: kc, Space: spaceSvc, Cfg: space.SandboxConfig{Namespace: "n", Image: "i"}},
		Connected: func(context.Context, string) bool { return true },
	}
	cs := &clients.Service{Pool: pool, Signer: signer}
	cl, err := cs.Create(ctx, clients.Input{Name: "check-client"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Save(ctx, nil, "", services.Config{Name: "check-agent", Model: models.Choice{ConnectionID: conn.ID, Model: "fake-1"}, Workspace: "nabu",
		Limits: services.Limits{TimeoutSec: 60, MaxTokens: 1 << 30}, Clients: []string{"check-client"}}, true); err != nil {
		t.Fatal(err)
	}
	hc, _ := cs.Load(ctx, cl.ID)
	start := func(t *testing.T, input string, check *services.RunCheck, withCopy bool) uuid.UUID {
		t.Helper()
		in := services.RunInput{Input: input, Check: check}
		if withCopy {
			up := uuid.New()
			if _, err := ss.Upload(ctx, cl.ID, up, strings.NewReader(string(tarGz(t, map[string]string{"a.go": "package a"})))); err != nil {
				t.Fatal(err)
			}
			in.Files = up.String()
		}
		st, err := ss.Start(ctx, hc, "check-agent", in)
		if err != nil {
			t.Fatal(err)
		}
		return st.RunID
	}
	handle := func(t *testing.T, id uuid.UUID) *services.Run {
		t.Helper()
		p, _ := json.Marshal(services.RunMessage{RunID: id})
		if err := eng.HandleRun(ctx, nil, p); err != nil {
			t.Fatal(err)
		}
		run, err := ss.GetRun(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	run := func(t *testing.T, input string, check *services.RunCheck) *services.Run {
		t.Helper()
		return handle(t, start(t, input, check, false))
	}
	events := func(id uuid.UUID, typ string) int {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id = $1 AND type = $2`, id, typ).Scan(&n)
		return n
	}

	// CHK-10: without a check block nothing changes.
	if r := run(t, "plain", nil); r.Status != "succeeded" || r.Check != nil || r.StopReason != nil {
		t.Fatalf("CHK-10: %+v", r)
	}
	// CHK-11, CHK-12: the code decides, not the words of the model.
	r := run(t, "claims success", &services.RunCheck{Command: "true"})
	if r.Status != "succeeded" || r.Check.Status != services.CheckPassed || r.Check.Attempts != 1 || events(r.ID, "check_finished") != 1 {
		t.Fatalf("CHK-11: %+v %+v", r, r.Check)
	}
	r = run(t, "claims success", &services.RunCheck{Command: "echo boom; exit 1"})
	if r.Status != "succeeded" || r.Check.Status != services.CheckFailed || !strings.Contains(r.Check.OutputTail, "boom") {
		t.Fatalf("CHK-12: %+v %+v", r, r.Check)
	}
	// CHK-17: the agent repairs in the first round, in the same session.
	r = run(t, "fix-me", &services.RunCheck{Command: "test -f fixed", MaxRepairs: 2})
	if r.Check.Status != services.CheckPassed || r.Check.Attempts != 2 || !strings.Contains(*r.Summary, "repaired") || !strings.Contains(*r.Summary, "Implemented: R1") {
		t.Fatalf("CHK-17: %+v %+v", r, r.Check)
	}
	// CHK-18: the rounds run out; the run stays a success.
	r = run(t, "never", &services.RunCheck{Command: "false", MaxRepairs: 2})
	if r.Status != "succeeded" || r.Check.Status != services.CheckFailed || r.Check.Attempts != 3 || events(r.ID, "check_started") != 3 {
		t.Fatalf("CHK-18: %+v %+v", r, r.Check)
	}
	// CHK-22: the command cannot run: no repair round.
	r = run(t, "missing", &services.RunCheck{Command: "no-such-command-anywhere", MaxRepairs: 2})
	if r.Check.Status != services.CheckUnavailable || r.Check.Reason != services.CheckReasonNotRunnable || r.Check.Attempts != 1 {
		t.Fatalf("CHK-22: %+v", r.Check)
	}
	// CHK-23: the command is too slow.
	r = run(t, "slow", &services.RunCheck{Command: "sleep 30", TimeoutSec: 10, MaxRepairs: 2})
	if r.Check.Status != services.CheckUnavailable || r.Check.Reason != services.CheckReasonTimeout || r.Check.Attempts != 1 {
		t.Fatalf("CHK-23: %+v", r.Check)
	}
	// CHK-14: an unchanged working copy; the changes are still collected.
	changed.Store(false)
	id := start(t, "answers in words", &services.RunCheck{Command: "false"}, true)
	r = handle(t, id)
	if r.Check.Status != services.CheckNotRun || r.Check.Reason != services.CheckReasonNoChanges || r.Check.Attempts != 0 {
		t.Fatalf("CHK-14: %+v %+v", r, r.Check)
	}
	if n := events(id, "changes_failed"); n != 0 {
		t.Fatalf("CHK-14: the changes were not collected: %d", n)
	}
	changed.Store(true)
	// CHK-28: the client cancels the run during the command.
	id = start(t, "to cancel", &services.RunCheck{Command: "sleep 45"}, false)
	go func() {
		time.Sleep(4 * time.Second)
		_, _ = pool.Exec(ctx, `UPDATE runs SET status = 'cancelled' WHERE id = $1`, id)
	}()
	began := time.Now()
	r = handle(t, id)
	if r.Status != "cancelled" || time.Since(began) > 30*time.Second {
		t.Fatalf("CHK-28: %+v in %v", r, time.Since(began))
	}
	// STL-10, STL-16, CHK-40: a stalled run wraps up and is checked once.
	r = run(t, "spin", &services.RunCheck{Command: "false", MaxRepairs: 2})
	if r.Status != "succeeded" || r.StopReason == nil || *r.StopReason != services.StopStalled || r.Check.Status != services.CheckFailed ||
		r.Check.Attempts != 1 || !strings.Contains(*r.Summary, "wrapped up") || events(r.ID, "stalled") != 1 {
		t.Fatalf("STL-10: %+v %+v", r, r.Check)
	}
	// STL-11: a tool call in the wrap-up ends the turn at once.
	began = time.Now()
	r = run(t, "spin-tool", nil)
	if r.Status != "succeeded" || r.StopReason == nil || time.Since(began) > 20*time.Second {
		t.Fatalf("STL-11: %+v in %v", r, time.Since(began))
	}
	// STL-09: how Pi marks a failed command and a search with no match.
	r = run(t, "probe", nil)
	rows, err := pool.Query(ctx, `SELECT data FROM run_events WHERE run_id = $1 AND type = 'tool_result' ORDER BY seq`, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	marks := map[string]bool{}
	for rows.Next() {
		var d struct {
			Name    string `json:"name"`
			IsError bool   `json:"isError"`
		}
		var b []byte
		_ = rows.Scan(&b)
		_ = json.Unmarshal(b, &d)
		marks[d.Name] = d.IsError
	}
	if !marks["bash"] || marks["grep"] {
		t.Fatalf("STL-09: a failed command is an error, a search with no match is not: %v", marks)
	}
}
