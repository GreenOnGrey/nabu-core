package operator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
)

// FTR.NAB.CMN-0009 qa §2: the operator side of subagents without a real Pi.

func subOp(t *testing.T, maxSessions int) (*Operator, *httptest.Server) {
	t.Helper()
	op, err := New(Config{Runtime: pi.Runtime{Command: []string{"/bin/false"}}, WorkDir: t.TempDir(), ServiceToken: "svc",
		MaxSessions: maxSessions, SubagentAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(op.SubagentHandler())
	t.Cleanup(srv.Close)
	return op, srv
}

func limits() *agent.SubagentLimits {
	return &agent.SubagentLimits{MaxParallel: 4, MaxPerTurn: 8, Timeout: agent.Duration(time.Minute), ResultChars: 16000,
		SlotWait: agent.Duration(time.Second)}
}

// a session of the operator without a Pi process: enough for the checks before a start
func addSession(op *Operator, id string, req agent.SessionRequest) *entry {
	e := &entry{id: id, token: "tok-" + id, kind: agent.KindChat, req: req, subToken: "sub-" + id, last: time.Now()}
	op.mu.Lock()
	op.sessions[id] = e
	op.mu.Unlock()
	return e
}

func task(desc, prompt string) map[string]any {
	return map[string]any{"description": desc, "prompt": prompt}
}

func TestSubagentChecks(t *testing.T) {
	op, srv := subOp(t, 10)
	req := agent.SessionRequest{Subagents: limits(), Skills: &agent.Skills{Names: []string{"code-review"}}}
	e := addSession(op, "s1", req)
	addSession(op, "s2", req)
	url := srv.URL + "/v1/sessions/s1/subagents"

	// SUB-15: a token of another session, of no session
	for _, tok := range []string{"sub-s2", "tok-s1", ""} {
		if code, errCode := call(t, http.MethodPost, url, tok, map[string]any{"call": "c", "tasks": []any{task("a", "b")}}); code != 401 || errCode != "subagent_token" {
			t.Fatalf("SUB-15 %q: %d %s", tok, code, errCode)
		}
	}
	// SUB-02: 0 and 5 tasks, an empty prompt, a long description, an unknown skill
	for name, tasks := range map[string][]any{
		"none": {}, "five": {task("a", "b"), task("a", "b"), task("a", "b"), task("a", "b"), task("a", "b")},
		"empty prompt": {task("a", "  ")}, "long description": {task(strings.Repeat("д", 81), "b")},
	} {
		if code, errCode := call(t, http.MethodPost, url, "sub-s1", map[string]any{"call": "c", "tasks": tasks}); code != 422 || errCode != "subagent_tasks" {
			t.Fatalf("SUB-02 %s: %d %s", name, code, errCode)
		}
	}
	skilled := map[string]any{"description": "a", "prompt": "b", "skill": "unknown"}
	if code, errCode := call(t, http.MethodPost, url, "sub-s1", map[string]any{"call": "c", "tasks": []any{skilled}}); code != 422 || errCode != "subagent_skill" {
		t.Fatalf("SUB-02 skill: %d %s", code, errCode)
	}
	// no prompt runs
	if code, errCode := call(t, http.MethodPost, url, "sub-s1", map[string]any{"call": "c", "tasks": []any{task("a", "b")}}); code != 409 || errCode != "subagent_no_turn" {
		t.Fatalf("no turn: %d %s", code, errCode)
	}
	// SUB-11: the ninth subagent of a turn; a new prompt starts the count again
	e.begin(context.Background(), time.Time{}, func(agent.Event) {})
	e.mu.Lock()
	e.turn.started = 8
	e.mu.Unlock()
	if code, errCode := call(t, http.MethodPost, url, "sub-s1", map[string]any{"call": "c", "tasks": []any{task("a", "b")}}); code != 429 || errCode != "subagent_turn_limit" {
		t.Fatalf("SUB-11: %d %s", code, errCode)
	}
	e.end()
	e.begin(context.Background(), time.Time{}, func(agent.Event) {})
	if e.turn.started != 0 {
		t.Fatal("SUB-11: the count goes on into the next prompt")
	}
	e.end()
}

// SUB-13: without a place within slotWait the call is refused; places are taken and given back.
func TestSubagentSlots(t *testing.T) {
	op, srv := subOp(t, 1)
	e := addSession(op, "s1", agent.SessionRequest{Subagents: limits()})
	op.counts[agent.KindChat] = 1 // the parent holds the only place
	e.begin(context.Background(), time.Time{}, func(agent.Event) {})
	defer e.end()
	start := time.Now()
	code, errCode := call(t, http.MethodPost, srv.URL+"/v1/sessions/s1/subagents", "sub-s1", map[string]any{"call": "c", "tasks": []any{task("a", "b")}})
	if code != 503 || errCode != "subagent_no_slot" || time.Since(start) < time.Second {
		t.Fatalf("SUB-13: %d %s after %s", code, errCode, time.Since(start))
	}
	if e.turn.started != 0 {
		t.Fatal("SUB-13: a refused call is counted in the turn")
	}
	op.counts[agent.KindChat] = 0
	if got := op.reserve(agent.KindChat, 3); got != 1 {
		t.Fatalf("SUB-13: reserved %d of one place", got)
	}
	op.release(agent.KindChat)
	if op.counts[agent.KindChat] != 0 {
		t.Fatal("SUB-13: the place is not given back")
	}
}

// SUB-05: the child gets the instructions of the parent and its own block, no history,
// the model of the parent now; MCP calls are marked.
func TestChildRequest(t *testing.T) {
	parent := agent.SessionRequest{SystemAppend: "Persona of Ada.", History: []agent.HistoryMessage{{Role: "user", Text: "the secret word is plum"}},
		Snapshot: nil, Subagents: limits(), Label: "conv 1234",
		Model: agent.ModelSpec{ModelID: "old", Thinking: "off"},
		MCP:   []agent.MCPServer{{Name: "nabu", URL: "http://api", HeaderNames: []string{"Authorization"}}}}
	c := childRequest(parent, "new", "high", "tc_01/2")
	if !strings.HasPrefix(c.SystemAppend, "Persona of Ada.") || !strings.Contains(c.SystemAppend, "You are a subagent") {
		t.Fatalf("SUB-05: %q", c.SystemAppend)
	}
	if c.History != nil || c.Subagents != nil || c.Model.ModelID != "new" || c.Model.Thinking != "high" { // SUB-10
		t.Fatalf("SUB-05, SUB-10: %+v", c)
	}
	if len(c.MCP) != 1 || c.MCP[0].HeaderNames[0] != "Authorization" {
		t.Fatalf("SUB-05: the MCP of the parent %+v", c.MCP)
	}
	if c.Label != "subagent tc_01/2 of conv 1234" {
		t.Fatal(c.Label)
	}
	if childPrompt(callTask{Prompt: "check", Skill: "code-review"}) != "/skill:code-review check" {
		t.Fatal("the skill is not called")
	}
}

// SUB-06: a long result is cut at a character with a line about it.
func TestClipResult(t *testing.T) {
	text := strings.Repeat("я", 20000)
	out, cut := clipResult(text, 16000)
	if !cut || !strings.HasPrefix(out, strings.Repeat("я", 16000)+"\n[truncated: 16000 of 20000 characters;") {
		t.Fatalf("SUB-06: %v %q", cut, out[len(out)-120:])
	}
	if out, cut := clipResult("short", 16000); cut || out != "short" {
		t.Fatal("SUB-06: a short result is changed")
	}
}

// SUB-01: the Pi of a session with subagents is told where to start them; MCP headers go to mcp.json.
func TestSubagentEnvAndFiles(t *testing.T) {
	env := strings.Join(subagentEnv("127.0.0.1:8091", "s1", "tok", 4), " ")
	for _, want := range []string{"NABU_SUBAGENT_URL=http://127.0.0.1:8091/v1/sessions/s1/subagents", "NABU_SUBAGENT_TOKEN=tok", "NABU_SUBAGENT_DEPTH=0", "NABU_SUBAGENT_MAX_PARALLEL=4"} {
		if !strings.Contains(env, want) {
			t.Fatalf("SUB-01: %s not in %s", want, env)
		}
	}
	s := pi.SettingsJSON(agent.SessionRequest{Subagents: limits()}, "/opt/ext")
	if ext := s["extensions"].([]string); len(ext) != 1 {
		t.Fatalf("SUB-01: the extension of a session with subagents %v", ext)
	}
	if ext := pi.SettingsJSON(agent.SessionRequest{}, "/opt/ext")["extensions"].([]string); len(ext) != 0 {
		t.Fatalf("a session without a workspace and subagents has the extension %v", ext)
	}
}

// SUB-14, STP-10: a stopped turn ends only after the last events of its subagents; a late event is dropped.
func TestTurnWaitsForSubagents(t *testing.T) {
	e := &entry{}
	var mu sync.Mutex
	var got []string
	tn := e.begin(context.Background(), time.Time{}, func(ev agent.Event) {
		mu.Lock()
		got = append(got, ev.Subagent.State)
		mu.Unlock()
	})
	tn.calls.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		tn.send(agent.Event{Type: agent.EventSubagent, Subagent: &agent.SubagentState{State: agent.SubagentAborted}})
		tn.calls.Done()
	}()
	e.end()
	tn.send(agent.Event{Type: agent.EventSubagent, Subagent: &agent.SubagentState{State: "late"}})
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != agent.SubagentAborted {
		t.Fatalf("SUB-14: %v", got)
	}
}
