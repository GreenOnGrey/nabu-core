package relay

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/fakellm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/workspace"
)

// RLY-09, RLY-08 (K): the nabu-workspace extension runs Pi's built-in tools in
// a workspace that is connected to the relay by its own WebSocket. Requires
// NABU_PI_CMD (a real pi).
func TestExtensionThroughRelay(t *testing.T) {
	cmd := strings.Fields(os.Getenv("NABU_PI_CMD"))
	if len(cmd) == 0 {
		t.Skip("NABU_PI_CMD is not set")
	}
	signer := jwt.NewSigner("https://nabu", []byte("contract-key"))
	srv := NewServer(Config{Pod: "self", WaitConnect: 10 * time.Second}, signer, &memStore{m: map[string]string{}})
	r := chi.NewRouter()
	srv.Routes(r)
	hs := httptest.NewServer(r)
	defer hs.Close()

	root := t.TempDir()
	ws := &workspace.Workspace{Root: root, Token: "local", Env: workspace.CommandEnv(root)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := &Client{URL: "ws" + strings.TrimPrefix(hs.URL, "http") + "/v1/workspaces/connect",
		Token:       signer.Issue(jwt.Claims{Audience: jwt.AudWorkspace, Workspace: "run-k", Kind: "external"}, time.Hour),
		WorkspaceID: "run-k", Kind: "external", Handler: ws.Handler(), HandlerToken: "local"}
	go func() { _ = client.Run(ctx) }()

	steps := []fakellm.ToolCall{
		{Name: "write", Args: map[string]any{"path": "notes/a.txt", "content": "hello from the agent\n"}},
		{Name: "read", Args: map[string]any{"path": "notes/a.txt"}},
		{Name: "bash", Args: map[string]any{"command": "echo done> b.txt"}},
		{Name: "grep", Args: map[string]any{"pattern": "hello"}},
		{Name: "edit", Args: map[string]any{"path": "notes/a.txt", "edits": []map[string]string{{"oldText": "hello", "newText": "hi"}}}},
		{Name: "ls", Args: map[string]any{"path": "notes"}},
	}
	llm := &fakellm.Server{Key: "sk-k", Script: func(r fakellm.Request) *fakellm.Reply {
		if n := r.ToolResults(); n < len(steps) {
			return &fakellm.Reply{ToolCalls: []fakellm.ToolCall{steps[n]}}
		}
		return &fakellm.Reply{Text: "finished"}
	}}
	lsrv := httptest.NewServer(llm)
	defer lsrv.Close()

	ext, _ := filepath.Abs(filepath.Join("..", "..", "pi-extensions", "nabu-workspace"))
	rt := pi.Runtime{Command: cmd, Options: pi.Options{Path: os.Getenv("PATH"), ExtensionDir: ext}}
	req := agent.SessionRequest{Kind: agent.KindRun,
		Model: agent.ModelSpec{Provider: "nabu-k", API: "openai-completions", BaseURL: lsrv.URL, ModelID: "m", Thinking: "off",
			Models: []agent.ModelDef{{ID: "m", ContextWindow: 100000, MaxTokens: 8000, Input: []string{"text"}}}},
		Secrets: agent.Secrets{LLMKey: "sk-k"},
		Workspace: &agent.Workspace{ID: "run-k", URL: hs.URL + "/internal/v1/workspaces/run-k",
			Token: signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: "run-k"}, time.Hour)}}
	s, err := pi.Start(ctx, rt, t.TempDir(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var results []agent.Event
	if err := s.Prompt(ctx, agent.PromptRequest{Text: "do the task"}, func(e agent.Event) {
		if e.Type == agent.EventToolResult {
			results = append(results, e)
		}
		if e.Type == agent.EventError {
			t.Errorf("error event %+v", e)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if len(results) != len(steps) {
		t.Fatalf("tool results %d: %+v", len(results), results)
	}
	for i, r := range results {
		if r.IsError {
			t.Errorf("%s failed: %s", steps[i].Name, r.Summary)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(root, "notes", "a.txt")); string(b) != "hi from the agent\n" {
		t.Fatalf("a.txt = %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "b.txt")); !strings.HasPrefix(string(b), "done") {
		t.Fatalf("bash did not run in the workspace: %q", b)
	}
}
