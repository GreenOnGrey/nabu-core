package relay

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/fakellm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
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
		// RLY-11: a path as the shell of the workspace shows it
		{Name: "read", Args: map[string]any{"path": filepath.Join(root, "notes", "a.txt")}},
		// RLY-10: truncated output: Pi saves it in full, the extension moves the file into the workspace
		{Name: "bash", Args: map[string]any{"command": "seq 1 200000"}},
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
	if !strings.Contains(results[6].Summary, "hi from the agent") {
		t.Fatalf("read by the absolute path: %s", results[6].Summary)
	}
	full, _ := filepath.Glob(filepath.Join(root, ".pi-output", "pi-*"))
	if len(full) != 1 {
		t.Fatalf("full output %v", full)
	}
	if b, _ := os.ReadFile(full[0]); !strings.HasSuffix(strings.TrimSpace(string(b)), "200000") {
		t.Fatalf("full output is not complete: %d bytes", len(b))
	}
}

// RLY-12: the model calls a deferred MCP tool by its name before tool_search
// loaded it: the extension loads it, and the repeated call runs; a tool that
// does not exist stays an error. Requires NABU_PI_CMD.
func TestExtensionLoadsDeferredTool(t *testing.T) {
	cmd := strings.Fields(os.Getenv("NABU_PI_CMD"))
	if len(cmd) == 0 {
		t.Skip("NABU_PI_CMD is not set")
	}
	signer := jwt.NewSigner("https://nabu", []byte("contract-key"))
	tools := mcp.NewServer(signer)
	tools.Register(mcp.Tool{Name: "echo", Description: "Echoes the text.", ReadOnly: true,
		InputSchema: mcp.Schema(map[string]any{"text": map[string]any{"type": "string"}}, "text"),
		Handler: func(_ context.Context, _ mcp.Grant, args json.RawMessage) (string, error) {
			var a struct{ Text string }
			_ = json.Unmarshal(args, &a)
			return "echo: " + a.Text, nil
		}})
	msrv := httptest.NewServer(tools)
	defer msrv.Close()
	token := signer.Issue(jwt.Claims{Audience: jwt.AudMCP, Subject: "builtin", User: uuid.NewString()}, time.Hour)

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
		Token:       signer.Issue(jwt.Claims{Audience: jwt.AudWorkspace, Workspace: "run-d", Kind: "external"}, time.Hour),
		WorkspaceID: "run-d", Kind: "external", Handler: ws.Handler(), HandlerToken: "local"}
	go func() { _ = client.Run(ctx) }()

	calls := []fakellm.ToolCall{
		{Name: "mcp__tools__echo", Args: map[string]any{"text": "hi"}},
		{Name: "mcp__tools__echo", Args: map[string]any{"text": "hi"}},
		{Name: "mcp__tools__nope", Args: map[string]any{}},
	}
	llm := &fakellm.Server{Key: "sk-d", Script: func(r fakellm.Request) *fakellm.Reply {
		if n := r.ToolResults(); n < len(calls) {
			return &fakellm.Reply{ToolCalls: []fakellm.ToolCall{calls[n]}}
		}
		return &fakellm.Reply{Text: "finished"}
	}}
	lsrv := httptest.NewServer(llm)
	defer lsrv.Close()

	ext, _ := filepath.Abs(filepath.Join("..", "..", "pi-extensions", "nabu-workspace"))
	rt := pi.Runtime{Command: cmd, Options: pi.Options{Path: os.Getenv("PATH"), ExtensionDir: ext}}
	req := agent.SessionRequest{Kind: agent.KindRun,
		Model: agent.ModelSpec{Provider: "nabu-d", API: "openai-completions", BaseURL: lsrv.URL, ModelID: "m", Thinking: "off",
			Models: []agent.ModelDef{{ID: "m", ContextWindow: 100000, MaxTokens: 8000, Input: []string{"text"}}}},
		MCP:     []agent.MCPServer{{Name: "tools", URL: msrv.URL, HeaderNames: []string{"Authorization"}, Exposure: "deferred"}},
		Secrets: agent.Secrets{LLMKey: "sk-d", MCPHeaders: map[string]map[string]string{"tools": {"Authorization": "Bearer " + token}}},
		Workspace: &agent.Workspace{ID: "run-d", URL: hs.URL + "/internal/v1/workspaces/run-d",
			Token: signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: "run-d"}, time.Hour)}}
	s, err := pi.Start(ctx, rt, t.TempDir(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var steps []agent.Event
	if err := s.Prompt(ctx, agent.PromptRequest{Text: "echo hi"}, func(e agent.Event) {
		if e.Type == agent.EventToolResult {
			steps = append(steps, e)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if len(steps) != 3 || steps[0].IsError || steps[0].Summary != pi.ToolLoaded || steps[1].IsError ||
		!steps[2].IsError || steps[2].Summary == pi.ToolLoaded {
		t.Fatalf("steps in the chat: %+v", steps)
	}
	last := llm.Requests()[len(llm.Requests())-1]
	var got []string
	for _, m := range last.Messages {
		if m.Role == "tool" {
			got = append(got, m.Text())
		}
	}
	if len(got) != 3 || !strings.Contains(got[0], "is loaded now") || got[1] != "echo: hi" || !strings.Contains(got[2], "not found") {
		t.Fatalf("tool results seen by the model: %q", got)
	}
}
