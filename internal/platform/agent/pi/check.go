package pi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// CheckTimeout bounds the probe of one model (arch §7).
var CheckTimeout = 30 * time.Second

// CheckLLM sends a minimal prompt to every model of a connection in a
// temporary Pi process and reports latency or the error class.
func CheckLLM(ctx context.Context, rt Runtime, root string, req agent.LLMCheckRequest) agent.LLMCheckResponse {
	out := agent.LLMCheckResponse{Results: []agent.LLMCheckResult{}}
	defer os.RemoveAll(root)
	if len(req.Model.Models) == 0 {
		return out
	}
	sreq := agent.SessionRequest{Kind: agent.KindChat, Model: req.Model, Secrets: agent.Secrets{LLMKey: req.Secrets.LLMKey}}
	sreq.Model.ModelID, sreq.Model.Thinking = req.Model.Models[0].ID, "off"
	s, err := Start(ctx, rt, root, sreq)
	if err != nil {
		for _, m := range req.Model.Models {
			out.Results = append(out.Results, agent.LLMCheckResult{Model: m.ID, ErrorClass: agent.ErrAgentCrashed, Message: err.Error()})
		}
		return out
	}
	defer s.Close()
	for _, m := range req.Model.Models {
		out.Results = append(out.Results, checkModel(ctx, s, m.ID))
	}
	return out
}

func checkModel(ctx context.Context, s *Session, modelID string) agent.LLMCheckResult {
	r := agent.LLMCheckResult{Model: modelID}
	if err := s.SetModel(ctx, modelID, "off"); err != nil {
		r.ErrorClass, r.Message = agent.ErrBadRequest, err.Error()
		return r
	}
	cctx, cancel := context.WithTimeout(ctx, CheckTimeout)
	defer cancel()
	start := time.Now()
	var failure *agent.Event
	text := ""
	err := s.Prompt(cctx, agent.PromptRequest{Text: "Reply with one word: ok"}, func(e agent.Event) {
		switch e.Type {
		case agent.EventError:
			ev := e
			failure = &ev
		case agent.EventTextDelta:
			text += e.Delta
		}
	})
	r.LatencyMs = time.Since(start).Milliseconds()
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(cctx.Err(), context.DeadlineExceeded):
		r.ErrorClass, r.Message = agent.ErrUnavailable, fmt.Sprintf("no answer in %s", CheckTimeout)
	case err != nil:
		r.ErrorClass, r.Message = agent.ErrAgentCrashed, err.Error()
	case failure != nil:
		r.ErrorClass, r.HTTPStatus, r.Message = failure.ErrorClass, failure.HTTPStatus, failure.Message
	default:
		r.OK = true
	}
	if !r.OK {
		r.LatencyMs = 0
	}
	return r
}

// CheckMCP checks an MCP server the way the agent connects to it — `pi mcp
// list` with a temporary mcp.json — and then asks the server for its tools
// with their annotations, which `pi mcp list` does not print.
func CheckMCP(ctx context.Context, rt Runtime, root string, req agent.MCPCheckRequest) agent.MCPCheckResponse {
	defer os.RemoveAll(root)
	out := agent.MCPCheckResponse{Tools: []agent.MCPTool{}}
	l := NewLayout(root, rt.CWD)
	sreq := agent.SessionRequest{MCP: []agent.MCPServer{req.Server},
		Secrets: agent.Secrets{MCPHeaders: map[string]map[string]string{req.Server.Name: req.Headers}}}
	if err := os.MkdirAll(l.AgentDir, 0o700); err != nil {
		out.Error = err.Error()
		return out
	}
	if err := writeJSON(l.AgentDir+"/mcp.json", MCPJSON(sreq)); err != nil {
		out.Error = err.Error()
		return out
	}
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	args := append(append([]string{}, rt.Command[1:]...), "mcp", "list", "--json")
	cmd := exec.CommandContext(cctx, rt.Command[0], args...)
	cmd.Env = Env(l, sreq, rt.Options)
	cmd.Dir = root
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	_ = cmd.Run() // exit code 1 when the server is not connected; the JSON says why
	var list struct {
		Servers []struct {
			Name  string   `json:"name"`
			State string   `json:"state"`
			Tools []string `json:"tools"`
			Error string   `json:"error"`
		} `json:"servers"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &list); err != nil {
		out.Error = "pi mcp list: " + lastLine(stderr.String())
		if out.Error == "pi mcp list: " {
			out.Error += err.Error()
		}
		return out
	}
	for _, sv := range list.Servers {
		if sv.Name != req.Server.Name {
			continue
		}
		if sv.State != "connected" {
			out.Error = sv.Error
			if out.Error == "" {
				out.Error = "not connected: " + sv.State
			}
			return out
		}
		out.OK = true
		annotated, err := listTools(cctx, req.Server.URL, req.Headers)
		byName := map[string]agent.MCPTool{}
		for _, t := range annotated {
			byName[t.Name] = t
		}
		for _, name := range sv.Tools {
			t, ok := byName[name]
			if !ok || err != nil {
				t = agent.MCPTool{Name: name}
			}
			out.Tools = append(out.Tools, t)
		}
		return out
	}
	out.Error = "the server is not in the configuration"
	if len(list.Errors) > 0 {
		b, _ := json.Marshal(list.Errors[0])
		out.Error = string(b)
	}
	return out
}

// listTools performs initialize and tools/list over streamable HTTP.
func listTools(ctx context.Context, url string, headers map[string]string) ([]agent.MCPTool, error) {
	session := ""
	call := func(id int, method string, params any) (json.RawMessage, error) {
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
		if id == 0 {
			body, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method})
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("MCP-Protocol-Version", "2025-06-18")
		if session != "" {
			req.Header.Set("Mcp-Session-Id", session)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if s := resp.Header.Get("Mcp-Session-Id"); s != "" {
			session = s
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(raw)))
		}
		if id == 0 {
			return nil, nil
		}
		if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			for _, line := range strings.Split(string(raw), "\n") {
				if d, ok := strings.CutPrefix(strings.TrimSpace(line), "data:"); ok {
					raw = []byte(strings.TrimSpace(d))
					break
				}
			}
		}
		var r struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		if r.Error != nil {
			return nil, errors.New(r.Error.Message)
		}
		return r.Result, nil
	}
	if _, err := call(1, "initialize", map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{},
		"clientInfo": map[string]string{"name": "nabu-agent-check", "version": "1"}}); err != nil {
		return nil, err
	}
	_, _ = call(0, "notifications/initialized", nil)
	res, err := call(2, "tools/list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var tl struct {
		Tools []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
			Annotations *struct {
				ReadOnlyHint    *bool `json:"readOnlyHint"`
				DestructiveHint *bool `json:"destructiveHint"`
			} `json:"annotations"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(res, &tl); err != nil {
		return nil, err
	}
	out := make([]agent.MCPTool, 0, len(tl.Tools))
	for _, t := range tl.Tools {
		// MCP defaults: not read-only, may be destructive.
		mt := agent.MCPTool{Name: t.Name, Description: clip(t.Description, 300), Destructive: true}
		if a := t.Annotations; a != nil {
			if a.ReadOnlyHint != nil {
				mt.ReadOnly = *a.ReadOnlyHint
			}
			if a.DestructiveHint != nil {
				mt.Destructive = *a.DestructiveHint
			}
		}
		if mt.ReadOnly {
			mt.Destructive = false
		}
		out = append(out, mt)
	}
	return out, nil
}
