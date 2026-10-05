// Package mcp is a minimal Model Context Protocol server (Streamable HTTP with
// JSON responses): the built-in MCP of Nabu through which personal agents
// keep memory, look at their space and manage scheduled tasks
// (FTR.NAB.CMN-0001 arch §4.2, tech §3.2a). Each session gets a signed token;
// its claims are the grant.
package mcp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
)

// ProtocolVersion is the MCP revision implemented.
const ProtocolVersion = "2025-06-18"

// Grant is what a session may do: always on behalf of one user.
type Grant struct {
	UserID       uuid.UUID
	Conversation uuid.UUID // the conversation of the session (uuid.Nil for task runs)
	Channel      string    // channel of the current message: the default delivery of tasks
	TaskID       uuid.UUID // set in task runs
}

// Tool is an MCP tool.
type Tool struct {
	Name        string
	Description string
	InputSchema map[string]any
	ReadOnly    bool
	// Summary renders the step shown in the chat ("Memory: saved …").
	Handler func(ctx context.Context, g Grant, args json.RawMessage) (string, error)
}

// ToolError is a tool failure reported to the agent as isError content.
type ToolError struct{ Msg string }

func (e *ToolError) Error() string { return e.Msg }

// Server serves the tools.
type Server struct {
	tools  []Tool
	signer *jwt.Signer
	// Allowed reports whether the user may still use the agent (not blocked).
	Allowed func(ctx context.Context, userID uuid.UUID) bool
	// ChannelOf is the channel of the conversation's latest user message: the
	// default delivery channel of tasks created there.
	ChannelOf func(ctx context.Context, conv uuid.UUID) string
}

// NewServer creates a server that accepts tokens of the signer.
func NewServer(signer *jwt.Signer) *Server { return &Server{signer: signer} }

// Register adds tools.
func (s *Server) Register(tools ...Tool) { s.tools = append(s.tools, tools...) }

// Tools lists the registered tools.
func (s *Server) Tools() []Tool { return s.tools }

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// ServeHTTP handles POST /internal/v1/mcp.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c, err := s.signer.Verify(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), jwt.AudMCP)
	if err != nil || c.Subject != "builtin" {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}
	g := Grant{Channel: c.Channel}
	g.UserID, _ = uuid.Parse(c.User)
	g.Conversation, _ = uuid.Parse(c.Conversation)
	g.TaskID, _ = uuid.Parse(c.Task)
	if g.Channel == "" && g.Conversation != uuid.Nil && s.ChannelOf != nil {
		g.Channel = s.ChannelOf(r.Context(), g.Conversation)
	}
	if g.UserID == uuid.Nil || (s.Allowed != nil && !s.Allowed(r.Context(), g.UserID)) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var req request
	if err := json.Unmarshal(body, &req); err != nil {
		writeRPC(w, nil, nil, &rpcErr{Code: -32700, Message: "parse error"})
		return
	}
	if len(req.ID) == 0 {
		w.WriteHeader(http.StatusAccepted) // notification
		return
	}
	res, rerr := s.dispatch(r.Context(), g, req)
	writeRPC(w, req.ID, res, rerr)
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func writeRPC(w http.ResponseWriter, id json.RawMessage, result any, e *rpcErr) {
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]any{"jsonrpc": "2.0", "id": id}
	if e != nil {
		resp["error"] = e
	} else {
		resp["result"] = result
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) dispatch(ctx context.Context, g Grant, req request) (any, *rpcErr) {
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		v := p.ProtocolVersion
		if v == "" {
			v = ProtocolVersion
		}
		return map[string]any{
			"protocolVersion": v,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]string{"name": "nabu", "version": "1.0.0"},
			"instructions": "Nabu platform tools: the user's long-term memory (memory_*), the personal space (space_info) " +
				"and scheduled tasks (task_*). Save to memory only durable facts and preferences of the user.",
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		list := []map[string]any{}
		for _, t := range s.tools {
			item := map[string]any{"name": t.Name, "description": t.Description, "inputSchema": t.InputSchema}
			if t.ReadOnly {
				item["annotations"] = map[string]any{"readOnlyHint": true}
			}
			list = append(list, item)
		}
		return map[string]any{"tools": list}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return nil, &rpcErr{Code: -32602, Message: "invalid params"}
		}
		if len(p.Arguments) == 0 {
			p.Arguments = json.RawMessage("{}")
		}
		for _, t := range s.tools {
			if t.Name != p.Name {
				continue
			}
			out, err := t.Handler(ctx, g, p.Arguments)
			if err != nil {
				slog.InfoContext(ctx, "mcp tool refused", "tool", t.Name, "err", err)
				return toolResult(err.Error(), true), nil
			}
			return toolResult(out, false), nil
		}
		return nil, &rpcErr{Code: -32602, Message: "unknown tool " + p.Name}
	default:
		return nil, &rpcErr{Code: -32601, Message: "method not found"}
	}
}

func toolResult(text string, isErr bool) map[string]any {
	return map[string]any{"content": []map[string]string{{"type": "text", "text": text}}, "isError": isErr}
}

// Schema builds an object input schema from properties and required names.
func Schema(props map[string]any, required ...string) map[string]any {
	s := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}
