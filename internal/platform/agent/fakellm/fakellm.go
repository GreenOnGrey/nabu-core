// Package fakellm is a scripted OpenAI-compatible chat completions endpoint
// for tests and the demo stack (FTR.NAB.CMN-0001 qa §1, ported from Hammurapi). The model id selects the
// behaviour; a Script can answer with tool calls. It is not an LLM; never use
// it in production.
//
// Models:
//
//	e401, e402, e429, e503, e400  — always fail with that status
//	ectx                          — context overflow (400)
//	ctxonce                       — context overflow on the first request to the server, then answers
//	flaky                         — 429 twice per server, then answers
//	anything else                 — streams "echo: <last user text>" (or the Script's reply)
package fakellm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// Message is a chat message of a request.
type Message struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  []struct {
		ID       string `json:"id"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	} `json:"tool_calls,omitempty"`
}

// Text returns the text of the message content.
func (m Message) Text() string {
	var s string
	if json.Unmarshal(m.Content, &s) == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	_ = json.Unmarshal(m.Content, &parts)
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "text" {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

// Request is a chat completions request.
type Request struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Tools    []struct {
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	} `json:"tools"`
}

// ToolNames lists the tools declared in the request.
func (r Request) ToolNames() []string {
	out := make([]string, 0, len(r.Tools))
	for _, t := range r.Tools {
		out = append(out, t.Function.Name)
	}
	return out
}

// LastUser returns the text of the last user message.
func (r Request) LastUser() string {
	for i := len(r.Messages) - 1; i >= 0; i-- {
		if r.Messages[i].Role == "user" {
			return r.Messages[i].Text()
		}
	}
	return ""
}

// FirstUser returns the text of the first user message (the task of a session).
func (r Request) FirstUser() string {
	for _, m := range r.Messages {
		if m.Role == "user" {
			return m.Text()
		}
	}
	return ""
}

// ToolResults counts the tool results in the conversation so far.
func (r Request) ToolResults() int {
	n := 0
	for _, m := range r.Messages {
		if m.Role == "tool" {
			n++
		}
	}
	return n
}

// ToolCall is a call the fake model makes.
type ToolCall struct {
	Name string
	Args any
}

// Reply is a scripted answer: text, or tool calls (the model is called again
// with their results).
type Reply struct {
	Text      string
	ToolCalls []ToolCall
}

// Server is the fake endpoint.
type Server struct {
	// Key, when set, is the only accepted API key (Bearer).
	Key string
	// Script answers instead of the echo; returning nil falls back to the echo.
	Script func(Request) *Reply

	mu         sync.Mutex
	requests   []Request
	flaky      int
	overflowed bool
}

// Requests returns the requests received so far.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/chat/completions") {
		http.NotFound(w, r)
		return
	}
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, 400, "invalid json")
		return
	}
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()
	if s.Key != "" && r.Header.Get("Authorization") != "Bearer "+s.Key {
		fail(w, 401, "Authentication Fails, Your api key is invalid")
		return
	}
	switch req.Model {
	case "e401":
		fail(w, 401, "Authentication Fails, Your api key is invalid")
		return
	case "e402":
		fail(w, 402, "Insufficient Balance")
		return
	case "e429":
		fail(w, 429, "Rate limit reached")
		return
	case "e503":
		fail(w, 503, "Service Unavailable")
		return
	case "e400":
		fail(w, 400, "Invalid request")
		return
	case "ectx":
		fail(w, 400, "This model's maximum context length is 131072 tokens. However, you requested 200000 tokens")
		return
	case "flaky":
		s.mu.Lock()
		s.flaky++
		n := s.flaky
		s.mu.Unlock()
		if n <= 2 {
			fail(w, 429, "Rate limit reached")
			return
		}
	case "ctxonce":
		s.mu.Lock()
		first := !s.overflowed
		s.overflowed = true
		s.mu.Unlock()
		if first {
			fail(w, 400, "This model's maximum context length is 131072 tokens. However, you requested 200000 tokens")
			return
		}
	}
	var reply *Reply
	if s.Script != nil {
		reply = s.Script(req)
	}
	if reply == nil {
		reply = &Reply{Text: "echo: " + req.LastUser()}
	}
	stream(w, req, reply)
}

func fail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"message": msg, "type": "api_error", "code": fmt.Sprint(code)}})
	_, _ = w.Write(b)
}

func stream(w http.ResponseWriter, req Request, r *Reply) {
	w.Header().Set("Content-Type", "text/event-stream")
	fl, _ := w.(http.Flusher)
	send := func(delta map[string]any, finish string, usage map[string]int) {
		choice := map[string]any{"index": 0, "delta": delta}
		if finish != "" {
			choice["finish_reason"] = finish
		}
		chunk := map[string]any{"id": "fake", "object": "chat.completion.chunk", "model": req.Model, "choices": []any{choice}}
		if usage != nil {
			chunk["usage"] = usage
		}
		b, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "data: %s\n\n", b)
		if fl != nil {
			fl.Flush()
		}
	}
	send(map[string]any{"role": "assistant", "content": ""}, "", nil)
	in := 0
	for _, m := range req.Messages {
		in += len(m.Content) / 4
	}
	usage := map[string]int{"prompt_tokens": in + 10, "completion_tokens": len(r.Text)/4 + 1, "total_tokens": in + 11 + len(r.Text)/4}
	if len(r.ToolCalls) > 0 {
		for i, tc := range r.ToolCalls {
			args, _ := json.Marshal(tc.Args)
			send(map[string]any{"tool_calls": []any{map[string]any{"index": i, "id": fmt.Sprintf("call_%d_%d", len(req.Messages), i),
				"type": "function", "function": map[string]any{"name": tc.Name, "arguments": string(args)}}}}, "", nil)
		}
		send(map[string]any{}, "tool_calls", usage)
	} else {
		words := strings.SplitAfter(r.Text, " ")
		for _, wd := range words {
			if wd != "" {
				send(map[string]any{"content": wd}, "", nil)
			}
		}
		send(map[string]any{}, "stop", usage)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	if fl != nil {
		fl.Flush()
	}
}
