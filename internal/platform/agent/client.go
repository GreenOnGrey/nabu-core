package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Client calls the operator's internal API. With the service token it may do
// everything; with a session token (runner) only prompt, abort and close of
// that session.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// BusyError is the operator's 503 agent_busy.
type BusyError struct{ RetryAfter time.Duration }

func (e *BusyError) Error() string {
	return fmt.Sprintf("the agent is busy, retry in %s", e.RetryAfter)
}

// ErrSessionGone means the operator no longer has the session (restart, idle close).
var ErrSessionGone = errors.New("agent: the session is gone")

// ErrStreamBroken means the prompt stream ended without settled or error —
// the operator went away in the middle of the run.
var ErrStreamBroken = errors.New("agent: the prompt stream broke off")

// APIError is any other error answer of the operator.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string { return fmt.Sprintf("agent %d %s: %s", e.Status, e.Code, e.Message) }

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) do(ctx context.Context, method, path string, in any, timeout time.Duration) (*http.Response, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	cancel := context.CancelFunc(func() {})
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, body)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http().Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("agent operator: %w", err)
	}
	if resp.StatusCode < 300 {
		resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
		return resp, nil
	}
	defer cancel()
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var e struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &e)
	switch {
	case resp.StatusCode == http.StatusServiceUnavailable && e.Error == "agent_busy":
		ra := 10 * time.Second
		if n, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && n > 0 {
			ra = time.Duration(n) * time.Second
		}
		return nil, &BusyError{RetryAfter: ra}
	case resp.StatusCode == http.StatusNotFound && e.Error == "session_not_found":
		return nil, ErrSessionGone
	}
	if e.Error == "" {
		e.Error, e.Message = http.StatusText(resp.StatusCode), strings.TrimSpace(string(raw))
	}
	return nil, &APIError{Status: resp.StatusCode, Code: e.Error, Message: e.Message}
}

func decodeBody(resp *http.Response, out any) error {
	defer resp.Body.Close()
	return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out)
}

// Open opens a session. When the operator does not know the skills snapshot,
// bundle supplies the archive and the request is repeated once (SK-10).
func (c *Client) Open(ctx context.Context, req SessionRequest, bundle func(ctx context.Context) ([]byte, error)) (SessionResponse, error) {
	var out SessionResponse
	resp, err := c.do(ctx, http.MethodPost, "/v1/sessions", req, 2*time.Minute)
	var ae *APIError
	if errors.As(err, &ae) && ae.Code == "skills_bundle_required" && bundle != nil && req.Skills != nil {
		b, berr := bundle(ctx)
		if berr != nil {
			return out, fmt.Errorf("skills bundle: %w", berr)
		}
		req.Skills.Bundle = b
		resp, err = c.do(ctx, http.MethodPost, "/v1/sessions", req, 2*time.Minute)
	}
	if err != nil {
		return out, err
	}
	return out, decodeBody(resp, &out)
}

// Prompt runs a prompt and delivers the stream events. It returns nil after
// settled or an error event (the error event was delivered to onEvent), and
// ErrStreamBroken if the stream ended otherwise.
func (c *Client) Prompt(ctx context.Context, sessionID string, p PromptRequest, onEvent func(Event)) error {
	resp, err := c.do(ctx, http.MethodPost, "/v1/sessions/"+sessionID+"/prompt", p, 0)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	r := bufio.NewReaderSize(resp.Body, 64<<10)
	for {
		line, err := r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var ev Event
			if json.Unmarshal(line, &ev) == nil {
				onEvent(ev)
				if ev.Type == EventSettled || ev.Type == EventError {
					return nil
				}
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrStreamBroken
		}
	}
}

// Abort aborts the current run of a session.
func (c *Client) Abort(ctx context.Context, sessionID string) error {
	resp, err := c.do(ctx, http.MethodPost, "/v1/sessions/"+sessionID+"/abort", nil, 30*time.Second)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Patch changes the model or the level of a session.
func (c *Client) Patch(ctx context.Context, sessionID string, p PatchRequest) error {
	resp, err := c.do(ctx, http.MethodPatch, "/v1/sessions/"+sessionID, p, 30*time.Second)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Snapshot returns the Pi session file of a session.
func (c *Client) Snapshot(ctx context.Context, sessionID string) ([]byte, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/sessions/"+sessionID+"/snapshot", nil, time.Minute)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, 256<<20))
}

// Close closes a session; a session that is already gone is not an error.
func (c *Client) Close(ctx context.Context, sessionID string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/v1/sessions/"+sessionID, nil, 30*time.Second)
	if errors.Is(err, ErrSessionGone) {
		return nil
	}
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// CheckLLM probes the models of a connection.
func (c *Client) CheckLLM(ctx context.Context, req LLMCheckRequest) (LLMCheckResponse, error) {
	var out LLMCheckResponse
	resp, err := c.do(ctx, http.MethodPost, "/v1/checks/llm", req, 5*time.Minute)
	if err != nil {
		return out, err
	}
	return out, decodeBody(resp, &out)
}

// CheckMCP connects to an MCP server and lists its tools.
func (c *Client) CheckMCP(ctx context.Context, req MCPCheckRequest) (MCPCheckResponse, error) {
	var out MCPCheckResponse
	resp, err := c.do(ctx, http.MethodPost, "/v1/checks/mcp", req, 2*time.Minute)
	if err != nil {
		return out, err
	}
	return out, decodeBody(resp, &out)
}

// cancelOnClose releases the request's timeout when the body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
