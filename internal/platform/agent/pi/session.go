package pi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/pkg/pirpc"
)

// maxFieldBytes bounds tool arguments and results in the stream (tech spec §4.3).
const maxFieldBytes = 4 << 10

// Runtime describes how to start Pi.
type Runtime struct {
	// Command is the Pi command: ["pi"] or ["node", "/path/cli.js"].
	Command []string
	Options
	// Stderr receives Pi's diagnostics (secrets masked); nil discards them.
	Stderr io.Writer
}

// Session is one Pi process of a session.
type Session struct {
	rt     Runtime
	layout Layout
	req    agent.SessionRequest

	mu       sync.Mutex
	client   *pirpc.Client
	model    string
	thinking string
	base     pirpc.SessionStats // totals already reported
	history  string             // seeds the first prompt when there is no snapshot
	file     string             // Pi session file, for snapshots and crash recovery
}

// Start writes the agent directory and starts Pi with the session's model.
func Start(ctx context.Context, rt Runtime, root string, req agent.SessionRequest) (*Session, error) {
	l := NewLayout(root, rt.CWD)
	if err := Write(l, req, rt.Options); err != nil {
		return nil, fmt.Errorf("agent directory: %w", err)
	}
	s := &Session{rt: rt, layout: l, req: req, model: req.Model.ModelID, thinking: thinkingOrOff(req.Model.Thinking)}
	if req.Snapshot == nil && len(req.History) > 0 {
		s.history = historyBlock(req.History)
	}
	resume := ""
	if req.Snapshot != nil {
		resume = SnapshotPath(l)
	}
	if err := s.start(ctx, resume); err != nil {
		return nil, err
	}
	return s, nil
}

// start launches Pi, optionally switching to an existing session file, and
// applies the model and level.
func (s *Session) start(ctx context.Context, sessionFile string) error {
	args := append(append([]string{}, s.rt.Command[1:]...), "--mode", "rpc", "--no-approve",
		"--session-dir", s.layout.SessionsDir, "--provider", s.req.Model.Provider, "--model", s.model)
	stderr := io.Discard
	if s.rt.Stderr != nil {
		stderr = &maskWriter{w: s.rt.Stderr, secrets: SecretValues(s.req)}
	}
	c, err := pirpc.Start(context.WithoutCancel(ctx), pirpc.Options{Binary: s.rt.Command[0], Args: args,
		Env: Env(s.layout, s.req, s.rt.Options), Dir: s.layout.CWD, Stderr: stderr})
	if err != nil {
		return err
	}
	setup := func() error {
		if err := c.SetAutoRetry(ctx, true); err != nil {
			return err
		}
		if err := c.SetAutoCompaction(ctx, true); err != nil {
			return err
		}
		if sessionFile != "" {
			if err := c.SwitchSession(ctx, sessionFile); err != nil {
				return fmt.Errorf("restore session: %w", err)
			}
		}
		if _, err := c.SetModel(ctx, s.req.Model.Provider, s.model); err != nil {
			return err
		}
		if err := c.SetThinkingLevel(ctx, s.thinking); err != nil {
			return err
		}
		st, err := c.GetState(ctx)
		if err != nil {
			return err
		}
		base, err := c.GetSessionStats(ctx)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.client, s.file, s.base = c, st.SessionFile, base
		s.mu.Unlock()
		return nil
	}
	if err := setup(); err != nil {
		_ = c.Close()
		var ee *pirpc.ExitError
		if errors.As(err, &ee) {
			return fmt.Errorf("pi exited during start (code %d): %s", ee.Code, lastLine(ee.StderrTail))
		}
		return err
	}
	return nil
}

func (s *Session) cl() *pirpc.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

// Model returns the current model and level.
func (s *Session) Model() (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.model, s.thinking
}

// Prompt runs one prompt and emits the operator's events until settled or an
// error event. It returns an error only when the run could not be performed
// at all (context cancelled, Pi unusable); LLM failures are error events.
func (s *Session) Prompt(ctx context.Context, p agent.PromptRequest, emit func(agent.Event)) error {
	text := p.Text
	s.mu.Lock()
	if s.history != "" {
		text = s.history + "\n\n" + text
		s.history = ""
	}
	s.mu.Unlock()
	images := make([]pirpc.Image, 0, len(p.Images))
	for _, im := range p.Images {
		images = append(images, pirpc.Image{Data: im.Data, MimeType: im.MimeType})
	}
	compacted, restarted := false, false
	for {
		res, err := s.run(ctx, pirpc.PromptParams{Message: text, Images: images}, emit)
		var ee *pirpc.ExitError
		switch {
		case errors.As(err, &ee) || errors.Is(err, pirpc.ErrClosed):
			if restarted || ctx.Err() != nil {
				emit(agent.Event{Type: agent.EventError, ErrorClass: agent.ErrAgentCrashed,
					Message: "the agent process stopped: " + lastLine(s.stderrTail(err))})
				return nil
			}
			restarted = true
			if rerr := s.restart(ctx); rerr != nil {
				emit(agent.Event{Type: agent.EventError, ErrorClass: agent.ErrAgentCrashed, Message: rerr.Error()})
				return nil
			}
			continue
		case errors.Is(err, context.Canceled) && ctx.Err() == nil:
			// Aborted through the operator's abort: count what was spent and
			// end the stream normally, so the caller does not take it for a
			// lost session and resend the prompt.
			s.usage(ctx, emit)
			emit(agent.Event{Type: agent.EventSettled, Aborted: true})
			return nil
		case err != nil:
			return err
		}
		s.usage(ctx, emit)
		if res.err == nil {
			emit(agent.Event{Type: agent.EventSettled})
			return nil
		}
		if res.err.Class == agent.ErrContextOverflow && !compacted {
			compacted = true
			if cr, cerr := s.cl().Compact(ctx, ""); cerr == nil {
				emit(agent.Event{Type: agent.EventCompaction, TokensBefore: cr.TokensBefore, TokensAfter: cr.EstimatedTokensAfter})
				s.usage(ctx, emit)
				continue
			}
		}
		emit(agent.Event{Type: agent.EventError, ErrorClass: res.err.Class, HTTPStatus: res.err.HTTPStatus, Message: res.err.Message})
		return nil
	}
}

type runResult struct {
	err *agent.Classified // the run ended with a provider error
}

// run performs one prompt and translates Pi's events.
func (s *Session) run(ctx context.Context, p pirpc.PromptParams, emit func(agent.Event)) (runResult, error) {
	var res runResult
	var lastErr string // provider error of the last assistant message, if it failed
	var finalErr string
	aborted := false
	err := s.cl().PromptAndWait(ctx, p, func(e pirpc.Event) {
		switch e.Type {
		case pirpc.EventMessageUpdate:
			var u pirpc.MessageUpdate
			if e.Decode(&u) != nil {
				return
			}
			switch u.AssistantMessageEvent.Type {
			case "text_delta":
				emit(agent.Event{Type: agent.EventTextDelta, Delta: u.AssistantMessageEvent.Delta})
			case "thinking_delta":
				emit(agent.Event{Type: agent.EventThinkingDelta, Delta: u.AssistantMessageEvent.Delta})
			}
		case pirpc.EventMessageEnd:
			var me pirpc.MessageEnd
			if e.Decode(&me) != nil || me.Message.Role != "assistant" {
				return
			}
			switch me.Message.StopReason {
			case "error":
				lastErr = me.Message.ErrorMessage
			case "aborted":
				aborted = true
			default:
				lastErr = "" // a later successful answer clears an earlier failure (retry)
			}
		case pirpc.EventToolExecutionStart:
			var te pirpc.ToolExecution
			if e.Decode(&te) == nil {
				emit(agent.Event{Type: agent.EventToolCall, ID: te.ToolCallID, Name: te.ToolName, Args: clipJSON(te.Args)})
			}
		case pirpc.EventToolExecutionEnd:
			var te pirpc.ToolExecution
			if e.Decode(&te) == nil {
				emit(agent.Event{Type: agent.EventToolResult, ID: te.ToolCallID, Name: te.ToolName, IsError: te.IsError,
					Summary: clip(resultText(te.Result), maxFieldBytes)})
			}
		case pirpc.EventAutoRetryStart:
			var r pirpc.AutoRetry
			if e.Decode(&r) == nil {
				emit(agent.Event{Type: agent.EventRetry, Attempt: r.Attempt, ErrorClass: agent.Classify(r.ErrorMessage).Class})
			}
		case pirpc.EventAutoRetryEnd:
			var r pirpc.AutoRetry
			if e.Decode(&r) == nil && !r.Success {
				finalErr = r.FinalError
			}
		case pirpc.EventCompactionEnd:
			var c pirpc.Compaction
			if e.Decode(&c) == nil && c.Result != nil {
				emit(agent.Event{Type: agent.EventCompaction, TokensBefore: c.Result.TokensBefore, TokensAfter: c.Result.EstimatedTokensAfter})
			}
		}
	})
	if err != nil {
		return res, err
	}
	if aborted && lastErr == "" {
		return res, context.Canceled
	}
	if lastErr != "" || finalErr != "" {
		msg := lastErr
		if msg == "" {
			msg = finalErr
		}
		c := agent.Classify(msg)
		res.err = &c
	}
	return res, nil
}

// usage emits the usage since the last report (get_session_stats totals
// include compaction and tool-reported usage).
func (s *Session) usage(ctx context.Context, emit func(agent.Event)) {
	c := s.cl()
	st, err := c.GetSessionStats(ctx)
	if err != nil {
		return
	}
	s.mu.Lock()
	b := s.base
	s.base = st
	s.mu.Unlock()
	u := agent.Usage{TokensIn: st.Tokens.Input - b.Tokens.Input, TokensOut: st.Tokens.Output - b.Tokens.Output,
		CacheRead: st.Tokens.CacheRead - b.Tokens.CacheRead, CacheWrite: st.Tokens.CacheWrite - b.Tokens.CacheWrite,
		CostUSD: st.Cost - b.Cost}
	if u.TokensIn < 0 || u.TokensOut < 0 || u.CostUSD < 0 { // stats were reset (switch, restart)
		u = agent.Usage{TokensIn: st.Tokens.Input, TokensOut: st.Tokens.Output, CacheRead: st.Tokens.CacheRead,
			CacheWrite: st.Tokens.CacheWrite, CostUSD: st.Cost}
	}
	if !u.IsZero() {
		emit(agent.Event{Type: agent.EventUsage, Usage: u})
	}
}

// restart replaces a crashed process, resuming the session file Pi wrote.
func (s *Session) restart(ctx context.Context) error {
	if c := s.cl(); c != nil {
		_ = c.Close()
	}
	s.mu.Lock()
	file := s.file
	s.mu.Unlock()
	if _, err := os.Stat(file); err != nil {
		file = ""
	}
	return s.start(ctx, file)
}

func (s *Session) stderrTail(err error) string {
	var ee *pirpc.ExitError
	if errors.As(err, &ee) {
		return ee.StderrTail
	}
	if c := s.cl(); c != nil {
		return c.StderrTail()
	}
	return ""
}

// Abort stops the current run.
func (s *Session) Abort(ctx context.Context) error { return s.cl().Abort(ctx) }

// SetModel switches the model and level; empty values keep the current ones.
func (s *Session) SetModel(ctx context.Context, modelID, thinking string) error {
	c := s.cl()
	if modelID != "" {
		if _, err := c.SetModel(ctx, s.req.Model.Provider, modelID); err != nil {
			return err
		}
		s.mu.Lock()
		s.model = modelID
		s.mu.Unlock()
	}
	if thinking != "" {
		if err := c.SetThinkingLevel(ctx, thinking); err != nil {
			return err
		}
		s.mu.Lock()
		s.thinking = thinking
		s.mu.Unlock()
	}
	return nil
}

// Snapshot returns the Pi session file.
func (s *Session) Snapshot(ctx context.Context) ([]byte, error) {
	st, err := s.cl().GetState(ctx)
	if err != nil {
		return nil, err
	}
	if st.SessionFile == "" {
		return nil, errors.New("the session has no file")
	}
	return os.ReadFile(st.SessionFile)
}

// Close stops Pi and removes the session directory.
func (s *Session) Close() error {
	if c := s.cl(); c != nil {
		_ = c.Close()
	}
	return os.RemoveAll(s.layout.Root)
}

// Done is closed when the Pi process exits.
func (s *Session) Done() <-chan struct{} { return s.cl().Done() }

func historyBlock(h []agent.HistoryMessage) string {
	var b strings.Builder
	b.WriteString("[Nabu — the previous conversation with this user, restored after the session moved. Continue it.]\n")
	for _, m := range h {
		who := "User"
		if m.Role == "assistant" {
			who = "You"
		}
		fmt.Fprintf(&b, "%s: %s\n", who, clip(m.Text, 2000))
	}
	return strings.TrimSpace(b.String())
}

func resultText(raw json.RawMessage) string {
	var r struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(raw, &r) != nil {
		return ""
	}
	var parts []string
	for _, c := range r.Content {
		if c.Type == "text" {
			parts = append(parts, c.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8Start(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

func utf8Start(b byte) bool { return b&0xC0 != 0x80 }

func clipJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) <= maxFieldBytes {
		return raw
	}
	b, _ := json.Marshal(map[string]string{"truncated": clip(string(raw), maxFieldBytes)})
	return b
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	return clip(s, 300)
}

// maskWriter replaces secret values before diagnostics reach the logs.
type maskWriter struct {
	mu      sync.Mutex
	w       io.Writer
	secrets []string
}

func (m *maskWriter) Write(p []byte) (int, error) {
	out := p
	for _, s := range m.secrets {
		out = bytes.ReplaceAll(out, []byte(s), []byte("***"))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.w.Write(out); err != nil {
		return 0, err
	}
	return len(p), nil
}
