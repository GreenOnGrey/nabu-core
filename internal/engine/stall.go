package engine

import (
	"context"
	"crypto/sha256"
	"log/slog"

	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Reasons of the stall detector (FTR.NAB.CMN-0010 R2).
const (
	stallRepeatedCall = "repeated_call"
	stallToolErrors   = "tool_errors"
)

// stallPrompt is the wrap-up request of a stopped turn (FTR.NAB.CMN-0010 R4).
const stallPrompt = "Stop: you have repeated the same action or kept failing with no result, so the run ends now.\n" +
	"Do not call any tools. Reply with: what is done and verified, what failed and why, what to try next."

// stallDetector counts the calls of one turn in a row (FTR.NAB.CMN-0010 R2).
// Only hashes are kept: no arguments or results stay in memory.
type stallDetector struct {
	repeatLimit, errorLimit int
	last                    [sha256.Size]byte
	repeats, errors         int
}

// observe counts a finished call and returns the reason of a stall, or "".
func (d *stallDetector) observe(name, args, result string, isError bool) string {
	h := sha256.New()
	for _, s := range []string{name, args, result} {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	if d.repeats > 0 && sum == d.last {
		d.repeats++
	} else {
		d.last, d.repeats = sum, 1
	}
	if isError {
		d.errors++
	} else {
		d.errors = 0
	}
	switch {
	case d.repeatLimit > 0 && d.repeats >= d.repeatLimit:
		return stallRepeatedCall
	case d.errorLimit > 0 && d.errors >= d.errorLimit:
		return stallToolErrors
	}
	return ""
}

// reset starts a new count: a new turn of the same session.
func (d *stallDetector) reset() { d.repeats, d.errors = 0, 0 }

// watchStall turns the detector on for the turn of a collector; kind is the
// label of the counter: personal, group, task or service.
func (e *Engine) watchStall(col *collector, kind string) {
	if !e.Cfg.StallDetector {
		return
	}
	col.stall = &stallDetector{repeatLimit: e.Cfg.StallRepeatLimit, errorLimit: e.Cfg.StallErrorLimit}
	col.stallKind = kind
}

// observeStall feeds the detector with a finished call and stops the turn when
// it fires (FTR.NAB.CMN-0010 R4, R9). The caller finishes with finishStalled.
func (c *collector) observeStall(ev agent.Event) {
	if c.stall == nil || c.stalled != "" {
		return
	}
	reason := c.stall.observe(ev.Name, c.args[ev.ID], ev.Summary, ev.IsError)
	if reason == "" {
		return
	}
	c.stalled, c.stalledTool = reason, ev.Name
	slog.WarnContext(c.ctx, "agent stalled", "agent", c.audit.Agent, "kind", c.stallKind, "reason", reason, "tool", ev.Name)
	metrics.AgentStalls.WithLabelValues(c.stallKind, reason).Inc()
	c.abortTurn()
}

// abortTurn aborts the running prompt without waiting for the operator: this
// runs inside the stream of its events.
func (c *collector) abortTurn() {
	if c.abort != nil {
		go c.abort()
	}
}

// finishStalled asks the model for the wrap-up of a stopped turn: an error of
// this request is not an error of the turn (FTR.NAB.CMN-0010 tech §4).
func (e *Engine) finishStalled(ctx context.Context, op Operator, sessionID string, col *collector) {
	col.wrapup = true
	defer func() { col.wrapup = false }()
	if col.text.Len() > 0 {
		col.handle(agent.Event{Type: agent.EventTextDelta, Delta: "\n\n"})
	}
	wctx, cancel := context.WithTimeout(ctx, e.Cfg.StallWrapupTimeout)
	defer cancel()
	if err := op.Prompt(wctx, sessionID, agent.PromptRequest{Text: stallPrompt, Deadline: deadline(wctx)}, col.handle); err != nil {
		slog.WarnContext(ctx, "the wrap-up of a stalled turn", "err", err)
		_ = op.Abort(context.WithoutCancel(ctx), sessionID)
	}
	col.failure = nil
}

// stallNote adds the explanation to the answer of a chat or a task
// (FTR.NAB.CMN-0010 R5): through the text stream, so every channel has it.
func stallNote(col *collector, lang string) {
	if col.text.Len() > 0 {
		col.handle(agent.Event{Type: agent.EventTextDelta, Delta: "\n\n"})
	}
	col.handle(agent.Event{Type: agent.EventTextDelta, Delta: channels.T(lang, "answer.stalled")})
}
