package engine

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Reasons of the stall detector (FTR.NAB.CMN-0010 R2).
const (
	stallRepeatedCall = "repeated_call"
	stallToolErrors   = "tool_errors"
	// stallNoOutput is the reason of the silence watchdog (R10): the operator
	// brought no events for TURN_SILENCE_TIMEOUT.
	stallNoOutput = "no_output"
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
// label of the counter and of the journal: personal, group, task or service.
// The kind is kept even with the detector off: the silence watchdog (R10)
// names it too.
func (e *Engine) watchStall(col *collector, kind string) {
	col.stallKind = kind
	if !e.Cfg.StallDetector {
		return
	}
	col.stall = &stallDetector{repeatLimit: e.Cfg.StallRepeatLimit, errorLimit: e.Cfg.StallErrorLimit}
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

// watchSilence ends a turn whose model request brings nothing for longer than
// TURN_SILENCE_TIMEOUT (FTR.NAB.CMN-0010 R10). Before this, a provider that
// stopped answering held the turn silently until TURN_TIMEOUT — half an hour
// of an empty answer that the user had to stop by hand.
//
// A running tool is not silence: while a call of the operator is in flight the
// turn waits for its answer as long as the tool runs (its own limits bound it).
// The returned function stops the watchdog and waits for its goroutine.
func (e *Engine) watchSilence(ctx context.Context, col *collector) func() {
	limit := e.Cfg.TurnSilenceTimeout
	if limit <= 0 {
		return func() {}
	}
	if col.lastEvent.Load() == 0 { // no event yet: the count starts now, not in 1970
		col.touch()
	}
	tick := min(limit/4, 15*time.Second)
	tick = max(tick, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			quiet, running := col.quiet()
			if running || quiet < limit {
				continue
			}
			if !col.silent.CompareAndSwap(false, true) {
				return
			}
			slog.WarnContext(ctx, "agent silent", "agent", col.audit.Agent, "kind", col.stallKind,
				"quiet", quiet.Round(time.Second), "limit", limit)
			metrics.AgentStalls.WithLabelValues(col.stallKind, stallNoOutput).Inc()
			col.abortTurn()
			return
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// turnAudit builds the row of a turn that ended as no usual answer
// (FTR.NAB.CMN-0010 R11); ok is false when the turn ended with a usual answer
// or when its provider error is already a row of the tool "model"
// (FTR.NAB.CMN-0001 R28). A turn the user stopped gets its row even then: the
// stop is the fact an administrator looks for.
func turnAudit(a ledger.Entry, res turnResult, providerFailed bool) (ledger.Entry, bool) {
	if res.podLost || (providerFailed && !res.stopped) {
		return a, false
	}
	status := res.status()
	if status == "done" && res.stalled == "" {
		return a, false
	}
	a.Tool, a.Result = "turn", "error"
	reason := res.errClass
	switch {
	case status == "stopped":
		reason = "stopped"
	case res.stalled != "":
		reason = "stalled:" + res.stalled
		if status == "done" { // the detector stopped an answer that still finished: not a failure
			a.Result = "ok"
		}
	}
	if reason == "" {
		reason = status
	}
	a.Error = &reason
	return a, true
}

// auditTurn writes the row of a turn that did not end as a usual answer.
func (e *Engine) auditTurn(ctx context.Context, col *collector, res turnResult) {
	if e.Ledger == nil {
		return
	}
	a, ok := turnAudit(col.audit, res, col.failure != nil)
	if !ok {
		return
	}
	e.Ledger.Audit(context.WithoutCancel(ctx), a, "", col.secrets)
	switch res.status() {
	case "failed":
		slog.WarnContext(ctx, "turn ended", "agent", a.Agent, "channel", a.Channel, "status", res.status(), "reason", *a.Error)
	case "stopped":
		slog.InfoContext(ctx, "turn ended", "agent", a.Agent, "channel", a.Channel, "status", res.status(), "reason", *a.Error)
	}
}
