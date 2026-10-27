package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Subagents (FTR.NAB.CMN-0009): the tool subagent of the nabu-workspace
// extension asks the operator over loopback to run 1–4 child Pi sessions for
// the turn being run. A child is built from the request of its parent, gets
// no conversation and no subagents of its own, and returns its last message.

// SubagentSystem is appended to the instructions of a subagent (tech §2.3).
const SubagentSystem = "You are a subagent started by the main agent for one task. You do not see its conversation; everything you need is in the task. " +
	"Do only this task, do not ask the user questions. Your last message is returned to the main agent as your result: make it complete and self-contained. " +
	"Put long results (over a few pages) into a file in the working directory and give its path."

// eventResultChars bounds the result in the stream of the turn and in the
// answer: it is for the person (tech §3, decision 2).
const eventResultChars = 2000

// turn is the prompt a session runs: subagents live within it.
type turn struct {
	ctx      context.Context
	deadline time.Time
	emit     func(agent.Event)
	started  int // subagents of the turn

	// calls are the subagent calls running in the turn: the turn ends
	// (settled) only after their last events, so a stop of the turn still
	// brings «aborted» to the worker (FTR.NAB.CHAT-0004 R12).
	calls  sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

// callDrain bounds the wait for subagent calls at the end of a turn: their
// children are aborted by then and only close.
const callDrain = 15 * time.Second

func (e *entry) begin(ctx context.Context, deadline time.Time, emit func(agent.Event)) *turn {
	t := &turn{ctx: ctx, deadline: deadline, emit: emit}
	e.mu.Lock()
	e.turn = t
	e.mu.Unlock()
	return t
}

// end waits for the calls of the turn and stops its stream: an event of a
// subagent after that is dropped, never written to a finished response.
func (e *entry) end() {
	e.mu.Lock()
	t := e.turn
	e.turn = nil
	e.mu.Unlock()
	if t == nil {
		return
	}
	t.drain()
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
}

// drain waits for the subagent calls of the turn, at most callDrain.
func (t *turn) drain() {
	done := make(chan struct{})
	go func() { t.calls.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(callDrain):
	}
}

// send writes an event of a subagent into the stream of the turn while it is open.
func (t *turn) send(ev agent.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.emit(ev)
	}
}

// subagentEnv tells the Pi of a session where to start its subagents.
func subagentEnv(addr, id, token string, maxParallel int) []string {
	return []string{"NABU_SUBAGENT_URL=http://" + addr + "/v1/sessions/" + id + "/subagents", "NABU_SUBAGENT_TOKEN=" + token,
		"NABU_SUBAGENT_DEPTH=0", "NABU_SUBAGENT_MAX_PARALLEL=" + strconv.Itoa(maxParallel)}
}

// SubagentHandler serves the loopback endpoint of subagents (tech §2.2).
func (o *Operator) SubagentHandler() http.Handler {
	r := chi.NewRouter()
	r.Post("/v1/sessions/{id}/subagents", o.subagents)
	return r
}

// SubagentAddr is where SubagentHandler is to listen; "" — nowhere.
func (o *Operator) SubagentAddr() string { return o.cfg.SubagentAddr }

// callTask is a task of a call of the tool.
type callTask struct {
	Description string `json:"description"`
	Prompt      string `json:"prompt"`
	Skill       string `json:"skill,omitempty"`
}

// SubagentResult is the result of a task for the agent.
type SubagentResult struct {
	Task      int    `json:"task"`
	State     string `json:"state"`
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
	Error     string `json:"error,omitempty"`
}

func reject(w http.ResponseWriter, code int, reason, errCode, msg string) {
	metrics.SubagentRejected.WithLabelValues(reason).Inc()
	writeErr(w, code, errCode, msg)
}

func (o *Operator) subagents(w http.ResponseWriter, r *http.Request) {
	e := o.get(chi.URLParam(r, "id"))
	if e == nil || e.subToken == "" || !equal(bearer(r), e.subToken) {
		writeErr(w, http.StatusUnauthorized, "subagent_token", "the token does not belong to this session")
		return
	}
	var in struct {
		Call  string     `json:"call"`
		Tasks []callTask `json:"tasks"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	lim := e.req.Subagents
	if msg := checkTasks(in.Tasks, lim.MaxParallel); msg != "" {
		reject(w, http.StatusUnprocessableEntity, "call_limit", "subagent_tasks", msg)
		return
	}
	var names []string
	if e.req.Skills != nil {
		names = e.req.Skills.Names
	}
	for _, t := range in.Tasks {
		if t.Skill != "" && !slices.Contains(names, t.Skill) {
			writeErr(w, http.StatusUnprocessableEntity, "subagent_skill", "the skill "+t.Skill+" is not in this session")
			return
		}
	}
	e.mu.Lock()
	tn := e.turn
	switch {
	case tn == nil:
		e.mu.Unlock()
		writeErr(w, http.StatusConflict, "subagent_no_turn", "the session runs no prompt")
		return
	case tn.started+len(in.Tasks) > lim.MaxPerTurn:
		left := lim.MaxPerTurn - tn.started
		e.mu.Unlock()
		reject(w, http.StatusTooManyRequests, "turn_limit", "subagent_turn_limit",
			fmt.Sprintf("this turn may start %d more subagents of %d; do the rest yourself", max(left, 0), lim.MaxPerTurn))
		return
	}
	tn.started += len(in.Tasks)
	tn.calls.Add(1)
	e.mu.Unlock()
	defer tn.calls.Done()
	refund := func() { e.mu.Lock(); tn.started -= len(in.Tasks); e.mu.Unlock() }

	// the call ends with the tool (the extension goes away) or with the turn (abort, close)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(tn.ctx, cancel)
	defer stop()

	// R6: a place in the pod for every task; without one for any within slotWait the call is refused
	slots := o.reserve(e.kind, len(in.Tasks))
	if slots == 0 {
		if !o.waitSlot(ctx, e.kind, time.Duration(lim.SlotWait)) {
			refund()
			reject(w, http.StatusServiceUnavailable, "no_slot", "subagent_no_slot", "the agent has no free place for a subagent now; try later or do it yourself")
			return
		}
		slots = 1
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)
	var mu sync.Mutex
	enc := json.NewEncoder(w)
	line := func(v any) {
		mu.Lock()
		defer mu.Unlock()
		_ = enc.Encode(v)
		if fl != nil {
			fl.Flush()
		}
	}

	results := make([]SubagentResult, len(in.Tasks))
	var wg sync.WaitGroup
	for i, t := range in.Tasks {
		have := i < slots
		wg.Go(func() {
			if !have && !o.waitSlot(ctx, e.kind, time.Until(tn.deadlineOr(time.Hour))) {
				results[i] = SubagentResult{Task: i + 1, State: agent.SubagentAborted, Error: "no place in the pod"}
				tn.send(agent.Event{Type: agent.EventSubagent, Subagent: &agent.SubagentState{Call: in.Call, Task: i + 1,
					Description: t.Description, State: agent.SubagentAborted, Error: "no place in the pod"}})
				return
			}
			defer o.release(e.kind)
			results[i] = o.runChild(ctx, e, tn, in.Call, i+1, t, line)
		})
	}
	wg.Wait()
	line(map[string]any{"results": results})
}

// checkTasks returns why the tasks are refused, "" — they are fine.
// Bounds of a task (tech §2.2); the schema of the tool in the nabu-workspace
// extension states the same, so the model rarely gets here.
const (
	maxDescription = 80
	maxPrompt      = 64000
)

func checkTasks(tasks []callTask, maxParallel int) string {
	if len(tasks) == 0 || len(tasks) > maxParallel {
		return fmt.Sprintf("give 1–%d tasks", maxParallel)
	}
	for i, t := range tasks {
		if n := utf8.RuneCountInString(strings.TrimSpace(t.Description)); n == 0 || n > maxDescription {
			return fmt.Sprintf("task %d: the description takes 1–%d characters", i+1, maxDescription)
		}
		if n := utf8.RuneCountInString(t.Prompt); strings.TrimSpace(t.Prompt) == "" || n > maxPrompt {
			return fmt.Sprintf("task %d: the prompt takes 1–%d characters", i+1, maxPrompt)
		}
	}
	return ""
}

func (t *turn) deadlineOr(d time.Duration) time.Time {
	if t.deadline.IsZero() {
		return time.Now().Add(d)
	}
	return t.deadline
}

// reserve takes up to n places of the kind at once and returns how many.
func (o *Operator) reserve(k agent.SessionKind, n int) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	got := min(n, max(o.limit(k)-o.counts[k], 0))
	o.counts[k] += got
	return got
}

func (o *Operator) release(k agent.SessionKind) {
	o.mu.Lock()
	o.counts[k]--
	o.mu.Unlock()
}

// waitSlot waits up to d for a place of the kind and takes it.
func (o *Operator) waitSlot(ctx context.Context, k agent.SessionKind, d time.Duration) bool {
	t := time.NewTimer(max(d, 0))
	defer t.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		if o.reserve(k, 1) == 1 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return false
		case <-tick.C:
		}
	}
}

// childRequest is the session of a subagent built from its parent's
// (tech §2.3): the same model, secrets, MCP servers, skills and workspace;
// no history, no snapshot, no subagents.
func childRequest(parent agent.SessionRequest, model, thinking, mark string) agent.SessionRequest {
	req := parent
	req.Model.ModelID, req.Model.Thinking = model, thinking
	req.Snapshot, req.History, req.Subagents = nil, nil, nil
	req.SystemAppend = strings.TrimSpace(parent.SystemAppend + "\n\n" + SubagentSystem)
	req.Label = "subagent " + mark + " of " + parent.Label
	return req
}

// childPrompt calls the skill first, as a turn of the user does.
func childPrompt(t callTask) string {
	if t.Skill != "" {
		return "/skill:" + t.Skill + " " + t.Prompt
	}
	return t.Prompt
}

// clipEllipsis cuts the result for the person at a character, with «…»:
// the answer shows it as is.
func clipEllipsis(text string, limit int) string {
	if r := []rune(text); len(r) > limit {
		return string(r[:limit]) + "…"
	}
	return text
}

// clipResult cuts the result at a character boundary and says so (tech §2.3).
func clipResult(text string, limit int) (string, bool) {
	n := utf8.RuneCountInString(text)
	if limit <= 0 || n <= limit {
		return text, false
	}
	r := []rune(text)
	return string(r[:limit]) + fmt.Sprintf("\n[truncated: %d of %d characters; ask the subagent to write long results to a file]", limit, n), true
}

// runChild runs one task to its end.
func (o *Operator) runChild(ctx context.Context, e *entry, tn *turn, call string, task int, t callTask, line func(any)) SubagentResult {
	lim := e.req.Subagents
	started := time.Now()
	st := agent.SubagentState{Call: call, Task: task, Description: t.Description, State: agent.SubagentRunning}
	var usage agent.Usage
	var mu sync.Mutex
	report := func() { // to the stream of the turn and to the extension
		mu.Lock()
		ev := agent.Event{Type: agent.EventSubagent, Subagent: new(st), Usage: usage}
		usage = agent.Usage{}
		mu.Unlock()
		tn.send(ev)
		line(map[string]any{"task": task, "state": ev.Subagent.State, "tools": ev.Subagent.Tools})
	}
	finish := func(state, text, errText string) SubagentResult {
		res := SubagentResult{Task: task, State: state, Error: errText}
		res.Text, res.Truncated = clipResult(text, lim.ResultChars)
		mu.Lock()
		st.State, st.Error = state, errText
		if state == agent.SubagentDone || state == agent.SubagentTimeout {
			st.Result = clipEllipsis(text, eventResultChars)
		}
		mu.Unlock()
		report()
		metrics.SubagentRuns.WithLabelValues(string(e.kind), state).Inc()
		metrics.SubagentDuration.WithLabelValues(string(e.kind)).Observe(time.Since(started).Seconds())
		slog.Info("subagent finished", "session", e.id, "label", e.label, "call", call, "task", task, "state", state)
		return res
	}
	report()

	timeout := time.Duration(lim.Timeout)
	if !tn.deadline.IsZero() {
		timeout = min(timeout, time.Until(tn.deadline))
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	model, thinking := e.s.Model() // R5: the model of the parent now, after a Patch
	mark := call + "/" + strconv.Itoa(task)
	req := childRequest(e.req, model, thinking, mark)
	rt := o.cfg.Runtime
	rt.SkillsDir = e.skillsDir
	id := e.id + "-" + call + "-" + strconv.Itoa(task)
	rt.Stderr = &logWriter{session: id, label: req.Label}
	rt.ExtraEnv = append(append([]string{}, rt.ExtraEnv...), "NABU_SUBAGENT_DEPTH=1")
	metrics.AgentProcessStarts.WithLabelValues("subagent").Inc()
	slog.Info("subagent started", "session", e.id, "label", e.label, "call", call, "task", task)
	sctx, scancel := context.WithTimeout(cctx, 90*time.Second)
	s, err := pi.Start(sctx, rt, filepath.Join(o.cfg.WorkDir, "sessions", "sub-"+randID()), req)
	scancel()
	if err != nil {
		return finish(taskEnd(cctx, ctx), "", "the subagent did not start: "+err.Error())
	}
	defer func() { _ = s.Close() }()

	// the last message of the assistant is the result: the text after the last tool call
	var text strings.Builder
	afterTool := false
	failure := ""
	aborted := false
	emit := func(ev agent.Event) {
		if ev.Type == agent.EventToolCall || ev.Type == agent.EventToolResult {
			ev.Of = mark // R9: the worker audits the tools of the subagent
			tn.send(ev)
		}
		mu.Lock()
		changed := false
		switch ev.Type {
		case agent.EventTextDelta:
			if afterTool {
				text.Reset()
				afterTool = false
			}
			text.WriteString(ev.Delta)
		case agent.EventToolCall:
			st.Tools++
			afterTool, changed = true, true
		case agent.EventUsage:
			usage.Add(ev.Usage)
			changed = true
		case agent.EventError:
			failure = ev.Message
		case agent.EventSettled:
			aborted = ev.Aborted
		}
		mu.Unlock()
		if changed {
			report()
		}
	}
	done := make(chan struct{})
	go func() { // a timeout or the end of the turn aborts the run
		select {
		case <-cctx.Done():
			_ = s.Abort(context.WithoutCancel(cctx))
		case <-done:
		}
	}()
	err = s.Prompt(cctx, agent.PromptRequest{Text: childPrompt(t)}, emit)
	close(done)
	mu.Lock()
	out := strings.TrimSpace(text.String())
	mu.Unlock()
	switch {
	case cctx.Err() != nil || aborted:
		return finish(taskEnd(cctx, ctx), out, "")
	case failure != "":
		return finish(agent.SubagentFailed, out, failure)
	case err != nil:
		return finish(agent.SubagentFailed, out, err.Error())
	}
	return finish(agent.SubagentDone, out, "")
}

// taskEnd tells a timeout of the task from the end of the call or the turn.
func taskEnd(cctx, call context.Context) string {
	if call.Err() == nil && errors.Is(cctx.Err(), context.DeadlineExceeded) {
		return agent.SubagentTimeout
	}
	if cctx.Err() == nil {
		return agent.SubagentFailed
	}
	return agent.SubagentAborted
}
