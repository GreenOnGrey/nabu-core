package engine

// The test cases named in this file are also of FTR.NAB.CMN-0001, FTR.NAB.CMN-0002 and FTR.NAB.CMN-0004.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/agentpods"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/space"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
)

// ─── scheduled task runs (tech §9a) ─────────────────────────────────

// HandleTaskRun performs one run of a scheduled task in a separate session of
// the user's personal agent: the same memory, connections and space; only the
// final message goes to the main conversation (TSK-07) and to the task's
// channel (TSK-06).
func (e *Engine) HandleTaskRun(ctx context.Context, key, value []byte) error {
	var m tasks.RunMessage
	if err := json.Unmarshal(value, &m); err != nil {
		return nil
	}
	ri, err := e.Tasks.LoadRun(ctx, m.RunID)
	if err != nil {
		return err
	}
	ref := "t:" + m.RunID.String()
	if ri == nil || ri.Status != "running" {
		e.Pods.Drop(ctx, m.UserID, ref) // nobody will run it: the later turns of the owner go on
		return nil
	}
	if e.Tasks.Cancelled(ctx, ri.TaskID) { // TSK-05: a cancelled task does not run
		_, _ = e.Pool.Exec(ctx, `DELETE FROM task_runs WHERE id = $1`, ri.RunID)
		e.Pods.Drop(ctx, ri.UserID, ref)
		return nil
	}
	u, err := e.Users.Get(ctx, ri.UserID)
	if err != nil || u == nil {
		return err
	}
	if u.Status == "blocked" || u.Status == "archived" { // TSK-10
		e.Pods.Drop(ctx, u.ID, ref)
		return e.Tasks.Finish(ctx, ri.RunID, false, "", "user_blocked", "the user is blocked", nil)
	}
	if ri.Kind != "" && ri.Kind != tasks.KindAgent {
		// FTR.NAB.CMN-0003 arch §1: a memory task needs neither the pod of the
		// user nor their personal agent.
		e.Pods.Drop(ctx, u.ID, ref)
		return e.memoryTask(ctx, u, ri)
	}
	// Q-16: a run waits for the pod of its user like a message does.
	turn := agentpods.Turn{Kind: agentpods.KindTask, Ref: ref, Topic: kafka.TopicTaskRun, Key: string(key), Value: value, Channel: ri.Channel}
	lease, err := e.acquire(ctx, u.ID, turn)
	if errors.Is(err, agentpods.ErrQueued) {
		return nil
	}
	if err != nil {
		return err
	}
	defer lease.Release(ctx)
	ctx, cancel := context.WithTimeout(ctx, e.Cfg.TaskRunTimeout)
	defer cancel()
	e.dropRunFiles(ctx, u.ID, ri.RunID) // FTR.NAB.CHAT-0001 R33: the files of an earlier attempt are not this run's
	text, class, errText, lost := e.taskTurn(ctx, lease, ri)
	if lost {
		// R5: the pod went away before the run began — it waits for a new one.
		if err := e.Pods.Requeue(ctx, lease, turn, uuid.Nil); err == nil {
			return nil
		}
	}
	main, err := e.Chat.Main(ctx, u.ID)
	if err != nil {
		return err
	}
	if class != "" {
		e.dropRunFiles(ctx, u.ID, ri.RunID)
		_ = e.Tasks.Finish(ctx, ri.RunID, false, "", class, errText, nil)
		if paused, reason := e.Tasks.PausedNow(ctx, ri.TaskID); paused {
			note := fmt.Sprintf("The scheduled task «%s» failed three times in a row and was paused. Reason: %s. Resume it in the Tasks section after fixing the cause.", ri.Title, reason)
			if u.Language == "ru" {
				note = fmt.Sprintf("Задание «%s» трижды подряд завершилось ошибкой и приостановлено. Причина: %s. Возобновите его в разделе «Задания», когда причина будет устранена.", ri.Title, reason)
			}
			e.postTaskMessage(ctx, u.ID, main.ID, ri, note)
		}
		return nil
	}
	files, err := e.Chat.TakeRunFiles(ctx, ri.RunID)
	if err != nil {
		slog.WarnContext(ctx, "files of a run", "run", ri.RunID, "err", err)
	}
	msg := e.postTaskCard(ctx, u.ID, main.ID, ri, text, nil, files)
	var mid *uuid.UUID
	if msg != nil {
		mid = &msg.ID
	}
	return e.Tasks.Finish(ctx, ri.RunID, true, firstLine(text), "", "", mid)
}

// postTaskMessage writes the result to the main conversation with the task
// mark and sends it to the task's channel.
func (e *Engine) postTaskMessage(ctx context.Context, uid, conv uuid.UUID, ri *tasks.RunInfo, text string) *chat.Message {
	return e.postTaskCard(ctx, uid, conv, ri, text, nil, nil)
}

// postTaskCard is postTaskMessage with a card for the web chat (the channels
// get the text: FTR.NAB.CMN-0003 arch §3.5) and the files the agent sent during
// the run, which go under the result and after its text (FTR.NAB.CHAT-0001 R33).
func (e *Engine) postTaskCard(ctx context.Context, uid, conv uuid.UUID, ri *tasks.RunInfo, text string, card json.RawMessage, files []chat.Attachment) *chat.Message {
	m, err := e.Chat.AddAssistant(ctx, conv, "task:"+ri.TaskID.String(), nil, "", nil)
	if err != nil {
		slog.ErrorContext(ctx, "task message", "err", err)
		e.dropFiles(ctx, uid, files)
		return nil
	}
	if len(files) > 0 {
		if err := e.Chat.SetAttachments(ctx, m.ID, files); err != nil {
			slog.ErrorContext(ctx, "task files", "err", err)
			e.dropFiles(ctx, uid, files)
		}
	}
	final, err := e.Chat.Finish(ctx, m.ID, text, "done", nil, "", "")
	if err != nil {
		return nil
	}
	if len(card) > 0 {
		if err := e.Chat.SetCard(ctx, m.ID, card); err == nil {
			final.Card = card
		}
	}
	e.publish(ctx, events.MessageCreated, uid, final)
	e.publish(ctx, events.MessageDone, uid, final)
	if ri.Channel == domain.ChannelWeb || ri.Channel == "" {
		return final
	}
	// R5, CH-07: an unavailable channel — the result stays in the web, the
	// run is marked and stays successful.
	if ok, why := e.Deliverable(ctx, uid, ri.Channel); !ok {
		slog.InfoContext(ctx, "task channel unavailable, delivered to the web", "task", ri.TaskID, "channel", ri.Channel, "reason", why)
		e.Tasks.NoteDelivery(ctx, ri.RunID, "channel_unavailable")
		return final
	}
	if ri.Channel == domain.ChannelEmail {
		u, err := e.Users.Get(ctx, uid)
		if err != nil || u == nil || e.Mail == nil {
			e.Tasks.NoteDelivery(ctx, ri.RunID, "channel_unavailable")
			return final
		}
		if sent, err := e.Mail.SendTask(context.WithoutCancel(ctx), u.Email, ri.Title, text, u.Language, e.outFiles(uid, final.Attachments)); err != nil || !sent {
			if err != nil {
				slog.WarnContext(ctx, "task mail", "task", ri.TaskID, "err", err)
			}
			e.Tasks.NoteDelivery(ctx, ri.RunID, "channel_unavailable")
		}
		return final
	}
	e.deliver(ctx, uid, ri.Channel, "⏰ "+ri.Title+"\n\n"+text)
	e.deliverFiles(ctx, uid, ri.Channel, final.Attachments)
	return final
}

// taskTurn runs a task in the pod of its user; the last result reports a pod
// that went away before the run produced anything.
func (e *Engine) taskTurn(ctx context.Context, lease *agentpods.Lease, ri *tasks.RunInfo) (string, string, string, bool) {
	op := lease.Operator()
	user, err := e.Users.Get(ctx, ri.UserID)
	if err != nil || user == nil {
		return "", string(agent.ErrAgentCrashed), "the user is gone", false
	}
	if e.Sandboxes != nil && e.Space.Enabled {
		if err := e.Sandboxes.Ensure(ctx, user.ID); err != nil { // R37: the space comes up for the run
			slog.WarnContext(ctx, "sandbox start for a task", "err", err)
		}
	}
	b, err := e.personalRequest(ctx, user, uuid.Nil, &ri.TaskID, "task "+ri.TaskID.String()[:8])
	if err != nil {
		return "", "agent_not_configured", err.Error(), false
	}
	resp, err := e.open(ctx, op, b.req)
	if err != nil {
		return "", operatorClass(err), operatorText(err), e.podLost(lease, err)
	}
	defer op.Close(context.WithoutCancel(ctx), resp.SessionID)
	col := &collector{e: e, ctx: ctx, uid: user.ID, secrets: b.secrets,
		audit: ledger.Entry{AgentKind: "personal", Agent: b.persona.Agent.Name, UserID: &user.ID, Channel: "task:" + ri.TaskID.String()}}
	connID, _ := uuid.Parse(b.model.ConnectionID)
	col.usage = ledger.UsageRow{UserID: &user.ID, Model: b.model.ModelID, ConnectionID: &connID}
	e.watchStall(col, "task")
	col.abort = func() { _ = op.Abort(context.WithoutCancel(ctx), resp.SessionID) }
	prompt := channelRules(ri.Channel, time.Now(), user.Timezone) + "\n" +
		"[This is a run of the scheduled task «" + ri.Title + "», not a live chat: the user is not here to answer questions. " +
		"Do the work with your tools and reply with the final result for the user only — it will be delivered to the main conversation" +
		deliveryNote(ri.Channel) + ".]\n\nTask instruction: " + ri.Instruction
	err = op.Prompt(ctx, resp.SessionID, agent.PromptRequest{Text: prompt, Deadline: deadline(ctx)}, col.handle)
	if col.stalled != "" && err == nil && col.failure == nil { // FTR.NAB.CMN-0010 R4, R5
		e.finishStalled(ctx, op, resp.SessionID, col)
		stallNote(col, user.Language)
	}
	if col.failure != nil {
		return "", string(col.failure.ErrorClass), col.failure.Message, false
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			_ = op.Abort(context.WithoutCancel(ctx), resp.SessionID)
			return "", string(agent.ErrUnavailable), "the run did not finish within the time limit", false
		}
		return "", operatorClass(err), operatorText(err), col.text.Len() == 0 && len(col.steps) == 0 && e.podLost(lease, err)
	}
	text := strings.TrimSpace(col.text.String())
	if text == "" {
		text = "(the run finished without a message)"
	}
	return text, "", "", false
}

func deliveryNote(ch string) string {
	switch ch {
	case domain.ChannelTelegram:
		return " and to Telegram"
	case domain.ChannelVKTeams:
		return " and to VK Teams"
	case domain.ChannelEmail:
		return " and by mail"
	}
	return ""
}

// ─── service agent runs (arch §4.3, tech §5) ────────────────────────

// HandleRun performs a run of a service agent for a client.
func (e *Engine) HandleRun(ctx context.Context, _, value []byte) error {
	var m services.RunMessage
	if err := json.Unmarshal(value, &m); err != nil {
		return nil
	}
	var agentName, status string
	var clientID uuid.UUID
	var clientName string
	var initiator *string
	var input []byte
	err := e.Pool.QueryRow(ctx, `SELECT r.agent, r.status, r.client_id, c.name, r.initiator_email, r.input FROM runs r
		JOIN service_clients c ON c.id = r.client_id WHERE r.id = $1`, m.RunID).Scan(&agentName, &status, &clientID, &clientName, &initiator, &input)
	if err != nil || status != "queued" {
		return nil
	}
	tag, err := e.Pool.Exec(ctx, `UPDATE runs SET status = 'running' WHERE id = $1 AND status = 'queued'`, m.RunID)
	if err != nil || tag.RowsAffected() == 0 {
		return err
	}
	var in services.RunInput
	_ = json.Unmarshal(input, &in)
	ag, err := e.Services.Get(ctx, agentName)
	if err != nil {
		e.finishRun(ctx, m.RunID, agentName, "failed", "", "agent_not_configured", err.Error(), agent.Usage{})
		return nil
	}
	e.Services.AppendEvent(ctx, m.RunID, "started", map[string]any{"runId": m.RunID, "agent": agentName})
	timeout := time.Duration(ag.Limits.TimeoutSec) * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	summary, class, errText, usage, cancelled := e.serviceTurn(ctx, m.RunID, ag, clientID, clientName, initiator, in)
	switch {
	case cancelled:
		e.finishRun(ctx, m.RunID, agentName, "cancelled", summary, "", "", usage)
	case class != "":
		e.finishRun(ctx, m.RunID, agentName, "failed", summary, class, errText, usage)
	default:
		e.finishRun(ctx, m.RunID, agentName, "succeeded", summary, "", "", usage)
	}
	return nil
}

func (e *Engine) finishRun(ctx context.Context, id uuid.UUID, agentName, status, summary, class, errText string, u agent.Usage) {
	ub, _ := json.Marshal(u)
	ctx = context.WithoutCancel(ctx)
	_, _ = e.Pool.Exec(ctx, `UPDATE runs SET status = CASE WHEN status = 'cancelled' THEN status ELSE $2 END, summary = NULLIF($3,''),
		error_class = NULLIF($4,''), error_text = NULLIF($5,''), usage = $6, finished_at = now() WHERE id = $1`, id, status, summary, class, errText, ub)
	if class != "" {
		e.Services.AppendEvent(ctx, id, "error", map[string]any{"errorClass": class, "message": errText, "retryable": agent.ErrorClass(class).Retryable()})
	}
	e.Services.AppendEvent(ctx, id, "completed", map[string]any{"status": status, "summary": summary, "usage": u, "errorClass": class})
	metrics.ServiceRuns.WithLabelValues(agentName, status).Inc()
}

func (e *Engine) serviceTurn(ctx context.Context, runID uuid.UUID, ag *services.Agent, clientID uuid.UUID, clientName string, initiator *string,
	in services.RunInput) (summary, class, errText string, total agent.Usage, cancelled bool) {
	model, key, err := e.Models.Resolve(ctx, ag.Model)
	if err != nil {
		return "", "agent_not_configured", err.Error(), total, false
	}
	cat, err := e.Catalog.ForServiceAgent(ctx, ag.MCP, ag.Skills, runID.String(), tokenTTL)
	if err != nil {
		return "", string(agent.ErrAgentCrashed), err.Error(), total, false
	}
	req := agent.SessionRequest{Kind: agent.KindRun, Model: model, Label: ag.Name + " " + runID.String()[:8],
		Secrets: agent.Secrets{LLMKey: key, MCPHeaders: map[string]map[string]string{}}, SystemAppend: ag.Instructions,
		Subagents: e.Cfg.Subagents} // FTR.NAB.CMN-0009 tech §2.1
	secrets := []string{key}
	for _, s := range cat.Servers {
		req.MCP = append(req.MCP, s)
		req.Secrets.MCPHeaders[s.Name] = cat.Headers[s.Name]
		secrets = append(secrets, strings.TrimPrefix(cat.Headers[s.Name]["Authorization"], "Bearer "))
	}
	// SVC-08: the caller's MCP servers exist only in this run.
	for _, c := range in.CallerMCP {
		names := make([]string, 0, len(c.Headers))
		for h, v := range c.Headers {
			names = append(names, h)
			secrets = append(secrets, v, strings.TrimPrefix(v, "Bearer "))
		}
		req.MCP = append(req.MCP, agent.MCPServer{Name: c.Name, URL: c.URL, HeaderNames: names, Exposure: "direct", Description: clientName + " tools of this run"})
		req.Secrets.MCPHeaders[c.Name] = c.Headers
	}
	req.Skills = e.sessionSkills(ctx, cat)
	wsID, hasCopy := "", false // FTR.HMR.CMN-0008 R6, R7: the check runs in the sandbox of the run
	switch ag.Workspace {
	case services.WorkspaceNabu:
		if e.Sandboxes == nil || e.Sandboxes.K8s == nil {
			return "", "workspace_not_connected", "temporary sandboxes are not enabled in this deployment", total, false
		}
		wsID = services.WorkspaceID(runID)
		// FTR.HMR.CMN-0007 R5, R17: the working copy of the caller and the
		// proxies of the agent.
		files := e.runHasFiles(ctx, runID)
		hasCopy = files
		st, err := e.Sandboxes.StartEphemeral(ctx, wsID, runID.String(), ag.WorkspaceProfile,
			space.Ephemeral{Files: files, Proxies: ag.Proxies, TokenTTL: time.Duration(ag.Limits.TimeoutSec)*time.Second + 10*time.Minute})
		if err != nil {
			return "", "workspace_not_connected", err.Error(), total, false
		}
		rs := st.Proxies
		secrets = append(secrets, st.Secrets...) // FTR.HMR.DEV-0004 R10: the tokens of the sandbox and the proxies
		defer e.Sandboxes.StopEphemeral(context.WithoutCancel(ctx), wsID)
		if files {
			// Runs before StopEphemeral: the changes are taken from the live sandbox.
			defer e.collectChanges(context.WithoutCancel(ctx), runID, wsID)
		}
		call := e.Signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: wsID}, tokenTTL)
		secrets = append(secrets, call)
		note := "The working directory is a temporary sandbox of this run; its files are deleted after the run. " +
			"It has a fixed set of tools: see /nabu/sandbox-profile.json; system packages cannot be installed."
		if files {
			note = "The working directory is a copy of the repository of " + clientName + " for this run. " +
				"Change the files in place: " + clientName + " takes the changes after the run and commits them; do not create branches or commits. " +
				"Tools: see /nabu/sandbox-profile.json; system packages cannot be installed."
		}
		req.Workspace = &agent.Workspace{ID: wsID, URL: e.Cfg.RelayInternalURL + "/internal/v1/workspaces/" + wsID, Token: call,
			Note: note + " " + space.ProxyNote(rs), Root: space.SandboxRoot}
	}
	resp, err := e.open(ctx, e.Op, req)
	if err != nil {
		return "", operatorClass(err), operatorText(err), total, false
	}
	defer e.Op.Close(context.WithoutCancel(ctx), resp.SessionID)
	var cid = clientID
	col := &collector{e: e, ctx: ctx, secrets: secrets,
		audit: ledger.Entry{AgentKind: "service", Agent: ag.Name, ClientID: &cid, InitiatorEmail: initiator, Channel: "client:" + clientName}}
	connID, _ := uuid.Parse(model.ConnectionID)
	col.usage = ledger.UsageRow{RunID: &runID, ClientID: &cid, Agent: ag.Name, Model: model.ModelID, ConnectionID: &connID}
	col.costUnknown = !e.Models.PriceKnown(ctx, connID, model.ModelID)
	e.watchStall(col, "service")
	col.abort = func() { _ = e.Op.Abort(context.WithoutCancel(ctx), resp.SessionID) }
	limitHit := false
	col.onEvent = func(ev agent.Event) {
		switch ev.Type {
		case agent.EventTextDelta:
			e.Services.AppendEvent(ctx, runID, "text_delta", map[string]string{"delta": ev.Delta})
		case agent.EventToolCall:
			// FTR.HMR.DEV-0004 R10, arch §4.4: the events of a run are read by its client: no secrets in them
			e.Services.AppendEvent(ctx, runID, "tool_call", map[string]any{"id": ev.ID, "name": ev.Name, "args": maskArgs(ev.Args, secrets)})
		case agent.EventToolResult:
			e.Services.AppendEvent(ctx, runID, "tool_result", map[string]any{"id": ev.ID, "name": ev.Name, "isError": ev.IsError,
				"summary": clip(ledger.Mask(ev.Summary, secrets, math.MaxInt), 2000)})
		case agent.EventUsage:
			e.Services.AppendEvent(ctx, runID, "usage", ev.Usage)
			var t agent.Usage
			t.Add(col.total)
			t.Add(ev.Usage)
			if ag.Limits.MaxTokens > 0 && t.Total() > ag.Limits.MaxTokens && !limitHit { // SVC-06
				limitHit = true
				go func() { _ = e.Op.Abort(context.WithoutCancel(ctx), resp.SessionID) }()
			}
		}
	}
	// A cancellation by the client aborts the run and its check.
	checkCtx, cancelCheck := context.WithCancel(ctx)
	defer cancelCheck()
	var clientCancelled atomic.Bool
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				var st string
				_ = e.Pool.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1`, runID).Scan(&st)
				if st == "cancelled" {
					clientCancelled.Store(true)
					cancelCheck()
					_ = e.Op.Abort(context.WithoutCancel(ctx), resp.SessionID)
					return
				}
			}
		}
	}()
	prompt := in.Input
	if len(in.Context) > 0 && string(in.Context) != "null" {
		prompt = "[Context: " + clip(string(in.Context), 8000) + "]\n\n" + prompt
	}
	if initiator != nil {
		prompt = "[Started on the initiative of " + *initiator + "]\n" + prompt
	}
	err = e.Op.Prompt(ctx, resp.SessionID, agent.PromptRequest{Text: prompt, Deadline: deadline(ctx)}, col.handle)
	e.Models.RecordResult(ctx, model.ConnectionID, func() agent.ErrorClass {
		if col.failure != nil {
			return col.failure.ErrorClass
		}
		return ""
	}())
	// FTR.HMR.CMN-0008 R6, R9, R20; FTR.NAB.CMN-0010 R4, R6: after a first turn
	// that ended well the work is not lost, whatever happens later.
	if finishedTurn := err == nil && col.failure == nil && !limitHit && !clientCancelled.Load() && ctx.Err() == nil; finishedTurn && (col.stalled != "" || in.Check != nil) {
		e.afterServiceTurn(checkCtx, runID, wsID, hasCopy, resp.SessionID, in.Check, col, model.ConnectionID, secrets, &limitHit)
		summary, total = strings.TrimSpace(col.text.String()), col.total
		if clientCancelled.Load() {
			return summary, "", "", total, true
		}
		return summary, "", "", total, false
	}
	if in.Check != nil {
		e.Services.SetRunOutcome(ctx, runID, &services.CheckResult{Status: services.CheckNotRun, Reason: services.CheckReasonTurnFailed,
			Command: in.Check.Command, MaxAttempts: in.Check.MaxRepairs + 1}, "")
	}
	summary = strings.TrimSpace(col.text.String())
	total = col.total
	cancelled = clientCancelled.Load()
	switch {
	case cancelled:
		return summary, "", "", total, true
	case limitHit:
		return summary, "limit_exceeded", fmt.Sprintf("the run used more than %d tokens", ag.Limits.MaxTokens), total, false
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		_ = e.Op.Abort(context.WithoutCancel(ctx), resp.SessionID)
		return summary, "limit_exceeded", fmt.Sprintf("the run took longer than %d seconds", ag.Limits.TimeoutSec), total, false
	case col.failure != nil:
		return summary, string(col.failure.ErrorClass), col.failure.Message, total, false
	case err != nil:
		return summary, operatorClass(err), operatorText(err), total, false
	}
	return summary, "", "", total, false
}

// afterServiceTurn finishes a run whose first turn ended well: the wrap-up of
// a stalled turn and the readiness check with its repair rounds.
func (e *Engine) afterServiceTurn(ctx context.Context, runID uuid.UUID, wsID string, hasCopy bool, sessionID string, spec *services.RunCheck,
	col *collector, connectionID string, secrets []string, limitHit *bool) {
	stopReason := ""
	wrapup := func() {
		stopReason = services.StopStalled
		e.Services.AppendEvent(ctx, runID, "stalled", map[string]string{"reason": col.stalled, "tool": col.stalledTool})
		e.finishStalled(ctx, e.Op, sessionID, col)
	}
	maxRepairs := 0
	if col.stalled != "" {
		wrapup()
	} else if spec != nil {
		maxRepairs = spec.MaxRepairs
	}
	var result *services.CheckResult
	if spec != nil && ctx.Err() == nil {
		r := checkRunner{
			exec: func(ctx context.Context, command string, t int) (checkExec, error) {
				return e.sandboxExec(ctx, wsID, command, t)
			},
			event: func(typ string, data any) { e.Services.AppendEvent(ctx, runID, typ, data) },
			mask:  maskSecrets(secrets),
			repair: func(ctx context.Context, prompt string) repairOutcome {
				if col.stall != nil {
					col.stall.reset()
				}
				err := e.Op.Prompt(ctx, sessionID, agent.PromptRequest{Text: prompt, Deadline: deadline(ctx)}, col.handle)
				var class agent.ErrorClass
				if col.failure != nil {
					class = col.failure.ErrorClass
				}
				e.Models.RecordResult(ctx, connectionID, class)
				switch {
				case col.stalled != "" && err == nil && col.failure == nil && !*limitHit && ctx.Err() == nil:
					wrapup()
					return repairLastCheck
				case err != nil || col.failure != nil || *limitHit || ctx.Err() != nil:
					col.failure = nil
					return repairStopped
				}
				return repairDone
			},
		}
		if hasCopy {
			r.changed = func(ctx context.Context) (bool, error) { return e.sandboxChanged(ctx, wsID) }
		}
		res := r.run(ctx, *spec, maxRepairs)
		slog.InfoContext(ctx, "the check of a run", append([]any{"run", runID}, checkResultLog(res)...)...)
		result = &res
	}
	e.Services.SetRunOutcome(ctx, runID, result, stopReason)
}

// dropRunFiles removes the files a run collected (FTR.NAB.CHAT-0001 R33): the
// run failed or starts again.
func (e *Engine) dropRunFiles(ctx context.Context, uid, run uuid.UUID) {
	files, err := e.Chat.TakeRunFiles(ctx, run)
	if err != nil {
		slog.WarnContext(ctx, "files of a run", "run", run, "err", err)
		return
	}
	e.dropFiles(ctx, uid, files)
}

// dropFiles removes attachments that reached no message.
func (e *Engine) dropFiles(ctx context.Context, uid uuid.UUID, files []chat.Attachment) {
	for _, f := range files {
		if key, err := e.Chat.DeleteAttachment(ctx, uid, f.ID); err == nil {
			_ = e.S3.Delete(ctx, key)
		}
	}
}

// runHasFiles reports a working copy of the run (FTR.HMR.CMN-0007 tech §2.2).
func (e *Engine) runHasFiles(ctx context.Context, runID uuid.UUID) bool {
	var key *string
	_ = e.Pool.QueryRow(ctx, `SELECT files_key FROM runs WHERE id = $1`, runID).Scan(&key)
	return key != nil
}

// collectChanges asks the sandbox of the run for the changes of its working
// copy (tech §2.6); a failure is an event of the run, not its error.
func (e *Engine) collectChanges(ctx context.Context, runID uuid.UUID, wsID string) {
	ctx, cancel := context.WithTimeout(ctx, e.Cfg.RunChangesTimeout)
	defer cancel()
	fail := func(err error) {
		slog.WarnContext(ctx, "changes of a run", "run", runID, "err", err)
		e.Services.AppendEvent(ctx, runID, "changes_failed", map[string]string{"message": err.Error()})
	}
	if !e.Connected(ctx, wsID) {
		fail(errors.New("the sandbox is not connected"))
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.Cfg.RelayInternalURL+"/internal/v1/workspaces/"+wsID+"/v1/workspace/changes",
		strings.NewReader("{}"))
	if err != nil {
		fail(err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.Signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: wsID}, e.Cfg.RunChangesTimeout))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fail(err)
		return
	}
	defer resp.Body.Close()
	var out struct {
		Files   int    `json:"files"`
		Message string `json:"message"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode >= 300 {
		fail(fmt.Errorf("%s: %s", resp.Status, out.Message))
		return
	}
	e.Services.SetChangesCount(ctx, runID, out.Files)
}

// maskArgs hides the secrets of a run in the arguments of a tool call
// (FTR.HMR.DEV-0004 R10); what stops being JSON after it is kept as a string.
func maskArgs(raw json.RawMessage, secrets []string) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	s := ledger.Mask(string(raw), secrets, math.MaxInt)
	if json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	b, _ := json.Marshal(s)
	return b
}
