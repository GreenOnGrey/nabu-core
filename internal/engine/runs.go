package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
)

// ─── scheduled task runs (tech §9a) ─────────────────────────────────

// HandleTaskRun performs one run of a scheduled task in a separate session of
// the user's personal agent: the same memory, connections and space; only the
// final message goes to the main conversation (TSK-07) and to the task's
// channel (TSK-06).
func (e *Engine) HandleTaskRun(ctx context.Context, _, value []byte) error {
	var m tasks.RunMessage
	if err := json.Unmarshal(value, &m); err != nil {
		return nil
	}
	ri, err := e.Tasks.LoadRun(ctx, m.RunID)
	if err != nil {
		return err
	}
	if ri == nil || ri.Status != "running" {
		return nil
	}
	if e.Tasks.Cancelled(ctx, ri.TaskID) { // TSK-05: a cancelled task does not run
		_, _ = e.Pool.Exec(ctx, `DELETE FROM task_runs WHERE id = $1`, ri.RunID)
		return nil
	}
	u, err := e.Users.Get(ctx, ri.UserID)
	if err != nil || u == nil {
		return err
	}
	if u.Status == "blocked" { // TSK-10
		return e.Tasks.Finish(ctx, ri.RunID, false, "", "user_blocked", "the user is blocked", nil)
	}
	ctx, cancel := context.WithTimeout(ctx, e.Cfg.TaskRunTimeout)
	defer cancel()
	text, class, errText := e.taskTurn(ctx, ri)
	main, err := e.Chat.Main(ctx, u.ID)
	if err != nil {
		return err
	}
	if class != "" {
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
	msg := e.postTaskMessage(ctx, u.ID, main.ID, ri, text)
	var mid *uuid.UUID
	if msg != nil {
		mid = &msg.ID
	}
	return e.Tasks.Finish(ctx, ri.RunID, true, firstLine(text), "", "", mid)
}

// postTaskMessage writes the result to the main conversation with the task
// mark and sends it to the task's channel.
func (e *Engine) postTaskMessage(ctx context.Context, uid, conv uuid.UUID, ri *tasks.RunInfo, text string) *chat.Message {
	m, err := e.Chat.AddAssistant(ctx, conv, "task:"+ri.TaskID.String(), nil, "", nil)
	if err != nil {
		slog.ErrorContext(ctx, "task message", "err", err)
		return nil
	}
	final, err := e.Chat.Finish(ctx, m.ID, text, "done", nil, "", "")
	if err != nil {
		return nil
	}
	e.publish(ctx, events.MessageCreated, uid, final)
	e.publish(ctx, events.MessageDone, uid, final)
	if domain.Messenger(ri.Channel) {
		e.deliver(ctx, uid, ri.Channel, "⏰ "+ri.Title+"\n\n"+text)
	}
	return final
}

func (e *Engine) taskTurn(ctx context.Context, ri *tasks.RunInfo) (string, string, string) {
	user, err := e.Users.Get(ctx, ri.UserID)
	if err != nil || user == nil {
		return "", string(agent.ErrAgentCrashed), "the user is gone"
	}
	if e.Sandboxes != nil && e.Space.Enabled {
		if err := e.Sandboxes.Ensure(ctx, user.ID); err != nil { // R37: the space comes up for the run
			slog.WarnContext(ctx, "sandbox start for a task", "err", err)
		}
	}
	b, err := e.personalRequest(ctx, user, uuid.Nil, &ri.TaskID, "task "+ri.TaskID.String()[:8])
	if err != nil {
		return "", "agent_not_configured", err.Error()
	}
	resp, err := e.open(ctx, b.req)
	if err != nil {
		return "", operatorClass(err), operatorText(err)
	}
	defer e.Op.Close(context.WithoutCancel(ctx), resp.SessionID)
	col := &collector{e: e, ctx: ctx, uid: user.ID, secrets: b.secrets,
		audit: ledger.Entry{AgentKind: "personal", Agent: b.persona.Agent.Name, UserID: &user.ID, Channel: "task:" + ri.TaskID.String()}}
	connID, _ := uuid.Parse(b.model.ConnectionID)
	col.usage = ledger.UsageRow{UserID: &user.ID, Model: b.model.ModelID, ConnectionID: &connID}
	prompt := channelRules(ri.Channel, time.Now(), user.Timezone) + "\n" +
		"[This is a run of the scheduled task «" + ri.Title + "», not a live chat: the user is not here to answer questions. " +
		"Do the work with your tools and reply with the final result for the user only — it will be delivered to the main conversation" +
		deliveryNote(ri.Channel) + ".]\n\nTask instruction: " + ri.Instruction
	err = e.Op.Prompt(ctx, resp.SessionID, agent.PromptRequest{Text: prompt}, col.handle)
	if col.failure != nil {
		return "", string(col.failure.ErrorClass), col.failure.Message
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			_ = e.Op.Abort(context.WithoutCancel(ctx), resp.SessionID)
			return "", string(agent.ErrUnavailable), "the run did not finish within the time limit"
		}
		return "", operatorClass(err), operatorText(err)
	}
	text := strings.TrimSpace(col.text.String())
	if text == "" {
		text = "(the run finished without a message)"
	}
	return text, "", ""
}

func deliveryNote(ch string) string {
	switch ch {
	case domain.ChannelTelegram:
		return " and to Telegram"
	case domain.ChannelVKWS:
		return " and to VK WorkSpace"
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
		Secrets: agent.Secrets{LLMKey: key, MCPHeaders: map[string]map[string]string{}}, SystemAppend: ag.Instructions}
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
	if len(cat.Skills) > 0 {
		var hash string
		for _, h := range cat.SkillSnapshots {
			hash = h
		}
		req.Skills = &agent.Skills{Hash: hash, Names: cat.Skills}
	}
	switch ag.Workspace {
	case services.WorkspaceExternal:
		wsID := services.WorkspaceID(runID)
		// RLY-07: the external workspace must connect within RUN_WORKSPACE_WAIT.
		deadline := time.Now().Add(e.Cfg.RunWorkspaceWait)
		for !e.Connected(ctx, wsID) {
			if time.Now().After(deadline) {
				return "", "workspace_not_connected", "the workspace did not connect to the relay in time", total, false
			}
			select {
			case <-ctx.Done():
				return "", string(agent.ErrUnavailable), "cancelled", total, false
			case <-time.After(time.Second):
			}
		}
		call := e.Signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: wsID}, tokenTTL)
		secrets = append(secrets, call)
		req.Workspace = &agent.Workspace{ID: wsID, URL: e.Cfg.RelayInternalURL + "/internal/v1/workspaces/" + wsID, Token: call,
			Note: "The working directory is the workspace of " + clientName + " for this run (for example a repository checkout). Paths are relative to its root."}
	case services.WorkspaceNabu:
		if e.Sandboxes == nil || e.Sandboxes.K8s == nil {
			return "", "workspace_not_connected", "temporary sandboxes are not enabled in this deployment", total, false
		}
		wsID := services.WorkspaceID(runID)
		if err := e.Sandboxes.StartEphemeral(ctx, wsID); err != nil {
			return "", "workspace_not_connected", err.Error(), total, false
		}
		defer e.Sandboxes.StopEphemeral(context.WithoutCancel(ctx), wsID)
		call := e.Signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: wsID}, tokenTTL)
		secrets = append(secrets, call)
		req.Workspace = &agent.Workspace{ID: wsID, URL: e.Cfg.RelayInternalURL + "/internal/v1/workspaces/" + wsID, Token: call,
			Note: "The working directory is a temporary sandbox of this run; its files are deleted after the run."}
	}
	resp, err := e.open(ctx, req)
	if err != nil {
		return "", operatorClass(err), operatorText(err), total, false
	}
	defer e.Op.Close(context.WithoutCancel(ctx), resp.SessionID)
	var cid = clientID
	col := &collector{e: e, ctx: ctx, secrets: secrets,
		audit: ledger.Entry{AgentKind: "service", Agent: ag.Name, ClientID: &cid, InitiatorEmail: initiator, Channel: "client:" + clientName}}
	connID, _ := uuid.Parse(model.ConnectionID)
	col.usage = ledger.UsageRow{RunID: &runID, ClientID: &cid, Agent: ag.Name, Model: model.ModelID, ConnectionID: &connID}
	limitHit := false
	col.onEvent = func(ev agent.Event) {
		switch ev.Type {
		case agent.EventTextDelta:
			e.Services.AppendEvent(ctx, runID, "text_delta", map[string]string{"delta": ev.Delta})
		case agent.EventToolCall:
			e.Services.AppendEvent(ctx, runID, "tool_call", map[string]any{"id": ev.ID, "name": ev.Name, "args": ev.Args})
		case agent.EventToolResult:
			e.Services.AppendEvent(ctx, runID, "tool_result", map[string]any{"id": ev.ID, "name": ev.Name, "isError": ev.IsError, "summary": clip(ev.Summary, 2000)})
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
	// A cancellation by the client aborts the run.
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
					cancelled = true
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
	err = e.Op.Prompt(ctx, resp.SessionID, agent.PromptRequest{Text: prompt}, col.handle)
	e.Models.RecordResult(ctx, model.ConnectionID, func() agent.ErrorClass {
		if col.failure != nil {
			return col.failure.ErrorClass
		}
		return ""
	}())
	summary = strings.TrimSpace(col.text.String())
	total = col.total
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
