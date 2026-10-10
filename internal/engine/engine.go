package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/agentpods"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/groups"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/space"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// Operator is the client of an agent operator: the pool of service agents
// or the pod of an owner.
type Operator = agentpods.Operator

// Config are the settings of the engine.
type Config struct {
	InternalURL      string // the built-in MCP and the MCP proxy
	RelayInternalURL string // the agent's calls to workspaces
	TurnTimeout      time.Duration
	TaskRunTimeout   time.Duration
	RunWorkspaceWait time.Duration
	SessionMaxAge    time.Duration // reopen sessions older than this (tokens)
	PiVersion        string
}

// Engine runs agents.
type Engine struct {
	Cfg  Config
	Pool *pgxpool.Pool
	Op   Operator
	// Pods gives the pod of an owner for a turn of a conversation or a task
	// (FTR.NAB.CMN-0004); nil runs everything in Op.
	Pods      *agentpods.Manager
	Users     *users.Repo
	Chat      *chat.Store
	Models    *models.Service
	Catalog   *catalog.Service
	Memory    *memory.Service
	Tasks     *tasks.Service
	Space     *space.Service
	Sandboxes *space.Sandboxes
	Services  *services.Service
	Ledger    *ledger.Ledger
	Signer    *jwt.Signer
	S3        storage.Storage
	Events    events.Publisher
	Bus       kafka.Publisher
	Adapters  map[string]channels.Adapter
	// FTR.NAB.CMN-0002: availability of channels, Telegram bindings, group
	// agents and the mail channel.
	Registry *channels.Registry
	Keys     *channels.Keys
	Groups   *groups.Service
	Mail     Mailer
	// Connected reports whether an external workspace is connected to the relay.
	Connected func(ctx context.Context, workspaceID string) bool

	locks sync.Map // conversation → *sync.Mutex
}

const tokenTTL = 24 * time.Hour

func (e *Engine) publish(ctx context.Context, typ string, uid uuid.UUID, data any) {
	e.Events.Publish(ctx, events.Event{Type: typ, UserID: &uid, Data: data})
}

func (e *Engine) lock(conv uuid.UUID) func() {
	m, _ := e.locks.LoadOrStore(conv, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// ─── session building ───────────────────────────────────────────────

type built struct {
	req     agent.SessionRequest
	persona Persona
	secrets []string
	model   agent.ModelSpec
	// group is set in the session of a group agent.
	group *groups.Agent
}

// personalRequest builds the session of a personal agent: the model, the
// built-in MCP, the connected catalog items, skills and the personal space.
func (e *Engine) personalRequest(ctx context.Context, u *users.User, conv uuid.UUID, taskID *uuid.UUID, label string) (*built, error) {
	if ga := e.group(ctx, u); ga != nil {
		return e.groupRequest(ctx, u, ga, conv, label)
	}
	ag, err := e.Users.GetAgent(ctx, u.ID)
	if err != nil {
		return nil, err
	}
	model, key, err := e.Models.ResolvePersonal(ctx, ag.ConnectionID, ag.Model)
	if err != nil {
		return nil, err
	}
	mem, err := e.Memory.ForInstructions(ctx, u.ID)
	if err != nil {
		return nil, err
	}
	convStr := ""
	if conv != uuid.Nil {
		convStr = conv.String()
	}
	cat, err := e.Catalog.ForSession(ctx, u.ID, "", convStr, tokenTTL)
	if err != nil {
		return nil, err
	}
	claims := jwt.Claims{Audience: jwt.AudMCP, Subject: "builtin", User: u.ID.String(), Conversation: convStr}
	if taskID != nil {
		claims.Task = taskID.String()
	}
	builtinTok := e.Signer.Issue(claims, tokenTTL)
	req := agent.SessionRequest{Kind: agent.KindChat, Model: model, Label: label,
		Secrets: agent.Secrets{LLMKey: key, MCPHeaders: map[string]map[string]string{"nabu": {"Authorization": "Bearer " + builtinTok}}},
		MCP: []agent.MCPServer{{Name: "nabu", URL: e.Cfg.InternalURL + "/internal/v1/mcp", HeaderNames: []string{"Authorization"}, Exposure: "direct",
			Description: "Nabu: the user's memory, personal space and scheduled tasks"}}}
	secrets := []string{key, builtinTok}
	var titles []string
	for _, s := range cat.Servers {
		req.MCP = append(req.MCP, s)
		req.Secrets.MCPHeaders[s.Name] = cat.Headers[s.Name]
		secrets = append(secrets, strings.TrimPrefix(cat.Headers[s.Name]["Authorization"], "Bearer "))
		titles = append(titles, s.Description)
	}
	if len(cat.Skills) > 0 {
		var hash string
		for _, h := range cat.SkillSnapshots {
			hash = h // one skills source per session (the latest)
		}
		req.Skills = &agent.Skills{Hash: hash, Names: cat.Skills}
	}
	if e.Space.Enabled {
		wsID := space.WorkspaceID(u.ID)
		call := e.Signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: wsID}, tokenTTL)
		secrets = append(secrets, call)
		req.Workspace = &agent.Workspace{ID: wsID, URL: e.Cfg.RelayInternalURL + "/internal/v1/workspaces/" + wsID, Token: call,
			Note: "The working directory is the user's personal space in Nabu: files persist between conversations and the user sees them in the Space section. Paths are relative to its root."}
	}
	p := Persona{User: u, Agent: ag, Memory: mem, Spaces: e.Space.Enabled, Now: time.Now(), Catalog: titles}
	req.SystemAppend = p.Instructions()
	return &built{req: req, persona: p, secrets: secrets, model: model}, nil
}

func (e *Engine) bundle(hash string) func(ctx context.Context) ([]byte, error) {
	return func(ctx context.Context) ([]byte, error) { return e.Catalog.Bundle(ctx, hash) }
}

func (e *Engine) open(ctx context.Context, op Operator, req agent.SessionRequest) (agent.SessionResponse, error) {
	var b func(ctx context.Context) ([]byte, error)
	if req.Skills != nil && req.Skills.Hash != "" {
		b = e.bundle(req.Skills.Hash)
	}
	return op.Open(ctx, req, b)
}

// acquire gets the pod of the owner for one turn; agentpods.ErrQueued means
// the turn waits and comes back as the same Kafka event.
func (e *Engine) acquire(ctx context.Context, owner uuid.UUID, t agentpods.Turn) (*agentpods.Lease, error) {
	if e.Pods == nil {
		return agentpods.Local(e.Op), nil
	}
	return e.Pods.Acquire(ctx, owner, t)
}

// ─── conversation sessions ──────────────────────────────────────────

type convSession struct {
	ID          uuid.UUID
	OpSession   string
	PersonaHash string
	Model       string
	OpenedAt    time.Time
	Generation  int64
	// Fresh: the session was opened for this turn (the first-token metric).
	Fresh bool
}

func snapshotKey(conv uuid.UUID) string { return "sessions/" + conv.String() + ".jsonl" }

func (e *Engine) loadSession(ctx context.Context, conv uuid.UUID) (*convSession, error) {
	var s convSession
	var model *string
	err := e.Pool.QueryRow(ctx, `SELECT id, COALESCE(operator_session,''), COALESCE(persona_hash,''), model, last_active_at, COALESCE(pod_generation, 0)
		FROM harness_sessions WHERE conversation_id = $1 AND closed_at IS NULL`, conv).Scan(&s.ID, &s.OpSession, &s.PersonaHash, &model, &s.OpenedAt, &s.Generation)
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	if model != nil {
		s.Model = *model
	}
	return &s, err
}

// ensureSession opens the harness session of a conversation, restoring it
// from the S3 snapshot (CONV-07/08) or seeding it with the recent history.
func (e *Engine) ensureSession(ctx context.Context, lease *agentpods.Lease, conv uuid.UUID, b *built, fresh bool) (*convSession, error) {
	op := lease.Operator()
	cur, err := e.loadSession(ctx, conv)
	if err != nil {
		return nil, err
	}
	// LC-06: a session of a previous start of the pod went with that pod.
	gone := cur != nil && cur.Generation != lease.Generation()
	if cur != nil && !fresh && !gone && cur.OpSession != "" && time.Since(cur.OpenedAt) < e.Cfg.SessionMaxAge {
		return cur, nil
	}
	if cur != nil {
		if cur.OpSession != "" && !gone {
			_ = op.Close(ctx, cur.OpSession)
		}
		_, _ = e.Pool.Exec(ctx, `UPDATE harness_sessions SET closed_at = now() WHERE id = $1`, cur.ID)
	}
	req := b.req
	if snap, err := storage.ReadAll(ctx, e.S3, snapshotKey(conv)); err == nil && len(snap) > 0 {
		req.Snapshot = snap
	} else {
		hist, _ := e.Chat.History(ctx, conv, 30)
		for _, m := range hist {
			if m.Status == "done" && strings.TrimSpace(m.Text) != "" {
				req.History = append(req.History, agent.HistoryMessage{Role: m.Role, Text: m.Text})
			}
		}
	}
	resp, err := e.open(ctx, op, req)
	if err != nil && req.Snapshot != nil {
		// a snapshot Pi cannot read: start over with the history
		slog.WarnContext(ctx, "restore from snapshot failed, seeding with history", "conversation", conv, "err", err)
		req.Snapshot = nil
		hist, _ := e.Chat.History(ctx, conv, 30)
		for _, m := range hist {
			if m.Status == "done" && strings.TrimSpace(m.Text) != "" {
				req.History = append(req.History, agent.HistoryMessage{Role: m.Role, Text: m.Text})
			}
		}
		resp, err = e.open(ctx, op, req)
	}
	if err != nil {
		return nil, err
	}
	s := &convSession{OpSession: resp.SessionID, PersonaHash: b.persona.Hash(), Model: b.model.ConnectionID + "/" + b.model.ModelID + "/" + b.model.Thinking,
		OpenedAt: time.Now(), Generation: lease.Generation(), Fresh: true}
	const ins = `INSERT INTO harness_sessions (conversation_id, harness, operator_session, persona_hash, model, snapshot_key, pod_generation)
		VALUES ($1,'pi',$2,$3,$4,$5,$6) RETURNING id`
	err = e.Pool.QueryRow(ctx, ins, conv, s.OpSession, s.PersonaHash, s.Model, snapshotKey(conv), s.Generation).Scan(&s.ID)
	if postgres.IsUniqueViolation(err) {
		_, _ = e.Pool.Exec(ctx, `UPDATE harness_sessions SET closed_at = now() WHERE conversation_id = $1 AND closed_at IS NULL`, conv)
		err = e.Pool.QueryRow(ctx, ins, conv, s.OpSession, s.PersonaHash, s.Model, snapshotKey(conv), s.Generation).Scan(&s.ID)
	}
	return s, err
}

func (e *Engine) saveSnapshot(ctx context.Context, op Operator, conv uuid.UUID, opSession string) {
	snap, err := op.Snapshot(ctx, opSession)
	if err != nil || len(snap) == 0 {
		return
	}
	if err := e.S3.Put(ctx, snapshotKey(conv), bytes.NewReader(snap), int64(len(snap)), "application/x-ndjson"); err != nil {
		slog.WarnContext(ctx, "save session snapshot", "conversation", conv, "err", err)
	}
	_, _ = e.Pool.Exec(ctx, `UPDATE harness_sessions SET last_active_at = now() WHERE conversation_id = $1 AND closed_at IS NULL`, conv)
}

// ─── the stream of a turn ───────────────────────────────────────────

// collector turns the operator's events into the answer, tool steps,
// usage and audit.
type collector struct {
	e        *Engine
	ctx      context.Context
	uid      uuid.UUID
	msgID    uuid.UUID
	conv     uuid.UUID
	stream   bool // publish deltas to the user
	audit    ledger.Entry
	secrets  []string
	usage    ledger.UsageRow
	text     strings.Builder
	pending  strings.Builder
	lastPush time.Time
	steps    []chat.ToolStep
	args     map[string]string
	failure  *agent.Event
	total    agent.Usage
	onEvent  func(agent.Event)
	// first is called once, with the first piece of the answer.
	first func()
}

func stepTitle(server, tool string) string {
	if server == "" {
		return tool
	}
	return server + ": " + tool
}

func (c *collector) flush() {
	if c.pending.Len() == 0 || !c.stream {
		return
	}
	c.e.publish(c.ctx, events.MessageDelta, c.uid, map[string]any{"messageId": c.msgID, "conversationId": c.conv, "delta": c.pending.String()})
	c.pending.Reset()
	c.lastPush = time.Now()
}

func (c *collector) handle(ev agent.Event) {
	if c.onEvent != nil {
		c.onEvent(ev)
	}
	if c.first != nil && (ev.Type == agent.EventTextDelta || ev.Type == agent.EventThinkingDelta) {
		c.first()
		c.first = nil
	}
	switch ev.Type {
	case agent.EventTextDelta:
		c.text.WriteString(ev.Delta)
		c.pending.WriteString(ev.Delta)
		if time.Since(c.lastPush) > 120*time.Millisecond {
			c.flush()
		}
	case agent.EventToolCall:
		c.flush()
		server, tool := ledger.SplitTool(ev.Name)
		if c.args == nil {
			c.args = map[string]string{}
		}
		c.args[ev.ID] = string(ev.Args)
		st := chat.ToolStep{ID: ev.ID, Server: server, Tool: tool, Summary: stepTitle(server, tool), Status: "running"}
		c.steps = append(c.steps, st)
		if c.stream {
			c.e.publish(c.ctx, events.ToolStep, c.uid, map[string]any{"messageId": c.msgID, "conversationId": c.conv, "step": st})
		}
	case agent.EventToolResult:
		server, tool := ledger.SplitTool(ev.Name)
		status, result := "done", "ok"
		var errText *string
		if ev.IsError {
			status, result = "error", "error"
			t := ev.Summary
			errText = &t
		}
		for i := range c.steps {
			if c.steps[i].ID == ev.ID {
				c.steps[i].Status = status
				if s := firstLine(ev.Summary); s != "" {
					c.steps[i].Summary = stepTitle(server, tool) + " — " + clip(s, 140)
				}
				if c.stream {
					c.e.publish(c.ctx, events.ToolStep, c.uid, map[string]any{"messageId": c.msgID, "conversationId": c.conv, "step": c.steps[i]})
				}
			}
		}
		a := c.audit
		a.Tool, a.Result, a.Error = tool, result, errText
		if server != "" {
			a.Server = &server
		}
		c.e.Ledger.Audit(c.ctx, a, c.args[ev.ID], c.secrets) // AUD-01
	case agent.EventUsage:
		c.total.Add(ev.Usage)
		u := c.usage
		u.Usage = ev.Usage
		c.e.Ledger.Usage(c.ctx, u)
	case agent.EventError:
		ev := ev
		c.failure = &ev
	}
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// ─── personal turns ─────────────────────────────────────────────────

// HandleInbound answers one user message (a Kafka handler of nabu.inbound).
func (e *Engine) HandleInbound(ctx context.Context, key, value []byte) error {
	var in chat.Inbound
	if err := json.Unmarshal(value, &in); err != nil {
		return nil // malformed: drop
	}
	unlock := e.lock(in.ConversationID)
	defer unlock()
	u, err := e.Users.Get(ctx, in.UserID)
	if err != nil {
		return err
	}
	if u == nil {
		return nil
	}
	turn := agentpods.Turn{Kind: agentpods.KindMessage, Ref: "m:" + in.MessageID.String(), Topic: kafka.TopicInbound, Key: string(key),
		Value: value, ConversationID: in.ConversationID, MessageID: in.MessageID, Channel: in.Channel}
	if in.RetryOf != nil {
		turn.Ref += ":r:" + in.RetryOf.String()
	}
	userMsg, err := e.Chat.Message(ctx, in.UserID, in.MessageID)
	if err != nil {
		e.Pods.Drop(ctx, u.ID, turn.Ref) // nobody will run it: the later turns of the owner go on
		return nil
	}
	// At-least-once delivery: an answered message is not answered again. An
	// answer left unfinished by a turn that lost its pod does not count: the
	// turn waits in the queue to continue it.
	if in.RetryOf == nil {
		var n int
		_ = e.Pool.QueryRow(ctx, `SELECT count(*) FROM messages m WHERE m.reply_to = $1 AND m.role = 'assistant'
			AND NOT EXISTS (SELECT 1 FROM agent_queue q WHERE q.resume = m.id)`, userMsg.ID).Scan(&n)
		if n > 0 {
			e.Pods.Drop(ctx, u.ID, turn.Ref)
			return nil
		}
	}
	// The pod of the owner (FTR.NAB.CMN-0004): without one the turn waits in
	// the queue and this event comes back when the pod is ready.
	lease := agentpods.Local(e.Op)
	if u.Status != "blocked" && u.Status != "archived" {
		lease, err = e.acquire(ctx, u.ID, turn)
		if errors.Is(err, agentpods.ErrQueued) {
			return nil
		}
		if err != nil {
			return err
		}
		defer lease.Release(ctx)
	}
	// R17: the time to the first piece of the answer counts from the message,
	// for a retry — from the retry.
	asked := userMsg.CreatedAt
	if in.RetryOf != nil {
		asked = time.Now()
	}
	ctx, cancel := context.WithTimeout(ctx, e.Cfg.TurnTimeout)
	defer cancel()
	var asst *chat.Message
	if lease.Resume != uuid.Nil {
		asst, _ = e.Chat.Message(ctx, in.UserID, lease.Resume) // R5: the answer the lost pod did not begin
	}
	if asst == nil {
		if asst, err = e.Chat.AddAssistant(ctx, in.ConversationID, in.Channel, &userMsg.ID, "", in.RetryOf); err != nil {
			return err
		}
		e.publish(ctx, events.MessageCreated, u.ID, asst)
	}
	if u.Status == "blocked" || u.Status == "archived" {
		e.fail(ctx, u, asst.ID, in.Channel, "user_blocked", "Access to the agent is closed for this user.", nil)
		return nil
	}
	started := time.Now()
	mctx := parseContext(in.Context)
	var draft *drafter
	if domain.Messenger(in.Channel) {
		if ad := e.Adapters[in.Channel]; ad != nil {
			if chatID, ok := e.chatOf(ctx, u, in.Channel); ok {
				if in.Channel == domain.ChannelTelegram && u.CreatedVia != "group" {
					draft = newDrafter(ad, chatID) // R14: private chats show the answer as it is written
				}
				tctx, stop := context.WithCancel(ctx)
				defer stop()
				go func() {
					t := time.NewTicker(4 * time.Second) // tech §7: typing every 4 seconds until the answer
					defer t.Stop()
					for {
						ad.Typing(tctx, chatID)
						select {
						case <-tctx.Done():
							return
						case <-t.C:
						}
					}
				}()
			}
		}
	}
	text, images := e.promptOf(ctx, u, userMsg, in)
	var author *uuid.UUID
	if mctx.Group != nil && mctx.Group.AuthorID != uuid.Nil {
		author = &mctx.Group.AuthorID
	}
	res := e.personalTurn(ctx, lease, asked, u, in.ConversationID, asst.ID, in.Channel, in.ClientID, text, images, author, draft)
	if res.podLost {
		// R5, LC-04: the pod went away before the answer began — the turn goes
		// back to the queue and continues this answer in a new pod.
		if err := e.Pods.Requeue(ctx, lease, turn, asst.ID); err == nil {
			return nil
		}
		slog.WarnContext(ctx, "requeue a turn", "conversation", in.ConversationID, "err", err)
	}
	status := "done"
	if res.errClass != "" {
		status = "failed"
	}
	final, err := e.Chat.Finish(ctx, asst.ID, res.text, status, res.steps, res.errClass, res.errText)
	if err != nil {
		return err
	}
	final.Retryable = res.errClass != ""
	if res.errClass != "" {
		metrics.Turns.WithLabelValues(channelLabel(in.Channel), "failed").Inc()
		e.publish(ctx, events.ChatError, u.ID, map[string]any{"messageId": asst.ID, "conversationId": in.ConversationID,
			"errorClass": res.errClass, "retryable": retryable(res.errClass), "text": res.errText})
	} else {
		metrics.Turns.WithLabelValues(channelLabel(in.Channel), "ok").Inc()
	}
	e.publish(ctx, events.MessageDone, u.ID, final)
	switch {
	case e.Chat.Source(ctx, in.ConversationID) == "email":
		// R8: a letter in the thread when the bot was the only recipient,
		// otherwise the topic with an unread mark; a failure stays in the topic.
		if e.Mail != nil && res.errClass == "" {
			if err := e.Mail.Reply(context.WithoutCancel(ctx), u.ID, in.ConversationID, res.text, started); err != nil {
				slog.WarnContext(ctx, "mail reply", "conversation", in.ConversationID, "err", err)
			}
		}
	case domain.Messenger(in.Channel):
		out := res.text
		if res.errClass != "" {
			out = errorText(u.Language, res.errClass)
		} else if ga := e.group(ctx, u); ga != nil {
			out = "**" + ga.Name + " " + groupMark(u.Language) + "**\n\n" + out // design §5 #4: the group agent signs its answers
		}
		e.deliver(ctx, u.ID, in.Channel, out)
	}
	return nil
}

func groupMark(lang string) string {
	if lang == "ru" {
		return "(групповой)"
	}
	return "(group)"
}

func channelLabel(ch string) string {
	if i := strings.IndexByte(ch, ':'); i > 0 {
		return ch[:i]
	}
	return ch
}

// retryable: every failed answer keeps «Retry» (R30); after an
// administrator fixes the connection the retry succeeds (CONV-09).
func retryable(string) bool { return true }

func (e *Engine) fail(ctx context.Context, u *users.User, msgID uuid.UUID, ch, class, text string, steps []chat.ToolStep) {
	final, err := e.Chat.Finish(ctx, msgID, "", "failed", steps, class, text)
	if err != nil {
		return
	}
	e.publish(ctx, events.ChatError, u.ID, map[string]any{"messageId": msgID, "conversationId": final.ConversationID, "errorClass": class, "retryable": false, "text": text})
	e.publish(ctx, events.MessageDone, u.ID, final)
	if domain.Messenger(ch) {
		e.deliver(ctx, u.ID, ch, errorText(u.Language, class))
	}
}

// promptOf builds the prompt: the channel rules, the context of a client's
// screen (into the instructions of the turn, not into the stored text: R22),
// the text and the attachments (images inline; files copied to the space).
func (e *Engine) promptOf(ctx context.Context, u *users.User, m *chat.Message, in chat.Inbound) (string, []agent.Image) {
	var b strings.Builder
	b.WriteString(channelRules(in.Channel, time.Now(), u.Timezone))
	b.WriteByte('\n')
	mc := parseContext(in.Context)
	if len(in.Context) > 0 && mc.Email == nil && mc.Group == nil && mc.Confirmation == nil {
		b.WriteString("[Context of the user's current screen in " + strings.TrimPrefix(in.Channel, "client:") + ": " + clip(string(in.Context), 8000) + "]\n")
	}
	var images []agent.Image
	for _, a := range m.Attachments {
		var key string
		if err := e.Pool.QueryRow(ctx, `SELECT s3_key FROM attachments WHERE id = $1`, a.ID).Scan(&key); err != nil {
			continue
		}
		if strings.HasPrefix(a.MimeType, "image/") && a.Size <= 5<<20 {
			if data, err := storage.ReadAll(ctx, e.S3, key); err == nil {
				images = append(images, agent.Image{MimeType: a.MimeType, Data: base64.StdEncoding.EncodeToString(data)})
			}
		}
		if e.Space.Enabled {
			rc, err := e.S3.Get(ctx, key)
			if err == nil {
				p := "attachments/" + a.FileName
				_, perr := e.Space.Put(ctx, u.ID, p, rc, a.Size, "user") // R10: attachments land in the space
				rc.Close()
				if perr == nil {
					fmt.Fprintf(&b, "[The user attached %s (%s, %d bytes); it is in your space at %s.]\n", a.FileName, a.MimeType, a.Size, p)
					continue
				}
			}
		}
		if strings.HasPrefix(a.MimeType, "text/") && a.Size <= 200<<10 {
			if data, err := storage.ReadAll(ctx, e.S3, key); err == nil {
				fmt.Fprintf(&b, "[Attached file %s]\n%s\n", a.FileName, clip(string(data), 50000))
				continue
			}
		}
		fmt.Fprintf(&b, "[The user attached %s (%s, %d bytes).]\n", a.FileName, a.MimeType, a.Size)
	}
	switch {
	case mc.Email != nil:
		b.WriteString(emailPrompt(mc, m.Text))
	case mc.Group != nil:
		b.WriteString(groupPrompt(mc, m.Text))
	default:
		b.WriteString(m.Text)
	}
	return b.String(), images
}

type turnResult struct {
	text, errClass, errText string
	steps                   []chat.ToolStep
	// podLost: the pod of the owner went away before the answer began; the
	// caller may put the turn back into the queue.
	podLost bool
}

// podLost reports a failure of the pod itself, not of the turn: the operator
// is not there, or the address belongs to another pod now.
func (e *Engine) podLost(lease *agentpods.Lease, err error) bool {
	if e.Pods == nil || lease.Generation() == 0 || lease.Attempts > 0 || err == nil {
		return false // no pods of owners, or the turn lost a pod already
	}
	var api *agent.APIError
	var busy *agent.BusyError
	if errors.As(err, &api) || errors.As(err, &busy) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

func (e *Engine) personalTurn(ctx context.Context, lease *agentpods.Lease, asked time.Time, u *users.User, conv, msgID uuid.UUID, ch string, clientID *uuid.UUID, text string, images []agent.Image,
	author *uuid.UUID, draft *drafter) turnResult {
	b, err := e.personalRequest(ctx, u, conv, nil, "conv "+conv.String()[:8])
	if err != nil {
		var class = "agent_not_configured"
		if !strings.Contains(err.Error(), "agent_not_configured") {
			class = string(agent.ErrAgentCrashed)
			slog.ErrorContext(ctx, "build session", "err", err)
		}
		return turnResult{errClass: class, errText: err.Error()}
	}
	if e.Sandboxes != nil && e.Space.Enabled {
		go func() {
			if err := e.Sandboxes.Ensure(context.WithoutCancel(ctx), u.ID); err != nil {
				slog.Warn("sandbox start", "user_id", u.ID, "err", err)
			}
		}()
	}
	col := &collector{e: e, ctx: ctx, uid: u.ID, msgID: msgID, conv: conv, stream: true, secrets: b.secrets,
		audit: ledger.Entry{AgentKind: "personal", Agent: b.persona.Agent.Name, UserID: &u.ID, ClientID: clientID, Channel: ch}}
	connID, _ := uuid.Parse(b.model.ConnectionID)
	col.usage = ledger.UsageRow{UserID: &u.ID, ClientID: clientID, Agent: "", Model: b.model.ModelID, ConnectionID: &connID}
	if b.group != nil {
		// GR-08: the cost and the audit of a group agent go to the group; the audit names the author
		col.audit.Agent, col.audit.UserID, col.audit.GroupAgentID = b.group.Name+" (group)", author, &b.group.ID
		col.usage = ledger.UsageRow{GroupAgentID: &b.group.ID, Agent: "group agent", Model: b.model.ModelID, ConnectionID: &connID}
	}
	if draft != nil {
		col.onEvent = func(ev agent.Event) { draft.event(ctx, ev) }
	}
	op := lease.Operator()
	var sess *convSession
	for attempt := 0; attempt < 2; attempt++ {
		sess, err = e.ensureSession(ctx, lease, conv, b, attempt > 0)
		if err != nil {
			return turnResult{errClass: operatorClass(err), errText: operatorText(err), podLost: e.podLost(lease, err)}
		}
		start := "hot"
		switch {
		case lease.Start != "":
			start = lease.Start // the turn waited for its pod
		case sess.Fresh:
			start = "warm"
		}
		col.first = func() { metrics.TurnFirstToken.WithLabelValues(start).Observe(time.Since(asked).Seconds()) }
		prompt := text
		if sess.PersonaHash != b.persona.Hash() {
			// MEM-02, CONV-01: the open session gets the new profile with this message.
			prompt = b.persona.Update() + "\n\n" + prompt
			_, _ = e.Pool.Exec(ctx, `UPDATE harness_sessions SET persona_hash = $2 WHERE id = $1`, sess.ID, b.persona.Hash())
		}
		if want := b.model.ConnectionID + "/" + b.model.ModelID + "/" + b.model.Thinking; sess.Model != want {
			if strings.SplitN(sess.Model, "/", 2)[0] == b.model.ConnectionID {
				if err := op.Patch(ctx, sess.OpSession, agent.PatchRequest{ModelID: b.model.ModelID, Thinking: b.model.Thinking}); err == nil {
					_, _ = e.Pool.Exec(ctx, `UPDATE harness_sessions SET model = $2 WHERE id = $1`, sess.ID, want)
				}
			} else if attempt == 0 {
				continue // another connection: a new session (from the snapshot)
			}
		}
		err = op.Prompt(ctx, sess.OpSession, agent.PromptRequest{Text: prompt, Images: images}, col.handle)
		if errors.Is(err, agent.ErrSessionGone) || errors.Is(err, agent.ErrStreamBroken) {
			_, _ = e.Pool.Exec(ctx, `UPDATE harness_sessions SET closed_at = now() WHERE id = $1`, sess.ID)
			if col.text.Len() == 0 && len(col.steps) == 0 {
				continue // CONV-08: the session moves to another pod from the snapshot
			}
		}
		break
	}
	col.flush()
	if err == nil && sess != nil {
		e.saveSnapshot(context.WithoutCancel(ctx), op, conv, sess.OpSession)
	}
	e.Models.RecordResult(ctx, b.model.ConnectionID, func() agent.ErrorClass {
		if col.failure != nil {
			return col.failure.ErrorClass
		}
		return ""
	}())
	res := turnResult{text: col.text.String(), steps: col.steps}
	res.podLost = col.failure == nil && col.text.Len() == 0 && len(col.steps) == 0 && e.podLost(lease, err)
	switch {
	case col.failure != nil:
		res.errClass, res.errText = string(col.failure.ErrorClass), col.failure.Message
	case err != nil:
		res.errClass, res.errText = operatorClass(err), operatorText(err)
	}
	return res
}

func operatorClass(err error) string {
	var busy *agent.BusyError
	switch {
	case errors.As(err, &busy):
		return "agent_busy"
	case strings.Contains(err.Error(), "agent_not_configured"):
		return "agent_not_configured"
	case errors.Is(err, context.DeadlineExceeded):
		return string(agent.ErrUnavailable)
	}
	return string(agent.ErrAgentCrashed)
}

func operatorText(err error) string {
	var busy *agent.BusyError
	if errors.As(err, &busy) {
		return "The agent is busy, try again in a minute."
	}
	return err.Error()
}

// ─── delivery ───────────────────────────────────────────────────────

// Outbound is the payload of nabu.outbound.
type Outbound struct {
	UserID  uuid.UUID `json:"userId"`
	Channel string    `json:"channel"`
	Text    string    `json:"text"`
}

func (e *Engine) deliver(ctx context.Context, uid uuid.UUID, ch, text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	b, _ := json.Marshal(Outbound{UserID: uid, Channel: ch, Text: text})
	if err := e.Bus.Publish(ctx, kafka.TopicOutbound, uid.String(), b); err != nil {
		slog.ErrorContext(ctx, "publish outbound", "err", err)
	}
}

// HandleOutbound sends an answer to the messenger (a Kafka handler).
func (e *Engine) HandleOutbound(ctx context.Context, _, value []byte) error {
	var o Outbound
	if err := json.Unmarshal(value, &o); err != nil {
		return nil
	}
	ad := e.Adapters[o.Channel]
	if ad == nil {
		return nil
	}
	u, err := e.Users.Get(ctx, o.UserID)
	if err != nil || u == nil {
		return err
	}
	// arch §2.2: the availability is checked when the answer is delivered too
	if u.CreatedVia != "group" && e.Registry != nil && !e.Registry.Open(ctx, o.UserID, o.Channel) {
		metrics.ChannelOutbound.WithLabelValues(o.Channel, "unavailable").Inc()
		return nil
	}
	chatID, ok := e.chatOf(ctx, u, o.Channel)
	if !ok {
		return nil
	}
	if err := ad.Send(ctx, chatID, o.Text); err != nil {
		var refusal interface{ Permanent() bool }
		if errors.As(err, &refusal) && refusal.Permanent() {
			slog.WarnContext(ctx, "channel delivery", "channel", o.Channel, "err", err)
			return nil
		}
		return err // a transient failure: the consumer retries the delivery
	}
	return nil
}

var errorTexts = map[string]map[string]string{
	"en": {
		"insufficient_balance": "The model provider has no balance left. An administrator needs to top it up.",
		"auth":                 "The model connection was rejected. An administrator needs to check its key.",
		"rate_limit":           "The model is overloaded. Please try again in a minute.",
		"unavailable":          "The model is temporarily unavailable. Please try again later.",
		"bad_request":          "The model rejected the request.",
		"context_overflow":     "The conversation became too long for the model. Start a new topic on the site.",
		"agent_crashed":        "The agent stopped unexpectedly. Please try again.",
		"agent_busy":           "The agent is busy. Please try again in a minute.",
		"queue_busy":           "The agent is busy, I will answer as soon as a place is free.",
		"agent_not_configured": "The agent is not configured yet: an administrator has to add a model connection.",
		"user_blocked":         "Access to the agent is closed for your account.",
	},
	"ru": {
		"insufficient_balance": "У провайдера моделей закончился баланс. Его должен пополнить администратор.",
		"auth":                 "Подключение к модели отклонено. Администратору нужно проверить ключ.",
		"rate_limit":           "Модель перегружена. Повторите через минуту.",
		"unavailable":          "Модель временно недоступна. Повторите позже.",
		"bad_request":          "Модель отклонила запрос.",
		"context_overflow":     "Разговор стал слишком длинным для модели. Начните новую тему на сайте.",
		"agent_crashed":        "Агент неожиданно остановился. Повторите запрос.",
		"agent_busy":           "Агент занят. Повторите через минуту.",
		"queue_busy":           "Агент занят, отвечу, как только освободится место.",
		"agent_not_configured": "Агент ещё не настроен: администратор должен добавить подключение к моделям.",
		"user_blocked":         "Доступ к агенту для вашей учётной записи закрыт.",
	},
}

func errorText(lang, class string) string {
	m, ok := errorTexts[lang]
	if !ok {
		m = errorTexts["en"]
	}
	if t, ok := m[class]; ok {
		return t
	}
	return m["agent_crashed"]
}
