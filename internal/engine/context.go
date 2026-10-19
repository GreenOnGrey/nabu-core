package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/agentpods"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// The context of a conversation (FTR.NAB.CHAT-0001 R1–R3, R17, arch §3.1–3.2):
// a reset closes the session of the conversation without a snapshot, a
// compaction runs in the session, and both leave an event in the conversation.

// autoCompactShare: a context fuller than this share of the window of the
// model of the turn is compacted before the turn (R17).
const autoCompactShare = 0.8

// handleCommand performs a command of the conversation taken from the queue
// of its turns (the caller holds the lock of the conversation).
func (e *Engine) handleCommand(ctx context.Context, key, value []byte, in chat.Inbound, u *users.User) error {
	switch in.Command {
	case chat.CommandReset:
		e.reset(ctx, u, in)
		return nil
	case chat.CommandCompact:
		return e.compactCommand(ctx, key, value, in, u)
	}
	return nil
}

// reset closes the session of the conversation and forgets its snapshot; the
// next message opens a session seeded with the messages after the reset only.
func (e *Engine) reset(ctx context.Context, u *users.User, in chat.Inbound) {
	if cur, err := e.loadSession(ctx, in.ConversationID); err == nil && cur != nil {
		if cur.OpSession != "" && e.Pods != nil {
			if op, ok := e.Pods.Peek(ctx, u.ID); ok {
				_ = op.Close(ctx, cur.OpSession)
			}
		} else if cur.OpSession != "" && e.Pods == nil && e.Op != nil {
			_ = e.Op.Close(ctx, cur.OpSession)
		}
	}
	_, _ = e.Pool.Exec(ctx, `UPDATE harness_sessions SET closed_at = now(), snapshot_key = NULL WHERE conversation_id = $1 AND closed_at IS NULL`, in.ConversationID)
	if err := e.S3.Delete(ctx, snapshotKey(in.ConversationID)); err != nil {
		slog.WarnContext(ctx, "delete the snapshot of a reset conversation", "conversation", in.ConversationID, "err", err)
	}
	// the event has the id of the job (tech §2.1: 202 {eventId}); a job delivered twice writes it once
	ev, err := e.Chat.AddEvent(ctx, in.ConversationID, eventChannel(in.Channel), chat.Event{Kind: chat.EventReset}, chat.EventAt{ID: &in.MessageID})
	if errors.Is(err, chat.ErrEventExists) {
		return
	}
	if err != nil {
		slog.ErrorContext(ctx, "reset event", "err", err)
		return
	}
	metrics.ContextEvents.WithLabelValues(chat.EventReset, "false").Inc()
	e.publish(ctx, events.MessageCreated, u.ID, ev)
	e.publishContext(ctx, u.ID, in.ConversationID, 0, 0)
	if domain.Messenger(in.Channel) {
		e.deliver(ctx, u.ID, in.Channel, channels.T(u.Language, "cmd.reset"))
	}
}

// eventChannel is the channel mark of an event: the channel of the command.
func eventChannel(ch string) string {
	if ch == "" {
		return domain.ChannelWeb
	}
	return ch
}

// compactCommand compacts the context in the pod of the owner (R2).
func (e *Engine) compactCommand(ctx context.Context, key, value []byte, in chat.Inbound, u *users.User) error {
	turn := agentpods.Turn{Kind: agentpods.KindMessage, Ref: "c:" + in.MessageID.String(), Topic: kafka.TopicInbound, Key: string(key),
		Value: value, ConversationID: in.ConversationID, Channel: in.Channel}
	lease, err := e.acquire(ctx, u.ID, turn)
	if errors.Is(err, agentpods.ErrQueued) {
		return nil
	}
	if err != nil {
		return err
	}
	defer lease.Release(ctx)
	ctx, cancel := context.WithTimeout(ctx, e.Cfg.TurnTimeout)
	defer cancel()
	b, err := e.personalRequest(ctx, u, in.ConversationID, nil, "compact "+in.ConversationID.String()[:8])
	if err != nil {
		e.compactFailed(ctx, u, in, requestClass(ctx, err), err.Error())
		return nil
	}
	sess, err := e.ensureSession(ctx, lease, in.ConversationID, b, false)
	if err != nil {
		e.compactFailed(ctx, u, in, operatorClass(err), operatorText(err))
		return nil
	}
	if sess.ContextTokens > 0 && sess.ContextTokens < chat.MinCompact { // CTX-06: the size became known in the session
		e.publish(ctx, events.ChatError, u.ID, map[string]any{"conversationId": in.ConversationID, "errorClass": "context_too_short", "retryable": false,
			"text": "the context is already short", "command": chat.CommandCompact})
		if domain.Messenger(in.Channel) {
			e.deliver(ctx, u.ID, in.Channel, channels.T(u.Language, "cmd.too_short"))
		}
		return nil
	}
	if _, err := e.compact(ctx, lease.Operator(), u, b, in.ConversationID, sess, in.Instructions, false, in.Channel); err != nil {
		class, text := operatorClass(err), err.Error()
		var api *agent.APIError
		if errors.As(err, &api) {
			class, text = string(agent.Classify(api.Message).Class), api.Message
		}
		e.compactFailed(ctx, u, in, class, text)
	}
	return nil
}

// compact compacts the session and records the result: the usage, the event,
// the fill of the context and the snapshot.
func (e *Engine) compact(ctx context.Context, op Operator, u *users.User, b *built, conv uuid.UUID, sess *convSession, instructions string, auto bool, ch string) (*chat.Message, error) {
	res, err := op.Compact(ctx, sess.OpSession, instructions)
	if err != nil {
		return nil, err
	}
	if !res.Usage.IsZero() {
		connID, _ := uuid.Parse(b.model.ConnectionID)
		e.Ledger.Usage(ctx, ledger.UsageRow{UserID: &u.ID, Model: b.model.ModelID, ConnectionID: &connID, Usage: res.Usage})
	}
	window := res.ContextWindow
	if window == 0 {
		window = modelWindow(b.model)
	}
	e.saveSnapshot(context.WithoutCancel(ctx), op, conv, sess.OpSession)
	m, err := e.compactionEvent(ctx, u, conv, ch, agent.Event{Summary: res.Summary, TokensBefore: res.TokensBefore, TokensAfter: res.TokensAfter, Auto: auto})
	e.recordContext(ctx, u.ID, conv, sess.ID, res.TokensAfter, window)
	return m, err
}

// compactionEvent writes the event of a compaction (asked for or made by the
// harness itself) and tells the user.
// The fill of the context follows with recordContext.
func (e *Engine) compactionEvent(ctx context.Context, u *users.User, conv uuid.UUID, ch string, ev agent.Event) (*chat.Message, error) {
	m, err := e.Chat.AddEvent(ctx, conv, eventChannel(ch), chat.Event{Kind: chat.EventCompact, Auto: ev.Auto,
		TokensBefore: ev.TokensBefore, TokensAfter: ev.TokensAfter, Summary: ev.Summary})
	if err != nil {
		return nil, err
	}
	metrics.ContextEvents.WithLabelValues(chat.EventCompact, fmt.Sprint(ev.Auto)).Inc()
	e.publish(ctx, events.MessageCreated, u.ID, m)
	if domain.Messenger(ch) {
		e.deliver(ctx, u.ID, ch, channels.T(u.Language, "cmd.compacted", shortTokens(ev.TokensBefore, u.Language), shortTokens(ev.TokensAfter, u.Language)))
	}
	return m, nil
}

// compactFailed tells the user why the context was not compacted (CTX-07):
// the context did not change.
func (e *Engine) compactFailed(ctx context.Context, u *users.User, in chat.Inbound, class, text string) {
	slog.WarnContext(ctx, "compaction failed", "conversation", in.ConversationID, "class", class, "err", text)
	e.publish(ctx, events.ChatError, u.ID, map[string]any{"conversationId": in.ConversationID, "errorClass": class, "retryable": true,
		"text": text, "command": chat.CommandCompact})
	if domain.Messenger(in.Channel) {
		e.deliver(ctx, u.ID, in.Channel, channels.T(u.Language, "cmd.compact_failed", errorText(u.Language, class)))
	}
}

// thousands writes a number of tokens shortly: 182 тыс., 1,2 млн.
func shortTokens(n int64, lang string) string {
	k, m := "K", "M"
	if lang == "ru" {
		k, m = " тыс.", " млн"
	}
	switch {
	case n >= 1_000_000:
		return trimZero(fmt.Sprintf("%.1f", float64(n)/1e6), lang) + m
	case n >= 1000:
		return fmt.Sprintf("%d", (n+500)/1000) + k
	}
	return fmt.Sprintf("%d", n)
}

func trimZero(s, lang string) string {
	if len(s) > 2 && s[len(s)-2:] == ".0" {
		s = s[:len(s)-2]
	}
	if lang == "ru" {
		for i := range s {
			if s[i] == '.' {
				return s[:i] + "," + s[i+1:]
			}
		}
	}
	return s
}

// modelWindow is the context window of the model of a spec.
func modelWindow(m agent.ModelSpec) int64 {
	if d := modelDef(m); d != nil {
		return d.ContextWindow
	}
	return 0
}

// modelDef is the description of the model of a spec.
func modelDef(m agent.ModelSpec) *agent.ModelDef {
	for i := range m.Models {
		if m.Models[i].ID == m.ModelID {
			return &m.Models[i]
		}
	}
	return nil
}

// recordContext stores the fill of the context reported after a run (R3).
func (e *Engine) recordContext(ctx context.Context, uid uuid.UUID, conv uuid.UUID, sessID uuid.UUID, tokens, window int64) {
	if window == 0 {
		return
	}
	_, _ = e.Pool.Exec(ctx, `UPDATE harness_sessions SET context_tokens = $2, context_window = $3 WHERE id = $1`, sessID, tokens, window)
	e.publishContext(ctx, uid, conv, tokens, window)
}

// publishContext tells the site the fill of the context of a conversation (R3).
func (e *Engine) publishContext(ctx context.Context, uid, conv uuid.UUID, tokens, window int64) {
	e.publish(ctx, events.ConversationContext, uid, map[string]any{"conversationId": conv, "tokens": tokens, "window": window, "at": time.Now().UTC()})
}

// sessionSkills is the skills snapshot of a session: the snapshots of several
// items merged into one (FTR.NAB.CHAT-0001 R5); nil without skills.
func (e *Engine) sessionSkills(ctx context.Context, cat *catalog.SessionMCP) *agent.Skills {
	if cat == nil || len(cat.Skills) == 0 {
		return nil
	}
	hash, names, err := e.Catalog.SessionSkills(ctx, cat)
	if err != nil || hash == "" {
		slog.WarnContext(ctx, "skills of a session", "err", err)
		return nil
	}
	return &agent.Skills{Hash: hash, Names: names}
}

// imagesFor keeps the images of a prompt only for a model that sees them
// (FTR.NAB.CHAT-0001 R11, MDL-11, MDL-12); otherwise the agent gets the
// files in the space (promptOf has named them) or a note.
func imagesFor(m agent.ModelSpec, images []agent.Image, text string, space bool) ([]agent.Image, string) {
	if len(images) == 0 {
		return images, text
	}
	if d := modelDef(m); d == nil || d.HasInput("image") {
		metrics.PromptImages.WithLabelValues("model").Add(float64(len(images)))
		return images, text
	}
	metrics.PromptImages.WithLabelValues("space").Add(float64(len(images)))
	if !space {
		text = "[The user attached images, but the current model cannot see images.]\n" + text
	}
	return nil, text
}

// hasSkill reports whether the skill is in the skill set of the session.
func hasSkill(sk *agent.Skills, name string) bool {
	return sk != nil && slices.Contains(sk.Names, name)
}

// skillPrompt is the request with the skill's command: Pi puts the skill's
// SKILL.md into the context and the rest as the user's request.
func skillPrompt(name, prompt string) string {
	return "/skill:" + name + " " + prompt
}
