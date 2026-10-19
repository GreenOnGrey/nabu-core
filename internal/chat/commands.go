package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
)

// conversation returns the conversation of the user; nil is the main one.
func (a *API) conversation(ctx context.Context, uid uuid.UUID, conv *uuid.UUID) (*Conversation, error) {
	if conv == nil {
		return a.Store.Main(ctx, uid)
	}
	return a.Store.Get(ctx, uid, *conv)
}

// Reset starts the conversation over with an empty context
// (FTR.NAB.CHAT-0001 R1): the job goes to the queue of the conversation, the
// engine closes the session and writes the event.
func (a *API) Reset(ctx context.Context, uid uuid.UUID, conv *uuid.UUID, channel string) (uuid.UUID, error) {
	c, err := a.conversation(ctx, uid, conv)
	if err != nil {
		return uuid.Nil, err
	}
	if a.Store.Busy(ctx, c.ID, staleTurn) { // CTX-02
		return uuid.Nil, apperr.Conflict("context_busy", "wait for the answer or stop it")
	}
	job := uuid.New()
	return job, a.enqueue(ctx, Inbound{UserID: uid, ConversationID: c.ID, MessageID: job, Channel: channel, Command: CommandReset})
}

// Compact compacts the context of the conversation with the user's
// instructions (R2). A short context is refused at once (CTX-06); a
// context of unknown size goes to the engine.
func (a *API) Compact(ctx context.Context, uid uuid.UUID, conv *uuid.UUID, channel, instructions string) (uuid.UUID, error) {
	c, err := a.conversation(ctx, uid, conv)
	if err != nil {
		return uuid.Nil, err
	}
	instructions = strings.TrimSpace(instructions)
	if len([]rune(instructions)) > 500 {
		return uuid.Nil, apperr.Unprocessable("invalid_instructions", "the instructions are longer than 500 characters").With("field", "instructions")
	}
	if a.Store.Busy(ctx, c.ID, staleTurn) {
		return uuid.Nil, apperr.Conflict("context_busy", "wait for the answer or stop it")
	}
	if f := a.Store.Context(ctx, c.ID); f.Window > 0 && f.Tokens < minCompact {
		return uuid.Nil, apperr.Unprocessable("context_too_short", "the context is already short")
	}
	job := uuid.New()
	return job, a.enqueue(ctx, Inbound{UserID: uid, ConversationID: c.ID, MessageID: job, Channel: channel, Command: CommandCompact, Instructions: instructions})
}

// say sends a service text to the messenger of the user.
func (a *API) say(ctx context.Context, uid uuid.UUID, ch, text string) {
	b, _ := json.Marshal(map[string]any{"userId": uid, "channel": ch, "text": text})
	if err := a.Bus.Publish(ctx, kafka.TopicOutbound, uid.String(), b); err != nil {
		slog.WarnContext(ctx, "publish outbound", "err", err)
	}
}

// command takes the commands of a messenger (R6): /new, /compact [text],
// /skills, /skill <name> [task]. done reports that the message was a command.
func (a *API) command(ctx context.Context, in channels.Inbound) (bool, error) {
	text := strings.TrimSpace(in.Text)
	if !strings.HasPrefix(text, "/") {
		return false, nil
	}
	word, rest, _ := strings.Cut(text, " ")
	word, _, _ = strings.Cut(word, "@") // /new@nabu_bot in a group
	rest = strings.TrimSpace(rest)
	lang := a.Users.Language(ctx, in.UserID)
	fail := func(err error) {
		if e, ok := apperr.As(err); ok {
			switch e.Code {
			case "context_busy":
				a.say(ctx, in.UserID, in.Channel, channels.T(lang, "cmd.busy"))
				return
			case "context_too_short":
				a.say(ctx, in.UserID, in.Channel, channels.T(lang, "cmd.too_short"))
				return
			}
		}
		slog.WarnContext(ctx, "messenger command", "command", word, "err", err)
		a.say(ctx, in.UserID, in.Channel, channels.T(lang, "tg.not_delivered"))
	}
	switch word {
	case "/new":
		if _, err := a.Reset(ctx, in.UserID, nil, in.Channel); err != nil {
			fail(err)
		}
		return true, nil
	case "/compact":
		if _, err := a.Compact(ctx, in.UserID, nil, in.Channel, rest); err != nil {
			fail(err)
		}
		return true, nil
	case "/skills":
		a.say(ctx, in.UserID, in.Channel, a.skillList(ctx, in.UserID, lang))
		return true, nil
	case "/skill":
		name, task, _ := strings.Cut(rest, " ")
		if name == "" {
			a.say(ctx, in.UserID, in.Channel, a.skillList(ctx, in.UserID, lang))
			return true, nil
		}
		_, err := a.PostMessage(ctx, in.UserID, nil, PostInput{Text: strings.TrimSpace(task), Channel: in.Channel, Attachments: in.Attachments,
			Context: in.Context, Skill: name})
		if e, ok := apperr.As(err); ok && e.Code == "skill_unavailable" {
			a.say(ctx, in.UserID, in.Channel, channels.T(lang, "cmd.skill_unavailable", name)+"\n\n"+a.skillList(ctx, in.UserID, lang))
			return true, nil
		}
		return true, err
	}
	return false, nil
}

// skillList is the answer of /skills.
func (a *API) skillList(ctx context.Context, uid uuid.UUID, lang string) string {
	var list []Skill
	if a.Skills != nil {
		list, _ = a.Skills(ctx, uid)
	}
	if len(list) == 0 {
		return channels.T(lang, "cmd.no_skills")
	}
	var b strings.Builder
	b.WriteString(channels.T(lang, "cmd.skills"))
	for _, s := range list {
		b.WriteString("\n• " + s.Name)
		if s.Description != "" {
			b.WriteString(" — " + s.Description)
		}
	}
	return b.String()
}

// audio reports whether an attachment is a voice message or a recording.
func audio(mime, name string) bool {
	if strings.HasPrefix(mime, "audio/") {
		return true
	}
	switch strings.ToLower(path.Ext(name)) {
	case ".ogg", ".oga", ".opus", ".m4a", ".aac", ".mp3", ".wav", ".amr", ".webm":
		return true
	}
	return false
}

// voice recognizes a voice message of a messenger (R21): the text of the
// message for the agent; the recording stays an attachment. On a failure the
// user is told and the recording goes to the agent as a file.
func (a *API) voice(ctx context.Context, uid uuid.UUID, ch string, att uuid.UUID) (string, bool) {
	if a.Whisper == nil {
		return "", false
	}
	var name, mime, key string
	var size int64
	if err := a.Pool.QueryRow(ctx, `SELECT file_name, mime_type, size_bytes, s3_key FROM attachments WHERE id = $1 AND user_id = $2`, att, uid).
		Scan(&name, &mime, &size, &key); err != nil || !audio(mime, name) {
		return "", false
	}
	lang := a.Users.Language(ctx, uid)
	label := channelLabel(ch)
	if a.MaxVoice > 0 && size > a.MaxVoice {
		metrics.Transcriptions.WithLabelValues(label, "too_long").Inc()
		a.say(ctx, uid, ch, channels.T(lang, "voice.failed"))
		return "", false
	}
	data, err := storage.ReadAll(ctx, a.S3, key)
	if err != nil {
		return "", false
	}
	start := time.Now()
	tctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	text, err := a.Whisper.Transcribe(tctx, bytes.NewReader(data), name, lang)
	metrics.TranscriptionSeconds.Observe(time.Since(start).Seconds())
	if err != nil || strings.TrimSpace(text) == "" {
		metrics.Transcriptions.WithLabelValues(label, "error").Inc()
		slog.WarnContext(ctx, "voice message", "channel", ch, "err", err)
		a.say(ctx, uid, ch, channels.T(lang, "voice.failed"))
		return "", false
	}
	metrics.Transcriptions.WithLabelValues(label, "ok").Inc()
	return strings.TrimSpace(text), true
}

func channelLabel(ch string) string {
	if i := strings.IndexByte(ch, ':'); i > 0 {
		return ch[:i]
	}
	return ch
}

// transcribe serves POST /transcribe of the site (R20).
func (a *API) transcribe(ctx context.Context, uid uuid.UUID, f io.Reader, name string) (string, error) {
	if a.Whisper == nil {
		return "", apperr.Unavailable("transcription_unavailable", "voice input is not configured")
	}
	data, err := io.ReadAll(io.LimitReader(f, a.maxVoice()+1))
	if err != nil {
		return "", apperr.BadRequest("invalid_upload", "the recording could not be read")
	}
	if int64(len(data)) > a.maxVoice() {
		metrics.Transcriptions.WithLabelValues("web", "too_long").Inc()
		return "", apperr.Unprocessable("transcription_too_long", "the recording is longer than five minutes")
	}
	start := time.Now()
	text, err := a.Whisper.Transcribe(ctx, bytes.NewReader(data), name, a.Users.Language(ctx, uid))
	metrics.TranscriptionSeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.Transcriptions.WithLabelValues("web", "error").Inc()
		slog.WarnContext(ctx, "transcription failed", "err", err)
		return "", apperr.Unavailable("transcription_failed", "the voice message could not be recognized")
	}
	metrics.Transcriptions.WithLabelValues("web", "ok").Inc()
	return strings.TrimSpace(text), nil
}

func (a *API) maxVoice() int64 {
	if a.MaxVoice > 0 {
		return a.MaxVoice
	}
	return 25 << 20
}
