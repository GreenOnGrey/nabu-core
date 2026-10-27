package chat

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
// engine closes the session and writes the event with the returned id.
func (a *API) Reset(ctx context.Context, uid uuid.UUID, conv *uuid.UUID, channel string) (uuid.UUID, error) {
	c, err := a.conversation(ctx, uid, conv)
	if err != nil {
		return uuid.Nil, err
	}
	if err := a.idle(ctx, c.ID); err != nil {
		return uuid.Nil, err
	}
	job := uuid.New()
	return job, a.enqueue(ctx, Inbound{UserID: uid, ConversationID: c.ID, MessageID: job, Channel: channel, Command: CommandReset})
}

// Compact compacts the context of the conversation with the user's
// instructions (R2). A short or empty context is refused at once (CTX-06);
// a context of unknown size goes to the engine, which checks it again.
func (a *API) Compact(ctx context.Context, uid uuid.UUID, conv *uuid.UUID, channel, instructions string) (uuid.UUID, error) {
	c, err := a.conversation(ctx, uid, conv)
	if err != nil {
		return uuid.Nil, err
	}
	instructions = strings.TrimSpace(instructions)
	if len([]rune(instructions)) > 500 {
		return uuid.Nil, apperr.Unprocessable("invalid_instructions", "the instructions are longer than 500 characters").With("field", "instructions")
	}
	if err := a.idle(ctx, c.ID); err != nil {
		return uuid.Nil, err
	}
	if f := a.Store.Context(ctx, c.ID); f.Empty || (f.Window > 0 && f.Tokens < MinCompact) {
		return uuid.Nil, apperr.Unprocessable("context_too_short", "the context is already short")
	}
	job := uuid.New()
	return job, a.enqueue(ctx, Inbound{UserID: uid, ConversationID: c.ID, MessageID: job, Channel: channel, Command: CommandCompact, Instructions: instructions})
}

// idle refuses a command while an answer is being written (CTX-02).
func (a *API) idle(ctx context.Context, conv uuid.UUID) error {
	if a.Store.Busy(ctx, conv, staleTurn) {
		return apperr.Conflict("context_busy", "wait for the answer or stop it")
	}
	return nil
}

// stop stops the answer being written in the main conversation of the chat
// (FTR.NAB.CHAT-0004 R11) and returns the key of the reply. In a group chat
// only the author of the message being answered may stop it.
func (a *API) stop(ctx context.Context, in channels.Inbound) string {
	c, err := a.Store.Main(ctx, in.UserID)
	if err != nil {
		slog.WarnContext(ctx, "stop: the main conversation", "err", err)
		return "cmd.stop_failed"
	}
	m := a.Store.Running(ctx, c.ID)
	if m == nil {
		return "cmd.not_running"
	}
	// in a group only the author of the message being answered; when the
	// author cannot be checked the answer goes on
	if by := groupAuthor(in.Context); by != uuid.Nil {
		if m.ReplyTo == nil {
			return "cmd.stop_forbidden"
		}
		if q, err := a.Store.Message(ctx, in.UserID, *m.ReplyTo); err != nil || groupAuthor(q.Context) != by {
			return "cmd.stop_forbidden"
		}
	}
	if err := a.Store.RequestStop(ctx, in.UserID, m.ID); err != nil {
		return "cmd.not_running"
	}
	return "cmd.stopped"
}

// groupAuthor is the Nabu user who wrote a message of a group chat
// (FTR.NAB.CMN-0007), uuid.Nil — not a group message.
func groupAuthor(raw json.RawMessage) uuid.UUID {
	var c struct {
		Group *struct {
			AuthorID uuid.UUID `json:"authorId"`
		} `json:"group"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &c) != nil || c.Group == nil {
		return uuid.Nil
	}
	return c.Group.AuthorID
}

// say sends a service text to the messenger of the user.
func (a *API) say(ctx context.Context, uid uuid.UUID, ch, text string) {
	b, _ := json.Marshal(map[string]any{"userId": uid, "channel": ch, "text": text})
	if err := a.Bus.Publish(ctx, kafka.TopicOutbound, uid.String(), b); err != nil {
		slog.WarnContext(ctx, "publish outbound", "err", err)
	}
}

// command takes the commands of a messenger (R6): /new, /compact [text],
// /skills, /skill <name> [task], /stop (FTR.NAB.CHAT-0004 R11). done reports
// that the message was a command.
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
	case "/stop":
		a.say(ctx, in.UserID, in.Channel, channels.T(lang, a.stop(ctx, in)))
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
	text, err := "", errTooLong
	if size <= a.voiceReadLimit() {
		data, rerr := storage.ReadAll(ctx, a.S3, key)
		if rerr != nil {
			return "", false
		}
		text, err = a.recognize(ctx, channelLabel(ch), data, name, lang)
	} else {
		metrics.Transcriptions.WithLabelValues(channelLabel(ch), "too_long").Inc()
	}
	if err != nil {
		if !errors.Is(err, errTooLong) {
			slog.WarnContext(ctx, "voice message", "channel", ch, "err", err)
		}
		a.say(ctx, uid, ch, channels.T(lang, "voice.failed"))
		return "", false
	}
	return text, true
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
	data, err := io.ReadAll(io.LimitReader(f, a.voiceReadLimit()+1))
	if err != nil {
		return "", apperr.BadRequest("invalid_upload", "the recording could not be read")
	}
	text, err := "", errTooLong
	if int64(len(data)) <= a.voiceReadLimit() {
		text, err = a.recognize(ctx, "web", data, name, a.Users.Language(ctx, uid))
	} else {
		metrics.Transcriptions.WithLabelValues("web", "too_long").Inc()
	}
	switch {
	case errors.Is(err, errTooLong):
		return "", apperr.Unprocessable("transcription_too_long", fmt.Sprintf("the recording is longer than %d seconds", a.maxVoiceSeconds())).
			With("maxSeconds", a.maxVoiceSeconds())
	case err != nil:
		slog.WarnContext(ctx, "transcription failed", "err", err)
		return "", apperr.Unavailable("transcription_failed", "the voice message could not be recognized")
	}
	return text, nil
}

// Limits of a recording (R20, tech §2.5).
const (
	recognizeTimeout = 60 * time.Second
	compressedRate   = 48 << 10  // bytes a second of compressed audio at most (opus, aac, mp3 up to 384 kbit/s)
	wavRate          = 192 << 10 // bytes a second of uncompressed audio at most (48 kHz stereo)
)

var errTooLong = errors.New("the recording is too long")

// recognize turns a recording into text within the limits: longer than
// TRANSCRIBE_MAX_SECONDS — errTooLong; no answer in 60 seconds or no
// words — an error.
func (a *API) recognize(ctx context.Context, label string, data []byte, name, lang string) (string, error) {
	if a.tooLong(data) {
		metrics.Transcriptions.WithLabelValues(label, "too_long").Inc()
		return "", errTooLong
	}
	start := time.Now()
	tctx, cancel := context.WithTimeout(ctx, recognizeTimeout)
	defer cancel()
	text, err := a.Whisper.Transcribe(tctx, bytes.NewReader(data), name, lang)
	metrics.TranscriptionSeconds.Observe(time.Since(start).Seconds())
	text = strings.TrimSpace(text)
	if err == nil && text == "" {
		err = errors.New("no words recognized")
	}
	if err != nil {
		metrics.Transcriptions.WithLabelValues(label, "error").Inc()
		return "", err
	}
	metrics.Transcriptions.WithLabelValues(label, "ok").Inc()
	return text, nil
}

// tooLong reports a recording longer than the limit: by the header of a WAV
// file, by the size of a compressed one.
func (a *API) tooLong(data []byte) bool {
	max := a.maxVoiceSeconds()
	if sec, ok := wavSeconds(data); ok {
		return sec > float64(max)
	}
	return int64(len(data)) > int64(max)*compressedRate
}

// wavSeconds is the length of a WAV recording from its header.
func wavSeconds(data []byte) (float64, bool) {
	if len(data) < 44 || string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return 0, false
	}
	for off := 12; off+8 <= len(data); {
		id, size := string(data[off:off+4]), int(binary.LittleEndian.Uint32(data[off+4:off+8]))
		if id == "fmt " && off+20 <= len(data) {
			rate := binary.LittleEndian.Uint32(data[off+16 : off+20])
			if rate == 0 {
				return 0, false
			}
			return float64(len(data)-44) / float64(rate), true
		}
		off += 8 + size + size%2
	}
	return 0, false
}

func (a *API) maxVoiceSeconds() int {
	if a.MaxVoiceSeconds > 0 {
		return a.MaxVoiceSeconds
	}
	return 300
}

// voiceReadLimit bounds what is read of a recording: the limit as WAV.
func (a *API) voiceReadLimit() int64 {
	return int64(a.maxVoiceSeconds())*wavRate + 1<<10
}
