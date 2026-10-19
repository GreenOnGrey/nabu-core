package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
	"github.com/GreenOnGrey/nabu-core/internal/platform/whisper"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// MaxText bounds a message.
const MaxText = 32000

// Inbound is the payload of nabu.inbound: a stored user message to answer.
type Inbound struct {
	UserID         uuid.UUID       `json:"userId"`
	ConversationID uuid.UUID       `json:"conversationId"`
	MessageID      uuid.UUID       `json:"messageId"`
	Channel        string          `json:"channel"`
	ClientID       *uuid.UUID      `json:"clientId,omitempty"`
	Context        json.RawMessage `json:"context,omitempty"`
	RetryOf        *uuid.UUID      `json:"retryOf,omitempty"`
	// Command is a command of the conversation instead of a message
	// (FTR.NAB.CHAT-0001 R1, R2, tech §2.1): reset or compact. MessageID is
	// then the id of the job; Instructions guide a compaction.
	Command      string `json:"command,omitempty"`
	Instructions string `json:"instructions,omitempty"`
}

// Commands of a conversation.
const (
	CommandReset   = "reset"
	CommandCompact = "compact"
)

// Skill is a skill the user may call (FTR.NAB.CHAT-0001 R4).
type Skill struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	ItemID      uuid.UUID `json:"itemId"`
	ItemTitle   string    `json:"itemTitle"`
}

// API serves the site's (and the delegated clients') conversation endpoints.
type API struct {
	Store  *Store
	Pool   *pgxpool.Pool
	Users  *users.Repo
	Models *models.Service
	// Channels lists the channels of a user for GET /me.
	Channels func(ctx context.Context, uid uuid.UUID) ([]channels.MyChannel, error)
	Bus      kafka.Publisher
	Events   events.Publisher
	S3       storage.Storage
	Whisper  whisper.Transcriber
	MaxFile  int64
	// Skills lists the skills the user may call (FTR.NAB.CHAT-0001 R4); nil: none.
	Skills func(ctx context.Context, uid uuid.UUID) ([]Skill, error)
	// MaxVoice bounds a voice recording in bytes (R20: five minutes).
	MaxVoice int64
}

// staleTurn: an answer streaming for longer is not a running turn (R1).
const staleTurn = 15 * time.Minute

// minCompact: a shorter context is not compacted (R2).
const minCompact = 2000

// ─── attachments ────────────────────────────────────────────────────

// Store implements channels.Attachments and the upload: the file goes to
// S3 attachments/ (arch §10); the engine copies it into the space.
func (a *API) storeAttachment(ctx context.Context, uid uuid.UUID, name, mime string, r io.Reader, size int64) (uuid.UUID, error) {
	name = path.Base(strings.ReplaceAll(strings.TrimSpace(name), "\\", "/"))
	if name == "" || name == "." || name == "/" {
		name = "file"
	}
	if mime == "" {
		mime = "application/octet-stream"
	}
	id := uuid.New()
	key := "attachments/" + uid.String() + "/" + id.String()
	cr := &counter{r: io.LimitReader(r, a.MaxFile+1)}
	if err := a.S3.Put(ctx, key, cr, size, mime); err != nil {
		return uuid.Nil, err
	}
	if cr.n > a.MaxFile {
		_ = a.S3.Delete(ctx, key)
		return uuid.Nil, apperr.TooLarge("too_large", fmt.Sprintf("the file is larger than %d MB", a.MaxFile>>20))
	}
	_, err := a.Pool.Exec(ctx, `INSERT INTO attachments (id, user_id, file_name, mime_type, size_bytes, s3_key) VALUES ($1,$2,$3,$4,$5,$6)`,
		id, uid, name, mime, cr.n, key)
	return id, err
}

// Attachments returns the attachments store for channels.
func (a *API) Attachments() channels.Attachments { return attachmentStore{a} }

type attachmentStore struct{ a *API }

func (s attachmentStore) Store(ctx context.Context, uid uuid.UUID, name, mime string, r io.Reader, size int64) (uuid.UUID, error) {
	return s.a.storeAttachment(ctx, uid, name, mime, r, size)
}

type counter struct {
	r io.Reader
	n int64
}

func (c *counter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (a *API) attachments(ctx context.Context, uid uuid.UUID, ids []uuid.UUID) ([]Attachment, error) {
	out := []Attachment{}
	for _, id := range ids {
		var at Attachment
		if err := a.Pool.QueryRow(ctx, `SELECT id, file_name, mime_type, size_bytes FROM attachments WHERE id = $1 AND user_id = $2`, id, uid).
			Scan(&at.ID, &at.FileName, &at.MimeType, &at.Size); err != nil {
			return nil, apperr.Unprocessable("invalid_attachment", "unknown attachment").With("attachmentId", id)
		}
		out = append(out, at)
	}
	return out, nil
}

// ─── receiving ──────────────────────────────────────────────────────

// Post stores a user message and queues the answer (R4: one main
// conversation for every channel). conv nil means the main conversation.
func (a *API) Post(ctx context.Context, uid uuid.UUID, conv *uuid.UUID, text, channel string, clientID *uuid.UUID, attIDs []uuid.UUID, msgCtx json.RawMessage) (*Message, error) {
	return a.PostMessage(ctx, uid, conv, PostInput{Text: text, Channel: channel, ClientID: clientID, Attachments: attIDs, Context: msgCtx})
}

// PostInput is a user message with its skill and voice mark.
type PostInput struct {
	Text, Channel string
	ClientID      *uuid.UUID
	Attachments   []uuid.UUID
	Context       json.RawMessage
	Skill         string // FTR.NAB.CHAT-0001 R5
	Voice         bool   // R21
}

// skill checks that the user may call the skill (R5, SKL-04).
func (a *API) skill(ctx context.Context, uid uuid.UUID, name string) error {
	if name == "" {
		return nil
	}
	if a.Skills != nil {
		list, err := a.Skills(ctx, uid)
		if err != nil {
			return err
		}
		for _, s := range list {
			if s.Name == name {
				metrics.SkillInvocations.WithLabelValues("ok").Inc()
				return nil
			}
		}
	}
	metrics.SkillInvocations.WithLabelValues("unavailable").Inc()
	return apperr.Unprocessable("skill_unavailable", "the skill is not available").With("skill", name)
}

// PostMessage stores a user message and queues the answer.
func (a *API) PostMessage(ctx context.Context, uid uuid.UUID, conv *uuid.UUID, in PostInput) (*Message, error) {
	text, channel, clientID, attIDs, msgCtx := strings.TrimSpace(in.Text), in.Channel, in.ClientID, in.Attachments, in.Context
	if err := a.skill(ctx, uid, in.Skill); err != nil {
		return nil, err
	}
	if text == "" && len(attIDs) == 0 && in.Skill == "" {
		return nil, apperr.Unprocessable("empty_message", "the message is empty").With("field", "text")
	}
	if utf8.RuneCountInString(text) > MaxText {
		return nil, apperr.Unprocessable("message_too_long", "the message is too long").With("field", "text")
	}
	if len(msgCtx) > 16<<10 {
		return nil, apperr.Unprocessable("context_too_large", "the context is larger than 16 KB")
	}
	var c *Conversation
	var err error
	if conv == nil {
		c, err = a.Store.Main(ctx, uid)
	} else {
		c, err = a.Store.Get(ctx, uid, *conv)
	}
	if err != nil {
		return nil, err
	}
	if c.ArchivedAt != nil {
		return nil, apperr.Conflict("conversation_archived", "the topic is archived; unarchive it to continue")
	}
	atts, err := a.attachments(ctx, uid, attIDs)
	if err != nil {
		return nil, err
	}
	m, err := a.Store.AddUserMessage(ctx, c.ID, UserMessage{Text: text, Channel: channel, Attachments: atts, Context: msgCtx, Skill: in.Skill, Voice: in.Voice})
	if err != nil {
		return nil, err
	}
	a.Events.Publish(ctx, events.Event{Type: events.MessageCreated, UserID: &uid, Data: m})
	if err := a.enqueue(ctx, Inbound{UserID: uid, ConversationID: c.ID, MessageID: m.ID, Channel: channel, ClientID: clientID, Context: msgCtx}); err != nil {
		return nil, err
	}
	return m, nil
}

func (a *API) enqueue(ctx context.Context, in Inbound) error {
	b, _ := json.Marshal(in)
	if err := a.Bus.Publish(ctx, kafka.TopicInbound, in.UserID.String(), b); err != nil {
		slog.ErrorContext(ctx, "publish inbound", "err", err)
		return apperr.Unavailable("agent_unavailable", "the agent is temporarily unavailable")
	}
	return nil
}

// Receive implements channels.Inbox: messenger messages go to the main
// conversation, letters to their topic (FTR.NAB.CMN-0002 R3). In messengers
// the commands of the conversation are taken here (FTR.NAB.CHAT-0001 R6) and
// a voice message becomes its text (R21).
func (a *API) Receive(ctx context.Context, in channels.Inbound) error {
	if domain.Messenger(in.Channel) && in.Conversation == nil {
		if done, err := a.command(ctx, in); done {
			return err
		}
	}
	pi := PostInput{Text: in.Text, Channel: in.Channel, Attachments: in.Attachments, Context: in.Context}
	if strings.TrimSpace(in.Text) == "" && len(in.Attachments) == 1 && domain.Messenger(in.Channel) {
		if text, ok := a.voice(ctx, in.UserID, in.Channel, in.Attachments[0]); ok {
			pi.Text, pi.Voice = text, true
		}
	}
	_, err := a.PostMessage(ctx, in.UserID, in.Conversation, pi)
	return err
}

// Retry queues the user message of a failed answer again (R30, CONV-09).
func (a *API) Retry(ctx context.Context, p *domain.Principal, id uuid.UUID) error {
	m, err := a.Store.Message(ctx, p.UserID, id)
	if err != nil {
		return err
	}
	if m.Role != "assistant" || m.Status != "failed" || m.ReplyTo == nil {
		return apperr.Conflict("not_retryable", "only a failed answer can be retried")
	}
	user, err := a.Store.Message(ctx, p.UserID, *m.ReplyTo)
	if err != nil {
		return err
	}
	return a.enqueue(ctx, Inbound{UserID: p.UserID, ConversationID: m.ConversationID, MessageID: user.ID, Channel: p.Channel(),
		ClientID: p.ClientID, Context: user.Context, RetryOf: &m.ID})
}

// ─── profile and agent (tech §3.1) ──────────────────────────────────

// Me is GET /me.
type Me struct {
	ID       uuid.UUID            `json:"id"`
	Email    string               `json:"email"`
	Name     string               `json:"name"`
	Avatar   string               `json:"avatarUrl,omitempty"`
	IsAdmin  bool                 `json:"isAdmin"`
	Language string               `json:"language"`
	Theme    string               `json:"theme"`
	Timezone string               `json:"timezone"`
	Channels []channels.MyChannel `json:"channels"`
}

// AgentView is GET /agent (FTR.NAB.CHAT-0001 tech §2.2): the model in use,
// the default one, the selectable ones with their capabilities and the fill
// of the context of the main conversation.
type AgentView struct {
	Name            string                  `json:"name"`
	Tone            string                  `json:"tone"`
	Model           *models.AvailableModel  `json:"model"`
	DefaultModel    *models.AvailableModel  `json:"defaultModel"`
	Chosen          bool                    `json:"chosen"` // the user chose the model; false — the default
	AvailableModels []models.AvailableModel `json:"availableModels"`
	Context         ContextFill             `json:"context"`
}

func (a *API) agentView(ctx context.Context, uid uuid.UUID) (*AgentView, error) {
	ag, err := a.Users.GetAgent(ctx, uid)
	if err != nil {
		return nil, err
	}
	avail, err := a.Models.AvailableFor(ctx, uid)
	if err != nil {
		return nil, err
	}
	v := &AgentView{Name: ag.Name, Tone: ag.Tone, AvailableModels: avail}
	for i, m := range avail {
		if ag.ConnectionID != nil && ag.Model != nil && m.ConnectionID == *ag.ConnectionID && m.Model == *ag.Model {
			v.Model, v.Chosen = &avail[i], true
		}
	}
	// the model the agent runs on when the user did not choose (FTR.NAB.CMN-0007 R8)
	if v.DefaultModel, err = a.Models.DefaultFor(ctx, uid); err != nil {
		return nil, err
	}
	if v.Model == nil {
		v.Model = v.DefaultModel
	}
	if main, err := a.Store.Main(ctx, uid); err == nil {
		v.Context = a.Store.Context(ctx, main.ID)
	}
	return v, nil
}

// PatchAgent changes name, tone and model; open sessions get them with the
// next message (R3). clearModel returns to the default model
// (FTR.NAB.CHAT-0001 R16).
func (a *API) PatchAgent(ctx context.Context, uid uuid.UUID, name, tone *string, model *models.Choice, clearModel ...bool) (*AgentView, error) {
	if name != nil {
		n := strings.TrimSpace(*name)
		if n == "" || utf8.RuneCountInString(n) > 40 {
			return nil, apperr.Unprocessable("invalid_name", "the name is 1–40 characters").With("field", "name")
		}
		name = &n
	}
	if tone != nil && !domain.ValidTone(*tone) {
		return nil, apperr.Unprocessable("invalid_tone", "unknown tone").With("field", "tone")
	}
	var conn *uuid.UUID
	var mid *string
	if model != nil {
		avail, err := a.Models.AvailableFor(ctx, uid)
		if err != nil {
			return nil, err
		}
		ok := false
		for _, m := range avail {
			if m.ConnectionID == model.ConnectionID && m.Model == model.Model {
				ok = true
			}
		}
		if !ok { // MOD-03, SEL-02
			return nil, apperr.Unprocessable("model_not_granted", "the model is not granted to you").With("field", "model")
		}
		conn, mid = &model.ConnectionID, &model.Model
	}
	before, _ := a.agentView(ctx, uid)
	if err := a.Users.SetAgent(ctx, uid, name, tone, conn, mid); err != nil {
		return nil, err
	}
	if len(clearModel) > 0 && clearModel[0] {
		if err := a.Users.ClearModel(ctx, uid); err != nil {
			return nil, err
		}
	}
	v, err := a.agentView(ctx, uid)
	if err != nil {
		return nil, err
	}
	a.Events.Publish(ctx, events.Event{Type: events.AgentUpdated, UserID: &uid, Data: v})
	// R16: the change of the model is an event of the main conversation
	if before != nil && v.Model != nil && (before.Model == nil || before.Model.ConnectionID != v.Model.ConnectionID || before.Model.Model != v.Model.Model) {
		if main, err := a.Store.Main(ctx, uid); err == nil {
			cid := v.Model.ConnectionID
			if ev, err := a.Store.AddEvent(ctx, main.ID, domain.ChannelWeb, Event{Kind: EventModel, ConnectionID: &cid, Model: v.Model.Model, Name: v.Model.Name}); err == nil {
				a.Events.Publish(ctx, events.Event{Type: events.MessageCreated, UserID: &uid, Data: ev})
			}
		}
	}
	return v, nil
}

// ─── HTTP ───────────────────────────────────────────────────────────

// Routes mounts the endpoints shared by the site (/api/v1) and delegated
// clients (/client/v1/me).
func (a *API) Routes(r chi.Router, site bool) {
	h := httpx.Handler
	if site {
		r.Get("/me", h(func(w http.ResponseWriter, r *http.Request) error {
			p, err := httpx.MustPrincipal(r)
			if err != nil {
				return err
			}
			u, err := a.Users.Get(r.Context(), p.UserID)
			if err != nil || u == nil {
				return apperr.ErrNoSession
			}
			chs := []channels.MyChannel{}
			if a.Channels != nil {
				if chs, err = a.Channels(r.Context(), p.UserID); err != nil {
					return err
				}
			}
			httpx.JSON(w, 200, Me{ID: u.ID, Email: u.Email, Name: u.Name, Avatar: u.AvatarURL, IsAdmin: u.IsAdmin, Language: u.Language,
				Theme: u.Theme, Timezone: u.Timezone, Channels: chs})
			return nil
		}))
		r.Patch("/me", h(func(w http.ResponseWriter, r *http.Request) error {
			p, err := httpx.MustPrincipal(r)
			if err != nil {
				return err
			}
			var in users.Patch
			if err := httpx.Decode(r, &in); err != nil {
				return err
			}
			if err := a.Users.UpdateProfile(r.Context(), p.UserID, in); err != nil {
				return err
			}
			httpx.NoContent(w)
			return nil
		}))
		r.Post("/transcribe", h(func(w http.ResponseWriter, r *http.Request) error {
			p, err := httpx.MustPrincipal(r)
			if err != nil {
				return err
			}
			if a.Whisper == nil {
				return apperr.Unavailable("transcription_unavailable", "voice input is not configured")
			}
			f, hdr, err := formFile(r, a.maxVoice())
			if err != nil {
				return err
			}
			defer f.Close()
			text, err := a.transcribe(r.Context(), p.UserID, f, hdr)
			if err != nil {
				return err
			}
			httpx.JSON(w, 200, map[string]string{"text": text})
			return nil
		}))
	}
	r.Get("/agent", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		v, err := a.agentView(r.Context(), p.UserID)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, v)
		return nil
	}))
	r.Patch("/agent", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		var in struct {
			Name  *string         `json:"name"`
			Tone  *string         `json:"tone"`
			Model json.RawMessage `json:"model"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		var model *models.Choice
		clear := string(in.Model) == "null" // R16: «По умолчанию»
		if len(in.Model) > 0 && !clear {
			model = &models.Choice{}
			if err := json.Unmarshal(in.Model, model); err != nil {
				return apperr.Unprocessable("invalid_model", "the model is {connectionId, model} or null").With("field", "model")
			}
		}
		v, err := a.PatchAgent(r.Context(), p.UserID, in.Name, in.Tone, model, clear)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, v)
		return nil
	}))
	r.Get("/conversations", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		l, err := a.Store.List(r.Context(), p.UserID, r.URL.Query().Get("archived") == "true")
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Get("/skills", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		list := []Skill{}
		if a.Skills != nil {
			if list, err = a.Skills(r.Context(), p.UserID); err != nil {
				return err
			}
		}
		httpx.JSON(w, 200, map[string]any{"items": list})
		return nil
	}))
	r.Get("/conversations/{id}", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := convID(r, a, p)
		if err != nil {
			return err
		}
		c, err := a.Store.Get(r.Context(), p.UserID, id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, struct {
			*Conversation
			Context ContextFill `json:"context"`
		}{c, a.Store.Context(r.Context(), id)})
		return nil
	}))
	r.Post("/conversations/{id}/context:reset", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := convID(r, a, p)
		if err != nil {
			return err
		}
		job, err := a.Reset(r.Context(), p.UserID, &id, p.Channel())
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusAccepted, map[string]any{"jobId": job})
		return nil
	}))
	r.Post("/conversations/{id}/context:compact", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := convID(r, a, p)
		if err != nil {
			return err
		}
		var in struct {
			Instructions string `json:"instructions"`
		}
		if r.ContentLength != 0 {
			if err := httpx.Decode(r, &in); err != nil {
				return err
			}
		}
		job, err := a.Compact(r.Context(), p.UserID, &id, p.Channel(), in.Instructions)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusAccepted, map[string]any{"jobId": job})
		return nil
	}))
	r.Post("/conversations", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		var in struct {
			Title string `json:"title"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		in.Title = strings.TrimSpace(in.Title)
		if in.Title == "" || utf8.RuneCountInString(in.Title) > 120 {
			return apperr.Unprocessable("invalid_title", "the title is 1–120 characters").With("field", "title")
		}
		c, err := a.Store.CreateTopic(r.Context(), p.UserID, in.Title)
		if err != nil {
			return err
		}
		a.Events.Publish(r.Context(), events.Event{Type: events.ConversationUpdated, UserID: &p.UserID, Data: c})
		httpx.JSON(w, http.StatusCreated, c)
		return nil
	}))
	r.Patch("/conversations/{id}", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			Title    *string `json:"title"`
			Archived *bool   `json:"archived"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		c, err := a.Store.Patch(r.Context(), p.UserID, id, in.Title, in.Archived)
		if err != nil {
			return err
		}
		a.Events.Publish(r.Context(), events.Event{Type: events.ConversationUpdated, UserID: &p.UserID, Data: c})
		httpx.JSON(w, 200, c)
		return nil
	}))
	r.Post("/conversations/{id}/read", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := convID(r, a, p)
		if err != nil {
			return err
		}
		if err := a.Store.Read(r.Context(), p.UserID, id); err != nil {
			return err
		}
		a.Events.Publish(r.Context(), events.Event{Type: "conversation.unread", UserID: &p.UserID, Data: map[string]any{"conversationId": id, "unreadCount": 0}})
		httpx.NoContent(w)
		return nil
	}))
	r.Get("/conversations/{id}/messages", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := convID(r, a, p)
		if err != nil {
			return err
		}
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		l, err := a.Store.Messages(r.Context(), id, page)
		if err != nil {
			return err
		}
		for i := range l.Items {
			if c := l.Items[i].ErrorClass; c != nil {
				l.Items[i].Retryable = true
			}
		}
		httpx.JSON(w, 200, l)
		return nil
	}))
	r.Post("/conversations/{id}/messages", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := convID(r, a, p)
		if err != nil {
			return err
		}
		var in struct {
			Text          string          `json:"text"`
			AttachmentIDs []uuid.UUID     `json:"attachmentIds"`
			Context       json.RawMessage `json:"context"`
			Skill         string          `json:"skill"`
		}
		if err := httpx.DecodeMax(r, &in, 256<<10); err != nil {
			return err
		}
		if p.ClientID == nil {
			in.Context = nil // the screen context comes from products only (R30)
		}
		m, err := a.PostMessage(r.Context(), p.UserID, &id, PostInput{Text: in.Text, Channel: p.Channel(), ClientID: p.ClientID,
			Attachments: in.AttachmentIDs, Context: in.Context, Skill: strings.TrimSpace(in.Skill)})
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusAccepted, map[string]any{"messageId": m.ID, "message": m})
		return nil
	}))
	r.Post("/messages/{id}/retry", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		if err := a.Retry(r.Context(), p, id); err != nil {
			return err
		}
		w.WriteHeader(http.StatusAccepted)
		return nil
	}))
	r.Post("/attachments", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		f, hdr, err := formFile(r, a.MaxFile)
		if err != nil {
			return err
		}
		defer f.Close()
		mime := r.MultipartForm.File["file"][0].Header.Get("Content-Type")
		id, err := a.storeAttachment(r.Context(), p.UserID, hdr, mime, f, -1)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, map[string]any{"attachmentId": id})
		return nil
	}))
	r.Get("/attachments/{id}", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var name, mime, key string
		if err := a.Pool.QueryRow(r.Context(), `SELECT file_name, mime_type, s3_key FROM attachments WHERE id = $1 AND user_id = $2`, id, p.UserID).
			Scan(&name, &mime, &key); err != nil {
			return apperr.NotFound("not_found", "attachment not found")
		}
		rc, err := a.S3.Get(r.Context(), key)
		if err != nil {
			return apperr.NotFound("not_found", "attachment not found")
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = io.Copy(w, rc)
		return nil
	}))
}

// convID reads {id}; "main" is the main conversation.
func convID(r *http.Request, a *API, p *domain.Principal) (uuid.UUID, error) {
	if chi.URLParam(r, "id") == "main" {
		c, err := a.Store.Main(r.Context(), p.UserID)
		if err != nil {
			return uuid.Nil, err
		}
		return c.ID, nil
	}
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return uuid.Nil, err
	}
	if _, err := a.Store.Get(r.Context(), p.UserID, id); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func formFile(r *http.Request, max int64) (io.ReadCloser, string, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, max+(1<<20))
	if err := r.ParseMultipartForm(8 << 20); err != nil {
		return nil, "", apperr.BadRequest("invalid_upload", "a multipart form with a file field is expected")
	}
	f, hdr, err := r.FormFile("file")
	if err != nil {
		return nil, "", apperr.BadRequest("invalid_upload", "the file field is missing")
	}
	return f, hdr.Filename, nil
}
