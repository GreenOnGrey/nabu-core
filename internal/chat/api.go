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
}

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
	text = strings.TrimSpace(text)
	if text == "" && len(attIDs) == 0 {
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
	m, err := a.Store.AddUser(ctx, c.ID, text, channel, atts, msgCtx)
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
// conversation, letters to their topic (FTR.NAB.CMN-0002 R3).
func (a *API) Receive(ctx context.Context, in channels.Inbound) error {
	_, err := a.Post(ctx, in.UserID, in.Conversation, in.Text, in.Channel, nil, in.Attachments, in.Context)
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

// AgentView is GET /agent.
type AgentView struct {
	Name            string                  `json:"name"`
	Tone            string                  `json:"tone"`
	Model           *models.AvailableModel  `json:"model"`
	AvailableModels []models.AvailableModel `json:"availableModels"`
}

func (a *API) agentView(ctx context.Context, uid uuid.UUID) (*AgentView, error) {
	ag, err := a.Users.GetAgent(ctx, uid)
	if err != nil {
		return nil, err
	}
	avail, err := a.Models.AvailableForUsers(ctx)
	if err != nil {
		return nil, err
	}
	v := &AgentView{Name: ag.Name, Tone: ag.Tone, AvailableModels: avail}
	pm, _ := a.Models.Personal(ctx)
	for i, m := range avail {
		if ag.ConnectionID != nil && ag.Model != nil && m.ConnectionID == *ag.ConnectionID && m.Model == *ag.Model {
			v.Model = &avail[i]
		}
	}
	if v.Model == nil && pm.Default != nil {
		for i, m := range avail {
			if m.ConnectionID == pm.Default.ConnectionID && m.Model == pm.Default.Model {
				v.Model = &avail[i]
			}
		}
	}
	return v, nil
}

// PatchAgent changes name, tone and model; open sessions get them with the
// next message (R3).
func (a *API) PatchAgent(ctx context.Context, uid uuid.UUID, name, tone *string, model *models.Choice) (*AgentView, error) {
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
		avail, err := a.Models.AvailableForUsers(ctx)
		if err != nil {
			return nil, err
		}
		ok := false
		for _, m := range avail {
			if m.ConnectionID == model.ConnectionID && m.Model == model.Model {
				ok = true
			}
		}
		if !ok { // MOD-03
			return nil, apperr.Unprocessable("model_not_available", "the model is not in the list of available models").With("field", "model")
		}
		conn, mid = &model.ConnectionID, &model.Model
	}
	if err := a.Users.SetAgent(ctx, uid, name, tone, conn, mid); err != nil {
		return nil, err
	}
	v, err := a.agentView(ctx, uid)
	if err == nil {
		a.Events.Publish(ctx, events.Event{Type: events.AgentUpdated, UserID: &uid, Data: v})
	}
	return v, err
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
			if a.Whisper == nil {
				return apperr.Unavailable("transcription_unavailable", "voice input is not configured")
			}
			f, hdr, err := formFile(r, 25<<20)
			if err != nil {
				return err
			}
			defer f.Close()
			text, err := a.Whisper.Transcribe(r.Context(), f, hdr)
			if err != nil {
				slog.WarnContext(r.Context(), "transcription failed", "err", err)
				return apperr.Unavailable("transcription_failed", "the voice message could not be recognized")
			}
			httpx.JSON(w, 200, map[string]string{"text": strings.TrimSpace(text)})
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
			Name  *string        `json:"name"`
			Tone  *string        `json:"tone"`
			Model *models.Choice `json:"model"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		v, err := a.PatchAgent(r.Context(), p.UserID, in.Name, in.Tone, in.Model)
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
		}
		if err := httpx.DecodeMax(r, &in, 256<<10); err != nil {
			return err
		}
		if p.ClientID == nil {
			in.Context = nil // the screen context comes from products only (R30)
		}
		m, err := a.Post(r.Context(), p.UserID, &id, in.Text, p.Channel(), p.ClientID, in.AttachmentIDs, in.Context)
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
