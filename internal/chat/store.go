// Package chat is the conversation layer of personal agents
// (FTR.NAB.CMN-0001 R3–R6, R9–R10, R22, R30; tech §3.1–3.2, §5): the main
// conversation shared by every channel, topics, messages with channel marks
// and tool steps, attachments. A message from any channel is stored and put on
// nabu.inbound; the worker's turn engine (engine.go) answers it.
package chat

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Conversation is the main conversation or a topic.
type Conversation struct {
	ID            uuid.UUID  `json:"id"`
	Kind          string     `json:"kind"` // main | topic
	Title         *string    `json:"title"`
	LastMessageAt *time.Time `json:"lastMessageAt"`
	ArchivedAt    *time.Time `json:"archivedAt"`
	CreatedAt     time.Time  `json:"createdAt"`
	// FTR.NAB.CMN-0002 tech §3: mail topics.
	Source      string `json:"source"` // chat | email | group
	UnreadCount int    `json:"unreadCount"`
	// WritesRequireConfirmation: tools that change data wait for the user (R9).
	WritesRequireConfirmation bool `json:"writesRequireConfirmation"`
	userID                    uuid.UUID
}

// ToolStep is a step of the agent's work with a tool (R30).
type ToolStep struct {
	ID      string `json:"id"`
	Server  string `json:"server"`
	Tool    string `json:"tool"`
	Summary string `json:"summary"`
	Status  string `json:"status"` // running | done | error
}

// Attachment is a file of a message.
type Attachment struct {
	ID       uuid.UUID `json:"id"`
	FileName string    `json:"fileName"`
	MimeType string    `json:"mimeType"`
	Size     int64     `json:"size"`
}

// Message is a message of a conversation.
type Message struct {
	ID             uuid.UUID       `json:"id"`
	ConversationID uuid.UUID       `json:"conversationId"`
	Role           string          `json:"role"`
	Text           string          `json:"text"`
	Channel        string          `json:"channel"`
	Status         string          `json:"status"`
	Attachments    []Attachment    `json:"attachments"`
	ToolSteps      []ToolStep      `json:"toolSteps"`
	Context        json.RawMessage `json:"context,omitempty"`
	// Card is the structured part of a message the web chat draws instead of
	// the text: the digest of commitments (FTR.NAB.CMN-0003 tech §3.5).
	Card json.RawMessage `json:"card,omitempty"`
	// Event is an event of the conversation shown as a divider (role
	// "event"): a reset, a compaction, a change of the model
	// (FTR.NAB.CHAT-0001 R1, R2, R16).
	Event json.RawMessage `json:"event,omitempty"`
	// Skill is the skill the user called with the message (R5).
	Skill *string `json:"skill"`
	// Voice: the text was recognized from a voice message (R21).
	Voice      bool       `json:"voice,omitempty"`
	Model      *string    `json:"model"`
	ErrorClass *string    `json:"errorClass"`
	ErrorText  *string    `json:"errorText"`
	Retryable  bool       `json:"retryable,omitempty"`
	ReplyTo    *uuid.UUID `json:"replyTo"`
	CreatedAt  time.Time  `json:"createdAt"`
}

// Store is the conversation repository.
type Store struct{ Pool *pgxpool.Pool }

const convCols = `id, user_id, kind, title, last_message_at, archived_at, created_at, source, unread_count, writes_require_confirmation`

func scanConv(row pgx.Row) (*Conversation, error) {
	var c Conversation
	err := row.Scan(&c.ID, &c.userID, &c.Kind, &c.Title, &c.LastMessageAt, &c.ArchivedAt, &c.CreatedAt, &c.Source, &c.UnreadCount, &c.WritesRequireConfirmation)
	return &c, err
}

// Main returns the main conversation of a user.
func (s *Store) Main(ctx context.Context, uid uuid.UUID) (*Conversation, error) {
	c, err := scanConv(s.Pool.QueryRow(ctx, `SELECT `+convCols+` FROM conversations WHERE user_id = $1 AND kind = 'main'`, uid))
	if postgres.IsNoRows(err) {
		if _, err := s.Pool.Exec(ctx, `INSERT INTO conversations (user_id, kind) VALUES ($1,'main') ON CONFLICT DO NOTHING`, uid); err != nil {
			return nil, err
		}
		return s.Main(ctx, uid)
	}
	return c, err
}

// Get loads a conversation of the user.
func (s *Store) Get(ctx context.Context, uid, id uuid.UUID) (*Conversation, error) {
	c, err := scanConv(s.Pool.QueryRow(ctx, `SELECT `+convCols+` FROM conversations WHERE id = $1 AND user_id = $2 AND kind <> 'task'`, id, uid))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "conversation not found")
	}
	return c, err
}

// List lists the main conversation and the topics (archived or active).
func (s *Store) List(ctx context.Context, uid uuid.UUID, archived bool) ([]Conversation, error) {
	if _, err := s.Main(ctx, uid); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+convCols+` FROM conversations WHERE user_id = $1 AND kind IN ('main','topic')
		AND (kind = 'main' AND NOT $2 OR kind = 'topic' AND (archived_at IS NOT NULL) = $2)
		ORDER BY kind = 'main' DESC, COALESCE(last_message_at, created_at) DESC`, uid, archived)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Conversation{}
	for rows.Next() {
		c, err := scanConv(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// CreateTopic creates a topic (R5).
func (s *Store) CreateTopic(ctx context.Context, uid uuid.UUID, title string) (*Conversation, error) {
	return scanConv(s.Pool.QueryRow(ctx, `INSERT INTO conversations (user_id, kind, title) VALUES ($1,'topic',$2) RETURNING `+convCols, uid, title))
}

// Patch renames or archives a topic; the main conversation is never archived.
func (s *Store) Patch(ctx context.Context, uid, id uuid.UUID, title *string, archived *bool) (*Conversation, error) {
	c, err := s.Get(ctx, uid, id)
	if err != nil {
		return nil, err
	}
	if c.Kind == "main" && (archived != nil && *archived) {
		return nil, apperr.Conflict("main_not_archivable", "the main conversation cannot be archived")
	}
	_, err = s.Pool.Exec(ctx, `UPDATE conversations SET title = COALESCE($3, title),
		archived_at = CASE WHEN $4::boolean IS NULL THEN archived_at WHEN $4 THEN COALESCE(archived_at, now()) ELSE NULL END
		WHERE id = $1 AND user_id = $2`, id, uid, title, archived)
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, uid, id)
}

// Read resets the unread counter of a conversation (UI-04).
func (s *Store) Read(ctx context.Context, uid, id uuid.UUID) error {
	_, err := s.Pool.Exec(ctx, `UPDATE conversations SET unread_count = 0 WHERE id = $1 AND user_id = $2`, id, uid)
	return err
}

// Source is the source of a conversation: chat, email or group.
func (s *Store) Source(ctx context.Context, id uuid.UUID) string {
	var src string
	_ = s.Pool.QueryRow(ctx, `SELECT source FROM conversations WHERE id = $1`, id).Scan(&src)
	return src
}

// ArchiveIdle archives topics without activity (R5: 14 days, CONV-06).
func (s *Store) ArchiveIdle(ctx context.Context, after time.Duration) ([]uuid.UUID, error) {
	rows, err := s.Pool.Query(ctx, `UPDATE conversations SET archived_at = now() WHERE kind = 'topic' AND archived_at IS NULL
		AND COALESCE(last_message_at, created_at) < now() - make_interval(secs => $1) RETURNING user_id`, after.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var u uuid.UUID
		if rows.Scan(&u) == nil {
			out = append(out, u)
		}
	}
	return out, rows.Err()
}

const msgCols = `m.id, m.conversation_id, m.role, m.text, m.channel, m.status, m.attachments, m.tool_steps, m.context, m.model,
	m.error_class, m.error_text, m.reply_to, m.created_at, m.card, m.event, m.skill, m.voice`

func scanMsg(row pgx.Row) (*Message, error) {
	var m Message
	var atts, steps, ctxb, card, ev []byte
	if err := row.Scan(&m.ID, &m.ConversationID, &m.Role, &m.Text, &m.Channel, &m.Status, &atts, &steps, &ctxb, &m.Model,
		&m.ErrorClass, &m.ErrorText, &m.ReplyTo, &m.CreatedAt, &card, &ev, &m.Skill, &m.Voice); err != nil {
		return nil, err
	}
	if len(card) > 0 {
		m.Card = card
	}
	if len(ev) > 0 {
		m.Event = ev
	}
	_ = json.Unmarshal(atts, &m.Attachments)
	_ = json.Unmarshal(steps, &m.ToolSteps)
	if len(ctxb) > 0 {
		m.Context = ctxb
	}
	if m.Attachments == nil {
		m.Attachments = []Attachment{}
	}
	if m.ToolSteps == nil {
		m.ToolSteps = []ToolStep{}
	}
	return &m, nil
}

// Messages lists messages, newest page first (the client reverses).
func (s *Store) Messages(ctx context.Context, conv uuid.UUID, page httpx.Page) (httpx.List[Message], error) {
	args := []any{conv, page.Limit + 1}
	cond := ""
	if page.Cursor != nil {
		cond = ` AND (m.created_at, m.id) < ($3, $4)`
		args = append(args, page.Cursor.T, page.Cursor.ID)
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+msgCols+` FROM messages m WHERE m.conversation_id = $1`+cond+
		` ORDER BY m.created_at DESC, m.id DESC LIMIT $2`, args...)
	if err != nil {
		return httpx.List[Message]{}, err
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		m, err := scanMsg(rows)
		if err != nil {
			return httpx.List[Message]{}, err
		}
		out = append(out, *m)
	}
	return httpx.NewList(out, page.Limit, func(m Message) (time.Time, string) { return m.CreatedAt, m.ID.String() }), rows.Err()
}

// Message loads a message of the user.
func (s *Store) Message(ctx context.Context, uid, id uuid.UUID) (*Message, error) {
	m, err := scanMsg(s.Pool.QueryRow(ctx, `SELECT `+msgCols+` FROM messages m JOIN conversations c ON c.id = m.conversation_id
		WHERE m.id = $1 AND c.user_id = $2`, id, uid))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "message not found")
	}
	return m, err
}

// AddUser stores a user message.
func (s *Store) AddUser(ctx context.Context, conv uuid.UUID, text, channel string, atts []Attachment, msgCtx json.RawMessage) (*Message, error) {
	return s.AddUserMessage(ctx, conv, UserMessage{Text: text, Channel: channel, Attachments: atts, Context: msgCtx})
}

// UserMessage is a user message to store.
type UserMessage struct {
	Text, Channel string
	Attachments   []Attachment
	Context       json.RawMessage
	Skill         string // FTR.NAB.CHAT-0001 R5
	Voice         bool   // R21
}

// AddUserMessage stores a user message with its skill and voice mark.
func (s *Store) AddUserMessage(ctx context.Context, conv uuid.UUID, u UserMessage) (*Message, error) {
	ab, _ := json.Marshal(u.Attachments)
	var c any
	if len(u.Context) > 0 {
		c = []byte(u.Context)
	}
	m, err := scanMsg(s.Pool.QueryRow(ctx, `WITH m AS (INSERT INTO messages (conversation_id, role, text, channel, attachments, context, skill, voice)
		VALUES ($1,'user',$2,$3,$4,$5,NULLIF($6,''),$7) RETURNING *) SELECT `+msgCols+` FROM m`, conv, u.Text, u.Channel, ab, c, u.Skill, u.Voice))
	if err == nil {
		_, _ = s.Pool.Exec(ctx, `UPDATE conversations SET last_message_at = now() WHERE id = $1`, conv)
	}
	return m, err
}

// AddAssistant creates the answer being written.
func (s *Store) AddAssistant(ctx context.Context, conv uuid.UUID, channel string, replyTo *uuid.UUID, model string, retryOf *uuid.UUID) (*Message, error) {
	return scanMsg(s.Pool.QueryRow(ctx, `WITH m AS (INSERT INTO messages (conversation_id, role, text, channel, status, reply_to, model, retry_of)
		VALUES ($1,'assistant','',$2,'streaming',$3,NULLIF($4,''),$5) RETURNING *) SELECT `+msgCols+` FROM m`, conv, channel, replyTo, model, retryOf))
}

// MaxAgentFiles bounds the files the agent sends with one answer or run
// (FTR.NAB.CHAT-0001 R33).
const MaxAgentFiles = 10

// Errors of the files of the agent.
var (
	ErrNoAnswer       = errors.New("no answer is being written in the conversation")
	ErrTooManyFiles   = errors.New("the answer already has the most files")
	ErrAttachmentGone = errors.New("the attachment is gone")
)

// AttachToAnswer adds a file the agent sends to the answer being written in
// the conversation (FTR.NAB.CHAT-0001 R33).
func (s *Store) AttachToAnswer(ctx context.Context, conv uuid.UUID, at Attachment) error {
	return s.appendFile(ctx, at, `SELECT id, jsonb_array_length(attachments) FROM messages
		WHERE conversation_id = $1 AND role = 'assistant' AND status = 'streaming'
		ORDER BY created_at DESC LIMIT 1 FOR UPDATE`, conv, `UPDATE messages SET attachments = attachments || $2::jsonb WHERE id = $1`)
}

// AttachToRun adds a file the agent sends during a run of a task; the file
// waits for the result of the run (FTR.NAB.CHAT-0001 R33).
func (s *Store) AttachToRun(ctx context.Context, task uuid.UUID, at Attachment) error {
	return s.appendFile(ctx, at, `SELECT id, jsonb_array_length(attachments) FROM task_runs
		WHERE task_id = $1 AND status = 'running' ORDER BY started_at DESC LIMIT 1 FOR UPDATE`, task,
		`UPDATE task_runs SET attachments = attachments || $2::jsonb WHERE id = $1`)
}

// appendFile locks the row that collects the files, checks their number and
// appends the file.
func (s *Store) appendFile(ctx context.Context, at Attachment, lock string, key uuid.UUID, update string) error {
	return postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var id uuid.UUID
		var n int
		if err := tx.QueryRow(ctx, lock, key).Scan(&id, &n); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNoAnswer
			}
			return err
		}
		if n >= MaxAgentFiles {
			return ErrTooManyFiles
		}
		b, _ := json.Marshal([]Attachment{at})
		_, err := tx.Exec(ctx, update, id, b)
		return err
	})
}

// TakeRunFiles returns the files of a run and forgets them: they go to its
// result, or are dropped when the run fails or starts again (FTR.NAB.CHAT-0001 R33).
func (s *Store) TakeRunFiles(ctx context.Context, run uuid.UUID) ([]Attachment, error) {
	var b []byte
	err := s.Pool.QueryRow(ctx, `UPDATE task_runs r SET attachments = '[]' FROM task_runs o
		WHERE r.id = $1 AND o.id = r.id RETURNING o.attachments`, run).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	var out []Attachment
	_ = json.Unmarshal(b, &out)
	return out, err
}

// SetAttachments sets the files of a message (the result of a task, FTR.NAB.CHAT-0001 R33).
func (s *Store) SetAttachments(ctx context.Context, id uuid.UUID, atts []Attachment) error {
	b, _ := json.Marshal(atts)
	_, err := s.Pool.Exec(ctx, `UPDATE messages SET attachments = $2 WHERE id = $1`, id, b)
	return err
}

// Attachment reads an attachment of the user and the S3 key of its content;
// a missing one is ErrAttachmentGone.
func (s *Store) Attachment(ctx context.Context, uid, id uuid.UUID) (Attachment, string, error) {
	a := Attachment{ID: id}
	var key string
	err := s.Pool.QueryRow(ctx, `SELECT file_name, mime_type, size_bytes, s3_key FROM attachments WHERE id = $1 AND user_id = $2`, id, uid).
		Scan(&a.FileName, &a.MimeType, &a.Size, &key)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, "", ErrAttachmentGone
	}
	return a, key, err
}

// DeleteAttachment removes the row of an attachment that reached no message
// and returns the S3 key of its content for the caller to delete.
func (s *Store) DeleteAttachment(ctx context.Context, uid, id uuid.UUID) (string, error) {
	var key string
	err := s.Pool.QueryRow(ctx, `DELETE FROM attachments WHERE id = $1 AND user_id = $2 RETURNING s3_key`, id, uid).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrAttachmentGone
	}
	return key, err
}

// Finish stores the final state of an answer.
func (s *Store) Finish(ctx context.Context, id uuid.UUID, text, status string, steps []ToolStep, errClass, errText string) (*Message, error) {
	sb, _ := json.Marshal(steps)
	m, err := scanMsg(s.Pool.QueryRow(ctx, `WITH m AS (UPDATE messages SET text = $2, status = $3, tool_steps = $4,
		error_class = NULLIF($5,''), error_text = NULLIF($6,'') WHERE id = $1 RETURNING *) SELECT `+msgCols+` FROM m`,
		id, text, status, sb, errClass, errText))
	if err == nil {
		_, _ = s.Pool.Exec(ctx, `UPDATE conversations SET last_message_at = now() WHERE id = $1`, m.ConversationID)
	}
	return m, err
}

// SetModel records the model that writes an answer (FTR.NAB.CHAT-0001 R18).
func (s *Store) SetModel(ctx context.Context, id uuid.UUID, model string) {
	_, _ = s.Pool.Exec(ctx, `UPDATE messages SET model = NULLIF($2,'') WHERE id = $1`, id, model)
}

// SetCard attaches a card to a message (the digest of commitments).
func (s *Store) SetCard(ctx context.Context, id uuid.UUID, card json.RawMessage) error {
	_, err := s.Pool.Exec(ctx, `UPDATE messages SET card = $2 WHERE id = $1`, id, []byte(card))
	return err
}

// History returns the latest messages of a conversation that the agent
// knows, oldest first: the messages of the user and the answers after the
// last reset of the context (FTR.NAB.CHAT-0001 R1); events are not history.
func (s *Store) History(ctx context.Context, conv uuid.UUID, n int) ([]Message, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+msgCols+` FROM messages m WHERE m.conversation_id = $1 AND m.role IN ('user','assistant')
		AND m.created_at > COALESCE((SELECT max(e.created_at) FROM messages e WHERE e.conversation_id = $1 AND e.role = 'event'
			AND e.event->>'kind' = 'reset'), '-infinity'::timestamptz)
		ORDER BY m.created_at DESC, m.id DESC LIMIT $2`, conv, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		m, err := scanMsg(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *m)
	}
	slices.Reverse(out)
	return out, rows.Err()
}

// Event kinds of a conversation (FTR.NAB.CHAT-0001 tech §3).
const (
	EventReset   = "reset"
	EventCompact = "compact"
	EventModel   = "model"
)

// Event is the payload of an event message.
type Event struct {
	Kind         string     `json:"kind"`
	Auto         bool       `json:"auto,omitempty"`
	TokensBefore int64      `json:"tokensBefore,omitempty"`
	TokensAfter  int64      `json:"tokensAfter,omitempty"`
	Summary      string     `json:"summary,omitempty"`
	ConnectionID *uuid.UUID `json:"connectionId,omitempty"`
	Model        string     `json:"model,omitempty"`
	Name         string     `json:"name,omitempty"`
}

// AddEvent stores an event of a conversation.
func (s *Store) AddEvent(ctx context.Context, conv uuid.UUID, channel string, ev Event, at ...EventAt) (*Message, error) {
	b, _ := json.Marshal(ev)
	var o EventAt
	if len(at) > 0 {
		o = at[0]
	}
	m, err := scanMsg(s.Pool.QueryRow(ctx, `WITH m AS (INSERT INTO messages (id, conversation_id, role, text, channel, event, created_at)
		VALUES (COALESCE($4, gen_random_uuid()), $1, 'event', '', $2, $3,
			COALESCE((SELECT created_at - interval '1 microsecond' FROM messages WHERE id = $5), now()))
		ON CONFLICT (id) DO NOTHING RETURNING *) SELECT `+msgCols+` FROM m`, conv, channel, b, o.ID, o.Before))
	if postgres.IsNoRows(err) {
		return nil, ErrEventExists
	}
	return m, err
}

// EventAt places an event: ID makes it idempotent (a command delivered twice
// writes one event); Before puts it right before a message.
type EventAt struct {
	ID, Before *uuid.UUID
}

// ErrEventExists: the event with this id is written already.
var ErrEventExists = errors.New("the event exists")

// PreviousModel is the model of the last answer of the conversation before
// the message (R16: a change of the model is shown where it takes effect).
func (s *Store) PreviousModel(ctx context.Context, conv, msg uuid.UUID) string {
	var m string
	_ = s.Pool.QueryRow(ctx, `SELECT model FROM messages WHERE conversation_id = $1 AND role = 'assistant' AND id <> $2 AND model IS NOT NULL
		ORDER BY created_at DESC LIMIT 1`, conv, msg).Scan(&m)
	return m
}

// Busy reports whether an answer is being written in the conversation (R1:
// a reset waits for it). An answer left streaming for longer than stale is
// not counted.
func (s *Store) Busy(ctx context.Context, conv uuid.UUID, stale time.Duration) bool {
	var busy bool
	_ = s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM messages WHERE conversation_id = $1 AND role = 'assistant'
		AND status IN ('pending','streaming') AND created_at > now() - make_interval(secs => $2))`, conv, stale.Seconds()).Scan(&busy)
	return busy
}

// ContextFill is the fill of the context window of a conversation (R3).
type ContextFill struct {
	Tokens int64 `json:"tokens"`
	Window int64 `json:"window"`
	// Empty: nothing was said since the conversation began or was reset.
	Empty bool `json:"-"`
}

// Context returns the fill of the open session of a conversation; a
// conversation without a session (new, or after a reset) is empty.
func (s *Store) Context(ctx context.Context, conv uuid.UUID) ContextFill {
	var f ContextFill
	_ = s.Pool.QueryRow(ctx, `SELECT context_tokens, context_window FROM harness_sessions WHERE conversation_id = $1 AND closed_at IS NULL`, conv).
		Scan(&f.Tokens, &f.Window)
	_ = s.Pool.QueryRow(ctx, `SELECT NOT EXISTS (SELECT 1 FROM messages m WHERE m.conversation_id = $1 AND m.role IN ('user','assistant')
		AND m.created_at > COALESCE((SELECT max(created_at) FROM messages WHERE conversation_id = $1 AND role = 'event' AND event->>'kind' = 'reset'), '-infinity'))`,
		conv).Scan(&f.Empty)
	return f
}

// LastUserChannel is the channel of the latest user message (the default
// delivery channel of tasks created in this conversation).
func (s *Store) LastUserChannel(ctx context.Context, conv uuid.UUID) string {
	var ch string
	_ = s.Pool.QueryRow(ctx, `SELECT channel FROM messages WHERE conversation_id = $1 AND role = 'user' ORDER BY created_at DESC LIMIT 1`, conv).Scan(&ch)
	return ch
}
