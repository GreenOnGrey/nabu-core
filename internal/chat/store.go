// Package chat is the conversation layer of personal agents
// (FTR.NAB.CMN-0001 R3–R6, R9–R10, R22, R30; tech §3.1–3.2, §5): the main
// conversation shared by every channel, topics, messages with channel marks
// and tool steps, attachments. A message from any channel is stored and put on
// nabu.inbound; the worker's turn engine (engine.go) answers it.
package chat

import (
	"context"
	"encoding/json"
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
	userID        uuid.UUID
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
	Model          *string         `json:"model"`
	ErrorClass     *string         `json:"errorClass"`
	ErrorText      *string         `json:"errorText"`
	Retryable      bool            `json:"retryable,omitempty"`
	ReplyTo        *uuid.UUID      `json:"replyTo"`
	CreatedAt      time.Time       `json:"createdAt"`
}

// Store is the conversation repository.
type Store struct{ Pool *pgxpool.Pool }

const convCols = `id, user_id, kind, title, last_message_at, archived_at, created_at`

func scanConv(row pgx.Row) (*Conversation, error) {
	var c Conversation
	err := row.Scan(&c.ID, &c.userID, &c.Kind, &c.Title, &c.LastMessageAt, &c.ArchivedAt, &c.CreatedAt)
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
	m.error_class, m.error_text, m.reply_to, m.created_at`

func scanMsg(row pgx.Row) (*Message, error) {
	var m Message
	var atts, steps, ctxb []byte
	if err := row.Scan(&m.ID, &m.ConversationID, &m.Role, &m.Text, &m.Channel, &m.Status, &atts, &steps, &ctxb, &m.Model,
		&m.ErrorClass, &m.ErrorText, &m.ReplyTo, &m.CreatedAt); err != nil {
		return nil, err
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
	ab, _ := json.Marshal(atts)
	var c any
	if len(msgCtx) > 0 {
		c = []byte(msgCtx)
	}
	m, err := scanMsg(s.Pool.QueryRow(ctx, `WITH m AS (INSERT INTO messages (conversation_id, role, text, channel, attachments, context)
		VALUES ($1,'user',$2,$3,$4,$5) RETURNING *) SELECT `+msgCols+` FROM m`, conv, text, channel, ab, c))
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

// History returns the latest messages of a conversation, oldest first.
func (s *Store) History(ctx context.Context, conv uuid.UUID, n int) ([]Message, error) {
	l, err := s.Messages(ctx, conv, httpx.Page{Limit: n})
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(l.Items))
	for i := len(l.Items) - 1; i >= 0; i-- {
		out = append(out, l.Items[i])
	}
	return out, nil
}

// LastUserChannel is the channel of the latest user message (the default
// delivery channel of tasks created in this conversation).
func (s *Store) LastUserChannel(ctx context.Context, conv uuid.UUID) string {
	var ch string
	_ = s.Pool.QueryRow(ctx, `SELECT channel FROM messages WHERE conversation_id = $1 AND role = 'user' ORDER BY created_at DESC LIMIT 1`, conv).Scan(&ch)
	return ch
}
