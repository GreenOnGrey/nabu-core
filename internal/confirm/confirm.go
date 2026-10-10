// Package confirm keeps calls of tools that change data until the user
// confirms them (FTR.NAB.CMN-0002 R9; arch §3.3; tech §6). In a conversation
// with writes_require_confirmation (mail topics) such a call is stored
// instead of executed; the user approves or rejects it in the web chat; an
// approved call is executed with the current credentials of the user and the
// agent gets the result with the next turn.
//
// Deviation from arch §3.3 (tech §13): the calls are held in the MCP proxy and
// the built-in MCP of api, through which every session already reaches its
// servers, not in a separate proxy of the agent operator.
package confirm

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// SSE events (tech §3).
const (
	Created  = "confirmation.created"
	Resolved = "confirmation.resolved"
)

// Builtin is the server name of the built-in MCP.
const Builtin = "nabu"

// Pending is a call waiting for the user.
type Pending struct {
	ID             uuid.UUID  `json:"id"`
	ConversationID uuid.UUID  `json:"conversationId"`
	Server         string     `json:"server"`
	Tool           string     `json:"tool"`
	Summary        string     `json:"summary"`
	ArgsPreview    string     `json:"argsPreview"`
	Status         string     `json:"status"`
	CreatedAt      time.Time  `json:"createdAt"`
	ExpiresAt      time.Time  `json:"expiresAt"`
	ItemID         *uuid.UUID `json:"-"`
	UserID         uuid.UUID  `json:"-"`
	args           json.RawMessage
}

// Executor runs a confirmed call: the built-in tool or the MCP server of a
// catalog item with the credentials of the user.
type Executor func(ctx context.Context, p *Pending, args json.RawMessage) (result string, isError bool, err error)

// FollowUp gives the agent the outcome with the next turn of the conversation.
type FollowUp func(ctx context.Context, uid, conv uuid.UUID, text string, meta json.RawMessage) error

// Service keeps confirmations.
type Service struct {
	Pool     *pgxpool.Pool
	Box      *crypto.Box
	Events   events.Publisher
	TTL      time.Duration // CONFIRMATION_TTL
	Execute  Executor
	FollowUp FollowUp
	now      func() time.Time
}

func (s *Service) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Required reports whether writes of the conversation need a confirmation.
func (s *Service) Required(ctx context.Context, conv uuid.UUID) bool {
	var ok bool
	_ = s.Pool.QueryRow(ctx, `SELECT writes_require_confirmation FROM conversations WHERE id = $1`, conv).Scan(&ok)
	return ok
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// Hold stores a call instead of executing it (ML-17, ML-20) and answers the
// agent that the action waits for the user; held is false when the
// conversation needs no confirmation.
func (s *Service) Hold(ctx context.Context, conv, uid uuid.UUID, item *uuid.UUID, server, tool string, args json.RawMessage) (string, bool) {
	if conv == uuid.Nil || !s.Required(ctx, conv) {
		return "", false
	}
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	enc, err := s.Box.Seal(string(args))
	if err != nil {
		return "The action could not be queued for confirmation.", true
	}
	var id uuid.UUID
	summary := server + ": " + tool
	err = s.Pool.QueryRow(ctx, `INSERT INTO pending_confirmations (conversation_id, user_id, item_id, server, tool, args_enc, summary, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`, conv, uid, item, server, tool, enc, summary, s.clock().Add(s.TTL)).Scan(&id)
	if err != nil {
		return "The action could not be queued for confirmation.", true
	}
	s.gauge(ctx)
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: Created, UserID: &uid, Data: map[string]any{"id": id, "conversationId": conv}})
	}
	return "The action is waiting for the user's confirmation (id " + id.String() + "). It was NOT performed. " +
		"Tell the user what you want to do and that they confirm it in Nabu; do not repeat the call.", true
}

func (s *Service) gauge(ctx context.Context) {
	var n int
	if s.Pool.QueryRow(ctx, `SELECT count(*) FROM pending_confirmations WHERE status = 'pending'`).Scan(&n) == nil {
		metrics.PendingConfirmations.Set(float64(n))
	}
}

const cols = `id, conversation_id, user_id, item_id, server, tool, summary, args_enc, status, created_at, expires_at`

func (s *Service) scan(row pgx.Row) (*Pending, error) {
	var p Pending
	var enc []byte
	if err := row.Scan(&p.ID, &p.ConversationID, &p.UserID, &p.ItemID, &p.Server, &p.Tool, &p.Summary, &enc, &p.Status, &p.CreatedAt, &p.ExpiresAt); err != nil {
		return nil, err
	}
	if pt, err := s.Box.Open(enc); err == nil {
		p.args = json.RawMessage(pt)
		p.ArgsPreview = clip(pt, 600)
	}
	return &p, nil
}

// List lists the pending confirmations of a conversation of the user.
func (s *Service) List(ctx context.Context, uid uuid.UUID, conv *uuid.UUID) ([]Pending, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+cols+` FROM pending_confirmations WHERE user_id = $1 AND status = 'pending' AND expires_at > $3
		AND ($2::uuid IS NULL OR conversation_id = $2) ORDER BY created_at`, uid, conv, s.clock())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Pending{}
	for rows.Next() {
		p, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

// PendingSince reports whether calls of the conversation were held since t
// (the letter gets the link «Confirm in Nabu»).
func (s *Service) PendingSince(ctx context.Context, conv uuid.UUID, t time.Time) bool {
	var ok bool
	_ = s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pending_confirmations WHERE conversation_id = $1 AND status = 'pending' AND created_at >= $2)`,
		conv, t).Scan(&ok)
	return ok
}

// ErrExpired is confirmation_expired (ML-19).
func ErrExpired() error {
	return apperr.Gone("confirmation_expired", "the confirmation expired; ask the agent again")
}

// take moves a pending confirmation of the user to the status.
func (s *Service) take(ctx context.Context, uid, id uuid.UUID, status string) (*Pending, error) {
	var p *Pending
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		p, err = s.scan(tx.QueryRow(ctx, `SELECT `+cols+` FROM pending_confirmations WHERE id = $1 AND user_id = $2 FOR UPDATE`, id, uid))
		if postgres.IsNoRows(err) {
			return apperr.NotFound("not_found", "confirmation not found")
		}
		if err != nil {
			return err
		}
		if p.Status == "expired" || (p.Status == "pending" && !p.ExpiresAt.After(s.clock())) {
			_, _ = tx.Exec(ctx, `UPDATE pending_confirmations SET status = 'expired', resolved_at = now() WHERE id = $1 AND status = 'pending'`, id)
			p.Status = "expired"
			return nil
		}
		if p.Status != "pending" {
			return apperr.Conflict("confirmation_resolved", "the confirmation is already resolved")
		}
		_, err = tx.Exec(ctx, `UPDATE pending_confirmations SET status = $2, resolved_at = now() WHERE id = $1`, id, status)
		p.Status = status
		return err
	})
	if err != nil {
		return nil, err
	}
	if p.Status == "expired" {
		return nil, ErrExpired()
	}
	return p, nil
}

func (s *Service) resolved(ctx context.Context, p *Pending) {
	s.gauge(ctx)
	if s.Events != nil {
		uid := p.UserID
		s.Events.Publish(ctx, events.Event{Type: Resolved, UserID: &uid, Data: map[string]any{"id": p.ID, "conversationId": p.ConversationID, "status": p.Status}})
	}
}

// Approve executes the call and passes the result to the agent (ML-18).
func (s *Service) Approve(ctx context.Context, uid, id uuid.UUID) (*Pending, error) {
	p, err := s.take(ctx, uid, id, "approved")
	if err != nil {
		return nil, err
	}
	result, isErr, err := s.Execute(ctx, p, p.args)
	if err != nil {
		result, isErr = err.Error(), true
	}
	_, _ = s.Pool.Exec(ctx, `UPDATE pending_confirmations SET result = $2 WHERE id = $1`, p.ID, clip(result, 4000))
	s.resolved(ctx, p)
	text := "[The user confirmed the action " + p.Summary + ". Result: " + clip(result, 8000) + "]"
	if isErr {
		text = "[The user confirmed the action " + p.Summary + ", but it failed: " + clip(result, 4000) + "]"
	}
	return p, s.follow(ctx, p, text, "approved")
}

// Reject tells the agent that the user refused.
func (s *Service) Reject(ctx context.Context, uid, id uuid.UUID) (*Pending, error) {
	p, err := s.take(ctx, uid, id, "rejected")
	if err != nil {
		return nil, err
	}
	s.resolved(ctx, p)
	return p, s.follow(ctx, p, "[The user rejected the action "+p.Summary+". Do not perform it.]", "rejected")
}

func (s *Service) follow(ctx context.Context, p *Pending, text, status string) error {
	if s.FollowUp == nil {
		return nil
	}
	meta, _ := json.Marshal(map[string]any{"confirmation": map[string]any{"id": p.ID, "status": status, "summary": p.Summary}})
	return s.FollowUp(ctx, p.UserID, p.ConversationID, text, meta)
}

// Expire marks confirmations older than the TTL.
func (s *Service) Expire(ctx context.Context) {
	_, _ = s.Pool.Exec(ctx, `UPDATE pending_confirmations SET status = 'expired', resolved_at = now() WHERE status = 'pending' AND expires_at < now()`)
	s.gauge(ctx)
}

// Routes mounts /confirmations of the site (tech §3).
func (s *Service) Routes(r chi.Router) {
	h := httpx.Handler
	r.Get("/confirmations", h(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		var conv *uuid.UUID
		if v := r.URL.Query().Get("conversationId"); v != "" {
			id, err := uuid.Parse(v)
			if err != nil {
				return apperr.BadRequest("invalid_id", "conversationId is not a UUID")
			}
			conv = &id
		}
		l, err := s.List(r.Context(), p.UserID, conv)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	act := func(fn func(ctx context.Context, uid, id uuid.UUID) (*Pending, error)) http.HandlerFunc {
		return h(func(w http.ResponseWriter, r *http.Request) error {
			p, err := httpx.MustPrincipal(r)
			if err != nil {
				return err
			}
			id, err := httpx.ParamUUID(r, "id")
			if err != nil {
				return err
			}
			c, err := fn(r.Context(), p.UserID, id)
			if err != nil {
				return err
			}
			httpx.JSON(w, 200, c)
			return nil
		})
	}
	r.Post("/confirmations/{id}:approve", act(s.Approve))
	r.Post("/confirmations/{id}:reject", act(s.Reject))
}
