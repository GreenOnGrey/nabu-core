// Package groups is the group agents of Telegram and VK Teams chats
// (FTR.NAB.CMN-0002 R16–R17; arch §6; tech §2.4). A group agent is a
// separate agent of the chat: an owner, a name, a tone, a model, the skills
// chosen by the owner and platform connections only; its conversation,
// memory and space belong to a technical user of the group, not to members.
package groups

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// States of a group agent.
const (
	Active   = "active"
	Disabled = "disabled"
	Removed  = "removed"
)

// Agent is a group agent.
type Agent struct {
	ID           uuid.UUID  `json:"id"`
	Channel      string     `json:"channel"`
	ChatID       string     `json:"chatId"`
	ChatTitle    string     `json:"chatTitle"`
	MembersCount *int       `json:"membersCount"`
	Owner        *Person    `json:"owner"`
	Name         string     `json:"name"`
	Tone         string     `json:"tone"`
	ConnectionID *uuid.UUID `json:"connectionId"`
	Model        *string    `json:"model"`
	Skills       []string   `json:"skills"`
	Status       string     `json:"status"`
	DataUntil    *time.Time `json:"dataUntil"`
	CostMonth    float64    `json:"costMonth"`
	CreatedAt    time.Time  `json:"createdAt"`
	DataUserID   *uuid.UUID `json:"-"`
	OwnerID      *uuid.UUID `json:"-"`
}

// Person is the owner.
type Person struct {
	ID    uuid.UUID `json:"id"`
	Email string    `json:"email"`
	Name  string    `json:"name"`
}

// Service manages group agents.
type Service struct {
	Pool     *pgxpool.Pool
	Registry *channels.Registry
	Users    *users.Repo
	Inbox    channels.Inbox
	// Retention is the retention of the data of a removed agent in days (the archive retention).
	RetentionDays func(ctx context.Context) int
	// DeleteData deletes the objects of the technical user in S3.
	DeleteData func(ctx context.Context, uid uuid.UUID) error
}

const cols = `g.id, g.channel, g.external_chat_id, COALESCE(g.chat_title,''), g.members_count, g.owner_id, g.data_user_id, g.name, g.tone,
	g.model_connection_id, g.model, g.skills, g.status, g.data_until, g.created_at, o.email, COALESCE(o.name,''),
	COALESCE((SELECT sum(cost_usd) FROM usage u WHERE u.group_agent_id = g.id AND u.created_at > date_trunc('month', now())), 0)::float8`

const from = ` FROM group_agents g LEFT JOIN users o ON o.id = g.owner_id`

func scan(row pgx.Row) (*Agent, error) {
	var a Agent
	var email *string
	var name string
	err := row.Scan(&a.ID, &a.Channel, &a.ChatID, &a.ChatTitle, &a.MembersCount, &a.OwnerID, &a.DataUserID, &a.Name, &a.Tone,
		&a.ConnectionID, &a.Model, &a.Skills, &a.Status, &a.DataUntil, &a.CreatedAt, &email, &name, &a.CostMonth)
	if err != nil {
		return nil, err
	}
	if a.OwnerID != nil && email != nil {
		a.Owner = &Person{ID: *a.OwnerID, Email: *email, Name: name}
	}
	if a.Skills == nil {
		a.Skills = []string{}
	}
	return &a, nil
}

// Get loads a group agent.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*Agent, error) {
	a, err := scan(s.Pool.QueryRow(ctx, `SELECT `+cols+from+` WHERE g.id = $1`, id))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "group agent not found")
	}
	return a, err
}

// ByChat loads the agent of a chat or nil.
func (s *Service) ByChat(ctx context.Context, channel, chatID string) (*Agent, error) {
	a, err := scan(s.Pool.QueryRow(ctx, `SELECT `+cols+from+` WHERE g.channel = $1 AND g.external_chat_id = $2`, channel, chatID))
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return a, err
}

// ByDataUser loads the agent whose data belong to the technical user, or nil.
func (s *Service) ByDataUser(ctx context.Context, uid uuid.UUID) (*Agent, error) {
	a, err := scan(s.Pool.QueryRow(ctx, `SELECT `+cols+from+` WHERE g.data_user_id = $1`, uid))
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return a, err
}

func (s *Service) retention(ctx context.Context) time.Duration {
	days := 180
	if s.RetentionDays != nil {
		days = s.RetentionDays(ctx)
	}
	return time.Duration(days) * 24 * time.Hour
}

// Added implements channels.Groups (GR-01, GR-02, GR-10).
func (s *Service) Added(ctx context.Context, ev channels.GroupEvent) (string, bool) {
	lang := ev.Language
	if ev.AdderID != uuid.Nil {
		lang = s.Users.Language(ctx, ev.AdderID)
	}
	switch {
	case !s.Registry.GroupsEnabled(ctx, ev.Channel):
		return channels.T(lang, "group.disabled"), false
	case ev.AdderID == uuid.Nil:
		return channels.T(lang, "group.not_linked"), false
	case !s.Registry.Open(ctx, ev.AdderID, ev.Channel):
		return channels.T(lang, "group.unavailable"), false
	}
	var members *int
	if ev.Members > 0 {
		members = &ev.Members
	}
	var owner string
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var id uuid.UUID
		var ownerID, dataUser *uuid.UUID
		var status string
		var until *time.Time
		err := tx.QueryRow(ctx, `SELECT id, owner_id, data_user_id, status, data_until FROM group_agents WHERE channel = $1 AND external_chat_id = $2 FOR UPDATE`,
			ev.Channel, ev.ChatID).Scan(&id, &ownerID, &dataUser, &status, &until)
		switch {
		case postgres.IsNoRows(err):
			id = uuid.New()
			uid, err := s.Users.CreateGroupUser(ctx, tx, id, ev.Title, lang)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO group_agents (id, channel, external_chat_id, chat_title, members_count, owner_id, data_user_id)
				VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7)`, id, ev.Channel, ev.ChatID, ev.Title, members, ev.AdderID, uid)
			if err != nil {
				return err
			}
		case err != nil:
			return err
		case status == Active:
			// added again while active: nothing changes
		case ownerID != nil && *ownerID == ev.AdderID && until != nil && until.After(time.Now()) && dataUser != nil:
			// R17: the same owner brings the bot back — the data return
			if _, err := tx.Exec(ctx, `UPDATE group_agents SET status = 'active', data_until = NULL, chat_title = COALESCE(NULLIF($2,''), chat_title),
				members_count = COALESCE($3, members_count) WHERE id = $1`, id, ev.Title, members); err != nil {
				return err
			}
		default:
			// another owner or expired data: a new agent of the chat
			if dataUser != nil {
				if s.DeleteData != nil {
					if err := s.DeleteData(ctx, *dataUser); err != nil {
						return err
					}
				}
				if _, err := tx.Exec(ctx, `DELETE FROM users WHERE id = $1 AND created_via = 'group'`, *dataUser); err != nil {
					return err
				}
			}
			uid, err := s.Users.CreateGroupUser(ctx, tx, id, ev.Title, lang)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE group_agents SET status = 'active', data_until = NULL, owner_id = $2, data_user_id = $3,
				name = 'Nabu', tone = 'business', model_connection_id = NULL, model = NULL, skills = '{}', chat_title = COALESCE(NULLIF($4,''), chat_title),
				members_count = COALESCE($5, members_count), created_at = now() WHERE id = $1`, id, ev.AdderID, uid, ev.Title, members); err != nil {
				return err
			}
		}
		return tx.QueryRow(ctx, `SELECT COALESCE(NULLIF(name,''), email) FROM users WHERE id = $1`, ev.AdderID).Scan(&owner)
	})
	if err != nil {
		slog.ErrorContext(ctx, "group agent added", "err", err)
		return "", false
	}
	return channels.T(lang, "group.welcome", owner), true
}

// Removed implements channels.Groups: the data are kept for the archive
// retention (R17).
func (s *Service) Removed(ctx context.Context, channel, chatID string) {
	_, err := s.Pool.Exec(ctx, `UPDATE group_agents SET status = 'removed', data_until = $3 WHERE channel = $1 AND external_chat_id = $2`,
		channel, chatID, time.Now().Add(s.retention(ctx)))
	if err != nil {
		slog.ErrorContext(ctx, "group agent removed", "err", err)
	}
}

// Message implements channels.Groups: an address of a member with a Nabu
// account and access to the channel goes to the agent (GR-04).
func (s *Service) Message(ctx context.Context, m channels.GroupMessage) string {
	a, err := s.ByChat(ctx, m.Channel, m.ChatID)
	if err != nil {
		slog.ErrorContext(ctx, "group agent lookup", "err", err)
		return ""
	}
	lang := m.Language
	if m.AuthorID != uuid.Nil {
		lang = s.Users.Language(ctx, m.AuthorID)
	}
	if a == nil || a.Status != Active || a.DataUserID == nil {
		return channels.T(lang, "group.inactive")
	}
	if m.AuthorID == uuid.Nil || !s.Registry.Open(ctx, m.AuthorID, m.Channel) {
		return channels.T(lang, "group.not_member")
	}
	if m.Text == "" && m.Quote == "" {
		return ""
	}
	author, _ := s.Users.Get(ctx, m.AuthorID)
	email, name := "", m.AuthorName
	if author != nil {
		email = author.Email
		if name == "" {
			name = author.Name
		}
	}
	meta, _ := json.Marshal(map[string]any{"group": map[string]any{"id": a.ID, "chatTitle": a.ChatTitle, "authorId": m.AuthorID,
		"authorName": name, "authorEmail": email, "quote": m.Quote}})
	if m.Title != "" && m.Title != a.ChatTitle {
		_, _ = s.Pool.Exec(ctx, `UPDATE group_agents SET chat_title = $2 WHERE id = $1`, a.ID, m.Title)
	}
	text := m.Text
	if text == "" {
		text = "(see the quote)"
	}
	if err := s.Inbox.Receive(ctx, channels.Inbound{UserID: *a.DataUserID, Channel: m.Channel, Text: text, Context: meta}); err != nil {
		slog.ErrorContext(ctx, "group inbound", "err", err)
		return channels.T(lang, "tg.not_delivered")
	}
	return ""
}

// OwnerArchived hands the agents of an archived owner to an administrator
// or switches them off (R18, AR-03).
func (s *Service) OwnerArchived(ctx context.Context, uid uuid.UUID, mode string, to *uuid.UUID) error {
	if mode == "transfer" && to != nil {
		_, err := s.Pool.Exec(ctx, `UPDATE group_agents SET owner_id = $2 WHERE owner_id = $1`, uid, *to)
		return err
	}
	_, err := s.Pool.Exec(ctx, `UPDATE group_agents SET status = 'disabled', data_until = COALESCE(data_until, $2)
		WHERE owner_id = $1 AND status = 'active'`, uid, time.Now().Add(s.retention(ctx)))
	return err
}

// Retention implements accounts.Groups: new removals use the new retention.
func (s *Service) Retention(int) {}

// Purge deletes agents whose data_until passed (GR-11).
func (s *Service) Purge(ctx context.Context) (int, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, data_user_id FROM group_agents WHERE status <> 'active' AND data_until < now() LIMIT 200`)
	if err != nil {
		return 0, err
	}
	type g struct {
		id   uuid.UUID
		data *uuid.UUID
	}
	var list []g
	for rows.Next() {
		var x g
		if rows.Scan(&x.id, &x.data) == nil {
			list = append(list, x)
		}
	}
	rows.Close()
	for _, x := range list {
		if x.data != nil {
			if s.DeleteData != nil {
				if err := s.DeleteData(ctx, *x.data); err != nil {
					return 0, err
				}
			}
			if _, err := s.Pool.Exec(ctx, `DELETE FROM users WHERE id = $1 AND created_via = 'group'`, *x.data); err != nil {
				return 0, err
			}
		}
		if _, err := s.Pool.Exec(ctx, `DELETE FROM group_agents WHERE id = $1`, x.id); err != nil {
			return 0, err
		}
	}
	return len(list), nil
}

// ─── management (tech §2.4) ─────────────────────────────────────────

// List lists group agents; owner limits to the agents of a user.
func (s *Service) List(ctx context.Context, status string, owner *uuid.UUID) ([]Agent, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+cols+from+` WHERE ($1 = '' OR g.status = $1) AND ($2::uuid IS NULL OR g.owner_id = $2)
		ORDER BY g.status = 'active' DESC, g.created_at DESC`, status, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Agent{}
	for rows.Next() {
		a, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// Patch is a change of a group agent.
type Patch struct {
	Name    *string      `json:"name"`
	Tone    *string      `json:"tone"`
	Model   *ModelChoice `json:"model"`
	Skills  *[]string    `json:"skills"`
	OwnerID *uuid.UUID   `json:"ownerId"`
	Status  *string      `json:"status"`
}

// ModelChoice is a model of the personal agents list.
type ModelChoice struct {
	ConnectionID uuid.UUID `json:"connectionId"`
	Model        string    `json:"model"`
}

// ModelAllowed reports whether the model is allowed for personal agents.
type ModelAllowed func(ctx context.Context, c ModelChoice) bool

// ErrForbidden is group_agent_forbidden (tech §1).
func ErrForbidden() error {
	return apperr.Forbidden("group_agent_forbidden", "only the owner and platform administrators manage the group agent")
}

// Update applies a patch; admin may also switch the state.
func (s *Service) Update(ctx context.Context, actor *domain.Principal, id uuid.UUID, p Patch, allowed ModelAllowed) (*Agent, error) {
	a, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !actor.IsAdmin && (a.OwnerID == nil || *a.OwnerID != actor.UserID) {
		return nil, ErrForbidden()
	}
	if p.Name != nil {
		n := strings.TrimSpace(*p.Name)
		if n == "" || utf8.RuneCountInString(n) > 40 {
			return nil, apperr.Unprocessable("invalid_name", "the name is 1–40 characters").With("field", "name")
		}
		p.Name = &n
	}
	if p.Tone != nil && !domain.ValidTone(*p.Tone) {
		return nil, apperr.Unprocessable("invalid_tone", "unknown tone").With("field", "tone")
	}
	var conn *uuid.UUID
	var model *string
	if p.Model != nil {
		if allowed != nil && !allowed(ctx, *p.Model) {
			return nil, apperr.Unprocessable("model_not_available", "the model is not in the list of available models").With("field", "model")
		}
		conn, model = &p.Model.ConnectionID, &p.Model.Model
	}
	if p.OwnerID != nil && *p.OwnerID != uuid.Nil && (a.OwnerID == nil || *p.OwnerID != *a.OwnerID) {
		if err := s.checkOwner(ctx, a, *p.OwnerID, actor.IsAdmin); err != nil {
			return nil, err
		}
	}
	if p.Status != nil {
		if !actor.IsAdmin {
			return nil, ErrForbidden()
		}
		if *p.Status != Active && *p.Status != Disabled {
			return nil, apperr.Unprocessable("invalid_status", "the state is active or disabled").With("field", "status")
		}
		if a.Status == Removed {
			return nil, apperr.Conflict("group_agent_removed", "the bot was removed from the group")
		}
	}
	var until *time.Time
	if p.Status != nil && *p.Status == Disabled {
		t := time.Now().Add(s.retention(ctx))
		until = &t
	}
	_, err = s.Pool.Exec(ctx, `UPDATE group_agents SET name = COALESCE($2, name), tone = COALESCE($3, tone),
		model_connection_id = CASE WHEN $5::text IS NULL THEN model_connection_id ELSE $4 END, model = COALESCE($5, model),
		skills = COALESCE($6, skills), owner_id = COALESCE($7, owner_id), status = COALESCE($8, status),
		data_until = CASE WHEN $8::text = 'active' THEN NULL WHEN $8::text = 'disabled' THEN $9 ELSE data_until END WHERE id = $1`,
		id, p.Name, p.Tone, conn, model, p.Skills, p.OwnerID, p.Status, until)
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}

// checkOwner: the new owner has an account and access to the channel and
// is a member of the group — known by the messages of the group (GR-09).
func (s *Service) checkOwner(ctx context.Context, a *Agent, uid uuid.UUID, admin bool) error {
	u, err := s.Users.Get(ctx, uid)
	if err != nil {
		return err
	}
	if u == nil || u.CreatedVia == "group" {
		return apperr.Unprocessable("invalid_owner", "unknown user").With("field", "ownerId")
	}
	if admin && u.IsAdmin && u.Status == "active" {
		return nil // administrators take agents over (R18)
	}
	if !s.Registry.Open(ctx, uid, a.Channel) {
		return apperr.Unprocessable("invalid_owner", "the new owner has no access to the channel").With("field", "ownerId")
	}
	var member bool
	if a.DataUserID != nil {
		_ = s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM messages m JOIN conversations c ON c.id = m.conversation_id
			WHERE c.user_id = $1 AND m.role = 'user' AND m.context->'group'->>'authorId' = $2::text)`, *a.DataUserID, uid).Scan(&member)
	}
	if !member {
		return apperr.Unprocessable("invalid_owner", "the new owner must be a member of the group who wrote to the agent").With("field", "ownerId")
	}
	return nil
}

// Routes mounts /group-agents of owners on the site.
func (s *Service) Routes(r chi.Router, allowed ModelAllowed) {
	r.Get("/group-agents", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		l, err := s.List(r.Context(), "", &p.UserID)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Patch("/group-agents/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in Patch
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		in.Status = nil
		owner := *p
		owner.IsAdmin = false // the site acts as the owner
		a, err := s.Update(r.Context(), &owner, id, in, allowed)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, a)
		return nil
	}))
}

// AdminRoutes mounts /group-agents of the administration.
func (s *Service) AdminRoutes(r chi.Router, allowed ModelAllowed) {
	r.Get("/group-agents", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		l, err := s.List(r.Context(), r.URL.Query().Get("status"), nil)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Patch("/group-agents/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in Patch
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		a, err := s.Update(r.Context(), p, id, in, allowed)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, a)
		return nil
	}))
}
