// Package users keeps users, identities, browser sessions and the profile of
// the personal agent (FTR.NAB.CMN-0001 R1, R3, R33–R34; tech §2, §3.1).
package users

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// User is a users row.
type User struct {
	ID         uuid.UUID  `json:"id"`
	Email      string     `json:"email"`
	Name       string     `json:"name"`
	AvatarURL  string     `json:"avatarUrl,omitempty"`
	IsAdmin    bool       `json:"isAdmin"`
	Status     string     `json:"status"`
	Language   string     `json:"language"`
	Theme      string     `json:"theme"`
	Timezone   string     `json:"timezone"`
	CreatedVia string     `json:"createdVia"`
	LastSeenAt *time.Time `json:"lastSeenAt"`
	CreatedAt  time.Time  `json:"createdAt"`
}

// Identity is a user as the sign-in provider sees them.
type Identity struct {
	Issuer, Subject     string
	Email, Name, Avatar string
}

// Repo persists users.
type Repo struct {
	Pool            *pgxpool.Pool
	DefaultLanguage string
	DefaultTimezone string
	SpaceQuota      int64
}

const userCols = `id, email, COALESCE(name,''), COALESCE(avatar_url,''), is_admin, status, language, theme, timezone, created_via, last_seen_at, created_at`

func scanUser(row pgx.Row) (*User, error) {
	var u User
	err := row.Scan(&u.ID, &u.Email, &u.Name, &u.AvatarURL, &u.IsAdmin, &u.Status, &u.Language, &u.Theme, &u.Timezone, &u.CreatedVia, &u.LastSeenAt, &u.CreatedAt)
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return &u, err
}

// Get loads a user or nil.
func (r *Repo) Get(ctx context.Context, id uuid.UUID) (*User, error) {
	return scanUser(r.Pool.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE id = $1`, id))
}

// ByEmail loads a user by email or nil.
func (r *Repo) ByEmail(ctx context.Context, email string) (*User, error) {
	return scanUser(r.Pool.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE email = $1`, domain.NormalizeEmail(email)))
}

// Principal loads the principal of a user (roles are read on every request).
func (r *Repo) Principal(ctx context.Context, id uuid.UUID) (*domain.Principal, error) {
	u, err := r.Get(ctx, id)
	if err != nil || u == nil {
		return nil, err
	}
	return &domain.Principal{UserID: u.ID, Email: u.Email, Name: u.Name, IsAdmin: u.IsAdmin, Blocked: u.Status == "blocked"}, nil
}

// ErrBlocked is the answer to a blocked user (R34, AUTH-11).
var ErrBlocked = apperr.Forbidden("user_blocked", "access to the agent is closed for this user")

// SignIn finds or creates the user of an identity (tech §2): by (issuer,
// subject), then by email among users without an identity of this provider
// (invited in advance or created by delegation), otherwise a new user. The
// email is refreshed on every sign-in; bootstrap administrators get the role.
func (r *Repo) SignIn(ctx context.Context, id Identity, bootstrapAdmin bool) (*User, bool, error) {
	email := domain.NormalizeEmail(id.Email)
	if email == "" {
		return nil, false, apperr.Unauthorized("email_required", "the provider gave no verified email")
	}
	var out *User
	created := false
	err := postgres.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		var uid uuid.UUID
		err := tx.QueryRow(ctx, `SELECT user_id FROM user_identities WHERE issuer = $1 AND subject = $2`, id.Issuer, id.Subject).Scan(&uid)
		switch {
		case err == nil:
			// AUTH-05: the same account; the email follows the provider unless another user owns it.
			if _, err := tx.Exec(ctx, `UPDATE users SET email = $2 WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM users WHERE email = $2 AND id <> $1)`, uid, email); err != nil {
				return err
			}
		case postgres.IsNoRows(err):
			err = tx.QueryRow(ctx, `SELECT u.id FROM users u WHERE u.email = $1
				AND NOT EXISTS (SELECT 1 FROM user_identities i WHERE i.user_id = u.id AND i.issuer = $2)`, email, id.Issuer).Scan(&uid)
			if postgres.IsNoRows(err) {
				err = tx.QueryRow(ctx, `INSERT INTO users (email, name, avatar_url, language, timezone) VALUES ($1,$2,$3,$4,$5) RETURNING id`,
					email, nilIfEmpty(id.Name), nilIfEmpty(id.Avatar), r.DefaultLanguage, r.DefaultTimezone).Scan(&uid)
				created = true
			}
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO user_identities (issuer, subject, user_id) VALUES ($1,$2,$3)`, id.Issuer, id.Subject, uid); err != nil {
				return err
			}
		default:
			return err
		}
		// AUTH-06: an invitation becomes active on the first sign-in, with its role.
		if _, err := tx.Exec(ctx, `UPDATE users SET status = CASE WHEN status = 'invited' THEN 'active' ELSE status END,
			name = COALESCE(NULLIF($2,''), name), avatar_url = COALESCE(NULLIF($3,''), avatar_url),
			is_admin = is_admin OR $4, last_seen_at = now() WHERE id = $1`, uid, id.Name, id.Avatar, bootstrapAdmin); err != nil {
			return err
		}
		if err := ensureAgent(ctx, tx, uid, r.SpaceQuota); err != nil {
			return err
		}
		u, err := scanUser(tx.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE id = $1`, uid))
		out = u
		return err
	})
	return out, created, err
}

// EnsureByEmail returns the user with the email, creating one for delegation
// when there is none (R1a, HMR-03).
func (r *Repo) EnsureByEmail(ctx context.Context, email string) (*User, error) {
	email = domain.NormalizeEmail(email)
	if email == "" || !validEmail(email) {
		return nil, apperr.BadRequest("invalid_email", "a valid email is required")
	}
	var out *User
	err := postgres.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		var uid uuid.UUID
		err := tx.QueryRow(ctx, `INSERT INTO users (email, language, timezone, created_via) VALUES ($1,$2,$3,'delegation')
			ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email RETURNING id`, email, r.DefaultLanguage, r.DefaultTimezone).Scan(&uid)
		if err != nil {
			return err
		}
		if err := ensureAgent(ctx, tx, uid, r.SpaceQuota); err != nil {
			return err
		}
		out, err = scanUser(tx.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE id = $1`, uid))
		return err
	})
	return out, err
}

func validEmail(e string) bool {
	at := 0
	for i, c := range e {
		if c == '@' {
			if at != 0 {
				return false
			}
			at = i
		}
		if c == ' ' {
			return false
		}
	}
	return at > 0 && at < len(e)-1
}

// ensureAgent creates the personal agent, the main conversation and the
// space of a user (R1: the agent exists after the first sign-in).
func ensureAgent(ctx context.Context, q postgres.Querier, uid uuid.UUID, quota int64) error {
	if _, err := q.Exec(ctx, `INSERT INTO agent_profiles (user_id) VALUES ($1) ON CONFLICT DO NOTHING`, uid); err != nil {
		return err
	}
	if _, err := q.Exec(ctx, `INSERT INTO conversations (user_id, kind, title) VALUES ($1, 'main', NULL) ON CONFLICT DO NOTHING`, uid); err != nil {
		return err
	}
	_, err := q.Exec(ctx, `INSERT INTO spaces (user_id, quota_bytes) VALUES ($1, $2) ON CONFLICT DO NOTHING`, uid, quota)
	return err
}

// Invite adds a user by email before the first sign-in (R34).
func (r *Repo) Invite(ctx context.Context, email string, admin bool) (*User, error) {
	email = domain.NormalizeEmail(email)
	if !validEmail(email) {
		return nil, apperr.BadRequest("invalid_email", "a valid email is required")
	}
	var out *User
	err := postgres.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		var uid uuid.UUID
		err := tx.QueryRow(ctx, `INSERT INTO users (email, is_admin, status, language, timezone, created_via)
			VALUES ($1,$2,'invited',$3,$4,'invite') RETURNING id`, email, admin, r.DefaultLanguage, r.DefaultTimezone).Scan(&uid)
		if postgres.IsUniqueViolation(err) {
			return apperr.Conflict("user_exists", "a user with this email exists")
		}
		if err != nil {
			return err
		}
		if err := ensureAgent(ctx, tx, uid, r.SpaceQuota); err != nil {
			return err
		}
		out, err = scanUser(tx.QueryRow(ctx, `SELECT `+userCols+` FROM users WHERE id = $1`, uid))
		return err
	})
	return out, err
}

// Touch records activity.
func (r *Repo) Touch(ctx context.Context, id uuid.UUID) {
	_, _ = r.Pool.Exec(ctx, `UPDATE users SET last_seen_at = now() WHERE id = $1 AND (last_seen_at IS NULL OR last_seen_at < now() - interval '5 minutes')`, id)
}

// ─── sessions ───────────────────────────────────────────────────────

// SessionTTL is the sliding lifetime of a browser session.
const SessionTTL = 30 * 24 * time.Hour

// SessionRow is a live session.
type SessionRow struct {
	ID         uuid.UUID
	UserID     uuid.UUID
	CSRFToken  string
	LastSeenAt time.Time
}

// CreateSession creates a browser session.
func (r *Repo) CreateSession(ctx context.Context, userID uuid.UUID, csrf string) (uuid.UUID, error) {
	var id uuid.UUID
	err := r.Pool.QueryRow(ctx, `INSERT INTO user_sessions (user_id, csrf_token, expires_at) VALUES ($1,$2,$3) RETURNING id`,
		userID, csrf, time.Now().Add(SessionTTL)).Scan(&id)
	return id, err
}

// Session loads a live session or nil.
func (r *Repo) Session(ctx context.Context, id uuid.UUID) (*SessionRow, error) {
	var s SessionRow
	err := r.Pool.QueryRow(ctx, `SELECT id, user_id, csrf_token, last_seen_at FROM user_sessions WHERE id = $1 AND expires_at > now()`, id).
		Scan(&s.ID, &s.UserID, &s.CSRFToken, &s.LastSeenAt)
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return &s, err
}

// TouchSession extends a session.
func (r *Repo) TouchSession(ctx context.Context, id uuid.UUID) error {
	_, err := r.Pool.Exec(ctx, `UPDATE user_sessions SET last_seen_at = now(), expires_at = $2 WHERE id = $1`, id, time.Now().Add(SessionTTL))
	return err
}

// DeleteSession removes a session.
func (r *Repo) DeleteSession(ctx context.Context, id uuid.UUID) error {
	_, err := r.Pool.Exec(ctx, `DELETE FROM user_sessions WHERE id = $1`, id)
	return err
}

// ─── profile and agent ──────────────────────────────────────────────

// Patch is PATCH /me.
type Patch struct {
	Language *string `json:"language"`
	Theme    *string `json:"theme"`
	Timezone *string `json:"timezone"`
}

// UpdateProfile applies a profile patch.
func (r *Repo) UpdateProfile(ctx context.Context, id uuid.UUID, p Patch) error {
	if p.Language != nil && !domain.ValidLanguage(*p.Language) {
		return apperr.Unprocessable("invalid_language", "unknown language")
	}
	if p.Theme != nil && !domain.ValidTheme(*p.Theme) {
		return apperr.Unprocessable("invalid_theme", "unknown theme")
	}
	if p.Timezone != nil {
		if _, err := time.LoadLocation(*p.Timezone); err != nil || *p.Timezone == "" || *p.Timezone == "Local" {
			return apperr.Unprocessable("invalid_timezone", "unknown time zone")
		}
	}
	_, err := r.Pool.Exec(ctx, `UPDATE users SET language = COALESCE($2, language), theme = COALESCE($3, theme),
		timezone = COALESCE($4, timezone) WHERE id = $1`, id, p.Language, p.Theme, p.Timezone)
	return err
}

// Agent is the profile of the personal agent.
type Agent struct {
	Name         string     `json:"name"`
	Tone         string     `json:"tone"`
	ConnectionID *uuid.UUID `json:"connectionId"`
	Model        *string    `json:"model"`
	UpdatedAt    time.Time  `json:"updatedAt"`
}

// GetAgent loads the agent profile.
func (r *Repo) GetAgent(ctx context.Context, userID uuid.UUID) (Agent, error) {
	var a Agent
	err := r.Pool.QueryRow(ctx, `SELECT name, tone, model_connection_id, model, updated_at FROM agent_profiles WHERE user_id = $1`, userID).
		Scan(&a.Name, &a.Tone, &a.ConnectionID, &a.Model, &a.UpdatedAt)
	if postgres.IsNoRows(err) {
		if err := ensureAgent(ctx, r.Pool, userID, r.SpaceQuota); err != nil {
			return a, err
		}
		return r.GetAgent(ctx, userID)
	}
	return a, err
}

// SetAgent updates name, tone and model (nil keeps the value).
func (r *Repo) SetAgent(ctx context.Context, userID uuid.UUID, name, tone *string, conn *uuid.UUID, model *string) error {
	_, err := r.Pool.Exec(ctx, `UPDATE agent_profiles SET name = COALESCE($2, name), tone = COALESCE($3, tone),
		model_connection_id = CASE WHEN $5::text IS NULL THEN model_connection_id ELSE $4 END,
		model = COALESCE($5, model), updated_at = now() WHERE user_id = $1`, userID, name, tone, conn, model)
	return err
}

// ─── administration ─────────────────────────────────────────────────

// AdminUser is a row of Administration → Users.
type AdminUser struct {
	User
	Channels []string `json:"channels"`
}

// List lists users with search and status filter.
func (r *Repo) List(ctx context.Context, q, status string, limit int) ([]AdminUser, error) {
	rows, err := r.Pool.Query(ctx, `SELECT `+prefixed("u.")+`, COALESCE(array_agg(c.channel ORDER BY c.channel) FILTER (WHERE c.channel IS NOT NULL), '{}')
		FROM users u LEFT JOIN channel_links c ON c.user_id = u.id
		WHERE ($1 = '' OR u.email ILIKE '%' || $1 || '%' OR u.name ILIKE '%' || $1 || '%') AND ($2 = '' OR u.status = $2)
		GROUP BY u.id ORDER BY u.last_seen_at DESC NULLS LAST, u.created_at DESC LIMIT $3`, q, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AdminUser{}
	for rows.Next() {
		var a AdminUser
		if err := rows.Scan(&a.ID, &a.Email, &a.Name, &a.AvatarURL, &a.IsAdmin, &a.Status, &a.Language, &a.Theme, &a.Timezone,
			&a.CreatedVia, &a.LastSeenAt, &a.CreatedAt, &a.Channels); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func prefixed(p string) string {
	return p + `id, ` + p + `email, COALESCE(` + p + `name,''), COALESCE(` + p + `avatar_url,''), ` + p + `is_admin, ` + p + `status, ` +
		p + `language, ` + p + `theme, ` + p + `timezone, ` + p + `created_via, ` + p + `last_seen_at, ` + p + `created_at`
}

// AdminPatch changes role and blocking.
func (r *Repo) AdminPatch(ctx context.Context, actor, id uuid.UUID, isAdmin, blocked *bool) (*User, error) {
	u, err := r.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, apperr.NotFound("not_found", "user not found")
	}
	if actor == id && ((isAdmin != nil && !*isAdmin) || (blocked != nil && *blocked)) {
		return nil, apperr.Conflict("cannot_demote_self", "you cannot remove your own role or block yourself")
	}
	status := u.Status
	if blocked != nil {
		switch {
		case *blocked:
			status = "blocked"
		case u.Status == "blocked":
			status = "active"
			if u.LastSeenAt == nil {
				status = "invited"
			}
		}
	}
	admin := u.IsAdmin
	if isAdmin != nil {
		admin = *isAdmin
	}
	if _, err := r.Pool.Exec(ctx, `UPDATE users SET is_admin = $2, status = $3 WHERE id = $1`, id, admin, status); err != nil {
		return nil, err
	}
	if status == "blocked" {
		_, _ = r.Pool.Exec(ctx, `DELETE FROM user_sessions WHERE user_id = $1`, id)
	}
	return r.Get(ctx, id)
}

// Allowed reports whether the user exists and is not blocked.
func (r *Repo) Allowed(ctx context.Context, id uuid.UUID) bool {
	var st string
	err := r.Pool.QueryRow(ctx, `SELECT status FROM users WHERE id = $1`, id).Scan(&st)
	return err == nil && st != "blocked"
}

// ErrNotFound is a missing user.
var ErrNotFound = errors.New("user not found")

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
