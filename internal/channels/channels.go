// Package channels connects channels to personal and group agents
// (FTR.NAB.CMN-0002; arch §2): the registry of channels with their settings
// and secrets, the availability rule, Telegram keys, the adapters of
// Telegram, VK Teams and mail. An adapter knows nothing of conversations:
// it normalizes an incoming message and sends a rendered answer.
package channels

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Adapter is a messenger (arch §2.1): Send renders the agent's Markdown for
// the channel; FetchFile reads an attachment of an incoming message.
type Adapter interface {
	Channel() string
	Send(ctx context.Context, chatID, markdown string) error
	Typing(ctx context.Context, chatID string)
	FetchFile(ctx context.Context, fileID string) (io.ReadCloser, string, error)
}

// Drafter streams a draft of the answer to a private chat (Telegram R14).
type Drafter interface {
	Draft(ctx context.Context, chatID string, draftID int64, text string) error
}

// Inbound is a normalized message from a channel.
type Inbound struct {
	UserID      uuid.UUID
	Channel     string
	Text        string
	Attachments []uuid.UUID
	// Conversation is a topic (mail); nil — the main conversation.
	Conversation *uuid.UUID
	// Context is shown to the agent with the message, not stored as its text.
	Context json.RawMessage
}

// Inbox accepts messages for the agent (the chat slice).
type Inbox interface {
	Receive(ctx context.Context, in Inbound) error
}

// Attachments stores files of incoming messages.
type Attachments interface {
	Store(ctx context.Context, uid uuid.UUID, name, mime string, r io.Reader, size int64) (uuid.UUID, error)
}

// AccessChanged is the broadcast event that resets the availability caches
// of api and worker (arch §2.2).
const AccessChanged = "access.changed"

// cacheTTL bounds how long a pod trusts its availability cache (CH-04).
const cacheTTL = 60 * time.Second

// Channel is a row of the registry as the administration sees it.
type Channel struct {
	Kind          string          `json:"kind"`
	Enabled       bool            `json:"enabled"`
	AllUsers      bool            `json:"allUsers"`
	GroupsEnabled bool            `json:"groupsEnabled"`
	UsersCount    int             `json:"usersCount"`
	Status        string          `json:"status"`
	StatusReason  *string         `json:"statusReason"`
	StatusAt      *time.Time      `json:"statusAt"`
	Settings      json.RawMessage `json:"settings"`
	// Secrets lists the names of stored secrets; values never leave the server.
	Secrets []string `json:"secrets"`
	// Locked: the channel cannot be switched off (the web application).
	Locked bool `json:"locked,omitempty"`
	// Product: the channel of a service client (delegation), not a messenger.
	Product bool `json:"product,omitempty"`
}

// Registry keeps the channels of the instance.
type Registry struct {
	Pool   *pgxpool.Pool
	Box    *crypto.Box
	Events events.Publisher
	// Changed is called after the settings of a channel change (adapters re-read them).
	Changed func(ctx context.Context, kind string)

	mu      sync.Mutex
	loaded  time.Time
	rows    map[string]row
	access  map[uuid.UUID]userAccess
	nowFunc func() time.Time
}

type row struct {
	enabled, allUsers, groups bool
}

type userAccess struct {
	at       time.Time
	status   string
	explicit map[string]bool
}

func (r *Registry) now() time.Time {
	if r.nowFunc != nil {
		return r.nowFunc()
	}
	return time.Now()
}

// Invalidate drops the caches of this pod.
func (r *Registry) Invalidate() {
	r.mu.Lock()
	r.rows, r.access = nil, nil
	r.mu.Unlock()
}

// Listen resets the caches on AccessChanged from any pod (arch §2.2).
func (r *Registry) Listen(ctx context.Context, hub *events.Hub) {
	sub := hub.Subscribe(uuid.Nil)
	defer hub.Unsubscribe(sub)
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-sub.C:
			if e.Type == AccessChanged {
				r.Invalidate()
				data, _ := e.Data.(map[string]any)
				if kind, _ := data["kind"].(string); kind != "" && r.Changed != nil {
					r.Changed(ctx, kind)
				}
			}
		}
	}
}

// changed resets the caches of every pod; the adapters re-read the settings
// of the kind through Listen (or at once without events).
func (r *Registry) changed(ctx context.Context, kind string) {
	r.Invalidate()
	if r.Events != nil {
		r.Events.Publish(ctx, events.Event{Type: AccessChanged, Data: map[string]any{"kind": kind}})
	} else if kind != "" && r.Changed != nil {
		r.Changed(ctx, kind)
	}
}

func (r *Registry) loadRows(ctx context.Context) (map[string]row, error) {
	r.mu.Lock()
	if r.rows != nil && r.now().Sub(r.loaded) < cacheTTL {
		rows := r.rows
		r.mu.Unlock()
		return rows, nil
	}
	r.mu.Unlock()
	q, err := r.Pool.Query(ctx, `SELECT kind, enabled, all_users, groups_enabled FROM channels`)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	rows := map[string]row{}
	for q.Next() {
		var k string
		var x row
		if err := q.Scan(&k, &x.enabled, &x.allUsers, &x.groups); err != nil {
			return nil, err
		}
		rows[k] = x
	}
	if err := q.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.rows, r.loaded = rows, r.now()
	if r.access == nil {
		r.access = map[uuid.UUID]userAccess{}
	}
	r.mu.Unlock()
	return rows, nil
}

func (r *Registry) userAccess(ctx context.Context, uid uuid.UUID) (userAccess, error) {
	r.mu.Lock()
	if a, ok := r.access[uid]; ok && r.now().Sub(a.at) < cacheTTL {
		r.mu.Unlock()
		return a, nil
	}
	r.mu.Unlock()
	a := userAccess{at: r.now(), explicit: map[string]bool{}}
	if err := r.Pool.QueryRow(ctx, `SELECT status FROM users WHERE id = $1`, uid).Scan(&a.status); err != nil {
		if postgres.IsNoRows(err) {
			return a, nil
		}
		return a, err
	}
	q, err := r.Pool.Query(ctx, `SELECT channel FROM channel_user_access WHERE user_id = $1`, uid)
	if err != nil {
		return a, err
	}
	defer q.Close()
	for q.Next() {
		var k string
		if q.Scan(&k) == nil {
			a.explicit[k] = true
		}
	}
	r.mu.Lock()
	if r.access == nil {
		r.access = map[uuid.UUID]userAccess{}
	}
	r.access[uid] = a
	r.mu.Unlock()
	return a, q.Err()
}

// Reasons of availability (tech §2.2).
const (
	ReasonAlways   = "always"
	ReasonAllUsers = "all_users"
	ReasonUser     = "user"
	ReasonDisabled = "disabled"
)

// Available applies the rule of arch §2.2: the web is always open; other
// channels are open to active users when the channel is enabled and either
// open to all users or enabled for the user.
func (r *Registry) Available(ctx context.Context, uid uuid.UUID, kind string) (bool, string, error) {
	if kind == domain.ChannelWeb {
		return true, ReasonAlways, nil
	}
	rows, err := r.loadRows(ctx)
	if err != nil {
		return false, "", err
	}
	a, err := r.userAccess(ctx, uid)
	if err != nil {
		return false, "", err
	}
	ch, ok := rows[kind]
	switch {
	case !ok || !ch.enabled:
		return false, ReasonDisabled, nil
	case a.status != "active":
		return false, ReasonDisabled, nil
	case ch.allUsers:
		return true, ReasonAllUsers, nil
	case a.explicit[kind]:
		return true, ReasonUser, nil
	}
	return false, ReasonDisabled, nil
}

// Open reports whether the channel is available (errors close it).
func (r *Registry) Open(ctx context.Context, uid uuid.UUID, kind string) bool {
	ok, _, err := r.Available(ctx, uid, kind)
	if err != nil {
		slog.WarnContext(ctx, "channel availability", "channel", kind, "err", err)
	}
	return ok
}

// Enabled reports whether the channel is enabled in the instance.
func (r *Registry) Enabled(ctx context.Context, kind string) bool {
	rows, err := r.loadRows(ctx)
	return err == nil && rows[kind].enabled
}

// GroupsEnabled reports whether group chats are enabled for the channel.
func (r *Registry) GroupsEnabled(ctx context.Context, kind string) bool {
	rows, err := r.loadRows(ctx)
	return err == nil && rows[kind].enabled && rows[kind].groups
}

// ErrUnavailable is channel_unavailable (tech §1).
func ErrUnavailable() error {
	return apperr.Forbidden("channel_unavailable", "the channel is not available for this account")
}

// ─── administration ─────────────────────────────────────────────────

const chanCols = `kind, enabled, all_users, groups_enabled, status, status_reason, status_at, settings, secrets_enc,
	(SELECT count(*) FROM channel_user_access a WHERE a.channel = c.kind)`

func (r *Registry) scan(row pgx.Row) (*Channel, error) {
	var c Channel
	var enc []byte
	if err := row.Scan(&c.Kind, &c.Enabled, &c.AllUsers, &c.GroupsEnabled, &c.Status, &c.StatusReason, &c.StatusAt, &c.Settings, &enc, &c.UsersCount); err != nil {
		return nil, err
	}
	c.Secrets = []string{}
	if len(enc) > 0 {
		if s, err := r.secrets(enc); err == nil {
			for k, v := range s {
				if v != "" {
					c.Secrets = append(c.Secrets, k)
				}
			}
		}
	}
	c.Locked = c.Kind == domain.ChannelWeb
	return &c, nil
}

func (r *Registry) secrets(enc []byte) (map[string]string, error) {
	out := map[string]string{}
	if len(enc) == 0 {
		return out, nil
	}
	pt, err := r.Box.Open(enc)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(pt), &out)
	return out, err
}

// List lists the channels in the order of the administration.
func (r *Registry) List(ctx context.Context) ([]Channel, error) {
	q, err := r.Pool.Query(ctx, `SELECT `+chanCols+` FROM channels c`)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	by := map[string]Channel{}
	for q.Next() {
		c, err := r.scan(q)
		if err != nil {
			return nil, err
		}
		by[c.Kind] = *c
	}
	// R15: the channel of a product is listed only while its service client exists
	hasProduct := false
	_ = r.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM service_clients WHERE name = $1)`, domain.ChannelHammurapi).Scan(&hasProduct)
	out := []Channel{}
	for _, k := range domain.ChannelKinds {
		if k == domain.ChannelHammurapi && !hasProduct {
			continue
		}
		if c, ok := by[k]; ok {
			c.Product = k == domain.ChannelHammurapi
			out = append(out, c)
		}
	}
	return out, q.Err()
}

// Get loads a channel.
func (r *Registry) Get(ctx context.Context, kind string) (*Channel, error) {
	c, err := r.scan(r.Pool.QueryRow(ctx, `SELECT `+chanCols+` FROM channels c WHERE kind = $1`, kind))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "unknown channel")
	}
	return c, err
}

// Config returns the settings and the decrypted secrets of a channel (adapters only).
func (r *Registry) Config(ctx context.Context, kind string, settings any) (map[string]string, bool, error) {
	var raw []byte
	var enc []byte
	var enabled bool
	err := r.Pool.QueryRow(ctx, `SELECT settings, secrets_enc, enabled FROM channels WHERE kind = $1`, kind).Scan(&raw, &enc, &enabled)
	if err != nil {
		return nil, false, err
	}
	if settings != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, settings); err != nil {
			return nil, false, err
		}
	}
	s, err := r.secrets(enc)
	return s, enabled, err
}

// Patch is PATCH /channels/{kind}.
type Patch struct {
	Enabled       *bool             `json:"enabled"`
	AllUsers      *bool             `json:"allUsers"`
	GroupsEnabled *bool             `json:"groupsEnabled"`
	Settings      json.RawMessage   `json:"settings"`
	Secrets       map[string]string `json:"secrets"` // "" removes a secret; absent keys are kept
}

// Validator checks the settings of a channel kind and returns them normalized.
type Validator func(settings json.RawMessage) (json.RawMessage, error)

// Update applies a patch (CH-01: the web cannot be switched off).
func (r *Registry) Update(ctx context.Context, kind string, p Patch, validate Validator) (*Channel, error) {
	if kind == domain.ChannelWeb {
		return nil, apperr.Conflict("channel_locked", "the web application is always available")
	}
	err := postgres.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		var raw, enc []byte
		if err := tx.QueryRow(ctx, `SELECT settings, secrets_enc FROM channels WHERE kind = $1 FOR UPDATE`, kind).Scan(&raw, &enc); err != nil {
			if postgres.IsNoRows(err) {
				return apperr.NotFound("not_found", "unknown channel")
			}
			return err
		}
		if len(p.Settings) > 0 && string(p.Settings) != "null" {
			raw = p.Settings
			if validate != nil {
				v, err := validate(p.Settings)
				if err != nil {
					return err
				}
				raw = v
			}
		}
		if p.Secrets != nil {
			cur, err := r.secrets(enc)
			if err != nil {
				return err
			}
			for k, v := range p.Secrets {
				if v == "" {
					delete(cur, k)
				} else {
					cur[k] = v
				}
			}
			b, _ := json.Marshal(cur)
			if enc, err = r.Box.Seal(string(b)); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `UPDATE channels SET enabled = COALESCE($2, enabled), all_users = COALESCE($3, all_users),
			groups_enabled = COALESCE($4, groups_enabled), settings = $5, secrets_enc = $6, updated_at = now() WHERE kind = $1`,
			kind, p.Enabled, p.AllUsers, p.GroupsEnabled, raw, enc)
		return err
	})
	if err != nil {
		return nil, err
	}
	r.changed(ctx, kind)
	return r.Get(ctx, kind)
}

// SeedSecrets stores secrets of a channel that has none yet (the
// configuration of FTR.NAB.CMN-0001 from the environment).
func (r *Registry) SeedSecrets(ctx context.Context, kind string, secrets map[string]string) error {
	b, _ := json.Marshal(secrets)
	enc, err := r.Box.Seal(string(b))
	if err != nil {
		return err
	}
	_, err = r.Pool.Exec(ctx, `UPDATE channels SET secrets_enc = $2 WHERE kind = $1 AND secrets_enc IS NULL`, kind, enc)
	return err
}

// SetStatus records the state of the connection of a channel.
func (r *Registry) SetStatus(ctx context.Context, kind, status, reason string) {
	_, _ = r.Pool.Exec(ctx, `UPDATE channels SET status = $2, status_reason = NULLIF($3,''), status_at = now() WHERE kind = $1`, kind, status, reason)
}

// SetSetting stores one value in the settings (e.g. the bot username read by getMe).
func (r *Registry) SetSetting(ctx context.Context, kind, key string, value any) {
	b, _ := json.Marshal(value)
	_, _ = r.Pool.Exec(ctx, `UPDATE channels SET settings = jsonb_set(settings, ARRAY[$2::text], $3::jsonb) WHERE kind = $1`, kind, key, b)
}

// UserRef is a user with explicit access to a channel.
type UserRef struct {
	ID    uuid.UUID `json:"id"`
	Email string    `json:"email"`
	Name  string    `json:"name"`
}

// Users lists users with explicit access.
func (r *Registry) Users(ctx context.Context, kind string) ([]UserRef, error) {
	q, err := r.Pool.Query(ctx, `SELECT u.id, u.email, COALESCE(u.name,'') FROM channel_user_access a JOIN users u ON u.id = a.user_id
		WHERE a.channel = $1 ORDER BY u.email`, kind)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	out := []UserRef{}
	for q.Next() {
		var u UserRef
		if err := q.Scan(&u.ID, &u.Email, &u.Name); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, q.Err()
}

// SetUsers adds and removes explicit access.
func (r *Registry) SetUsers(ctx context.Context, kind string, add, remove []uuid.UUID) error {
	if kind == domain.ChannelWeb {
		return apperr.Conflict("channel_locked", "the web application is always available")
	}
	err := postgres.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		for _, id := range add {
			if _, err := tx.Exec(ctx, `INSERT INTO channel_user_access (channel, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, kind, id); err != nil {
				if postgres.IsForeignKeyViolation(err) {
					return apperr.Unprocessable("not_found", "unknown user").With("userId", id)
				}
				return err
			}
		}
		for _, id := range remove {
			if _, err := tx.Exec(ctx, `DELETE FROM channel_user_access WHERE channel = $1 AND user_id = $2`, kind, id); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		r.changed(ctx, kind)
	}
	return err
}

// UserChannel is a channel in the card of a user (tech §2.2).
type UserChannel struct {
	Kind      string   `json:"kind"`
	Available bool     `json:"available"`
	Reason    string   `json:"reason"`
	Binding   *Binding `json:"binding,omitempty"`
}

// Binding is the linked Telegram account.
type Binding struct {
	Account string    `json:"account"`
	BoundAt time.Time `json:"boundAt"`
}

// OfUser lists the channels of a user with availability and the reason.
// hammurapi is listed only while the service client exists (R15).
func (r *Registry) OfUser(ctx context.Context, uid uuid.UUID) ([]UserChannel, error) {
	hasHammurapi := false
	_ = r.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM service_clients WHERE name = $1)`, domain.ChannelHammurapi).Scan(&hasHammurapi)
	out := []UserChannel{}
	for _, k := range domain.ChannelKinds {
		if k == domain.ChannelHammurapi && !hasHammurapi {
			continue
		}
		ok, reason, err := r.Available(ctx, uid, k)
		if err != nil {
			return nil, err
		}
		uc := UserChannel{Kind: k, Available: ok, Reason: reason}
		if k == domain.ChannelTelegram {
			var b Binding
			var name *string
			var tg int64
			if err := r.Pool.QueryRow(ctx, `SELECT tg_user_id, username, bound_at FROM telegram_bindings WHERE user_id = $1`, uid).Scan(&tg, &name, &b.BoundAt); err == nil {
				b.Account = accountName(tg, name)
				uc.Binding = &b
			}
		}
		out = append(out, uc)
	}
	return out, nil
}

// SetUserChannels switches channels of a user that are not open to all users.
func (r *Registry) SetUserChannels(ctx context.Context, uid uuid.UUID, set map[string]bool) error {
	rows, err := r.loadRows(ctx)
	if err != nil {
		return err
	}
	err = postgres.InTx(ctx, r.Pool, func(tx pgx.Tx) error {
		for k, on := range set {
			if k == domain.ChannelWeb {
				if !on {
					return apperr.Conflict("channel_locked", "the web application is always available")
				}
				continue
			}
			if _, ok := rows[k]; !ok {
				return apperr.Unprocessable("not_found", "unknown channel").With("channel", k)
			}
			q := `DELETE FROM channel_user_access WHERE channel = $1 AND user_id = $2`
			if on {
				q = `INSERT INTO channel_user_access (channel, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`
			}
			if _, err := tx.Exec(ctx, q, k, uid); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		r.changed(ctx, "")
	}
	return err
}
