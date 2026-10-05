// Package channels connects messengers to personal agents
// (FTR.NAB.CMN-0001 R2, R9, R10; arch §6; tech §8): Telegram first, VK
// WorkSpace behind VKWS_ENABLED once its bot API is ready. A messenger
// account is linked to a Nabu account with a one-time code from the site;
// messages of unlinked accounts are never passed to the agent.
package channels

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Adapter is a messenger (tech §8: Receive is the webhook, Send, Typing, FetchFile).
type Adapter interface {
	Channel() string
	Send(ctx context.Context, chatID, markdown string) error
	Typing(ctx context.Context, chatID string)
	FetchFile(ctx context.Context, fileID string) (io.ReadCloser, string, error)
}

// Inbound is a normalized message from a channel.
type Inbound struct {
	UserID      uuid.UUID
	Channel     string
	Text        string
	Attachments []uuid.UUID
}

// Inbox accepts messages for the agent (the chat slice).
type Inbox interface {
	Receive(ctx context.Context, in Inbound) error
}

// Attachments stores files of incoming messages.
type Attachments interface {
	Store(ctx context.Context, uid uuid.UUID, name, mime string, r io.Reader, size int64) (uuid.UUID, error)
}

// LinkTTL is the lifetime of a link code; codes are one-time (tech §3.5).
const LinkTTL = 10 * time.Minute

const codeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

// Links keeps the links of messenger accounts.
type Links struct {
	Pool   *pgxpool.Pool
	Events events.Publisher
	// Enabled lists the enabled messengers.
	Enabled map[string]bool
	// BotNames are the usernames of the bots (for the instruction on the site).
	BotNames map[string]string
	now      func() time.Time
}

func (l *Links) clock() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

func codeHash(code string) []byte {
	h := sha256.Sum256([]byte(strings.ToUpper(strings.TrimSpace(code))))
	return h[:]
}

// NewCode issues a link code: 8 characters, 10 minutes, stored as a hash.
func (l *Links) NewCode(ctx context.Context, uid uuid.UUID, ch string) (string, time.Time, error) {
	if !l.Enabled[ch] {
		return "", time.Time{}, apperr.NotFound("channel_disabled", "the channel is not enabled")
	}
	b := make([]byte, 8)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(codeAlphabet))))
		b[i] = codeAlphabet[n.Int64()]
	}
	code := string(b)
	exp := l.clock().Add(LinkTTL)
	_, _ = l.Pool.Exec(ctx, `DELETE FROM link_codes WHERE user_id = $1 AND channel = $2 AND used_at IS NULL`, uid, ch)
	if _, err := l.Pool.Exec(ctx, `INSERT INTO link_codes (code_hash, user_id, channel, expires_at) VALUES ($1,$2,$3,$4)`, codeHash(code), uid, ch, exp); err != nil {
		return "", time.Time{}, err
	}
	return code, exp, nil
}

// ErrCodeInvalid is link_code_invalid (AUTH-09).
func ErrCodeInvalid() error {
	return apperr.Unprocessable("link_code_invalid", "the link code is invalid or expired")
}

// Link consumes a code and links the messenger account (AUTH-08).
func (l *Links) Link(ctx context.Context, ch, code, externalID, chatID, account string) (uuid.UUID, error) {
	var uid uuid.UUID
	err := postgres.InTx(ctx, l.Pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `UPDATE link_codes SET used_at = now() WHERE code_hash = $1 AND channel = $2 AND used_at IS NULL AND expires_at > $3
			RETURNING user_id`, codeHash(code), ch, l.clock()).Scan(&uid)
		if postgres.IsNoRows(err) {
			return ErrCodeInvalid()
		}
		if err != nil {
			return err
		}
		// One messenger account belongs to one user: a re-link moves it.
		if _, err := tx.Exec(ctx, `DELETE FROM channel_links WHERE channel = $1 AND external_id = $2`, ch, externalID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO channel_links (user_id, channel, external_id, chat_id, account) VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (user_id, channel) DO UPDATE SET external_id = EXCLUDED.external_id, chat_id = EXCLUDED.chat_id,
			account = EXCLUDED.account, linked_at = now()`, uid, ch, externalID, chatID, account)
		return err
	})
	if err == nil && l.Events != nil {
		l.Events.Publish(ctx, events.Event{Type: events.ConnectionsChanged, UserID: &uid, Data: map[string]string{"channel": ch}})
	}
	return uid, err
}

// Linked is a linked account.
type Linked struct {
	UserID  uuid.UUID
	Status  string
	ChatID  string
	Account string
}

// ByExternal finds the user of a messenger account.
func (l *Links) ByExternal(ctx context.Context, ch, externalID string) (*Linked, error) {
	var k Linked
	err := l.Pool.QueryRow(ctx, `SELECT c.user_id, u.status, COALESCE(c.chat_id, c.external_id), COALESCE(c.account,'') FROM channel_links c
		JOIN users u ON u.id = c.user_id WHERE c.channel = $1 AND c.external_id = $2`, ch, externalID).Scan(&k.UserID, &k.Status, &k.ChatID, &k.Account)
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return &k, err
}

// ChatOf returns the chat of a user's linked messenger.
func (l *Links) ChatOf(ctx context.Context, uid uuid.UUID, ch string) (string, bool) {
	var chat string
	err := l.Pool.QueryRow(ctx, `SELECT COALESCE(chat_id, external_id) FROM channel_links WHERE user_id = $1 AND channel = $2`, uid, ch).Scan(&chat)
	return chat, err == nil
}

// Unlink removes a link.
func (l *Links) Unlink(ctx context.Context, uid uuid.UUID, ch string) error {
	_, err := l.Pool.Exec(ctx, `DELETE FROM channel_links WHERE user_id = $1 AND channel = $2`, uid, ch)
	if err == nil && l.Events != nil {
		l.Events.Publish(ctx, events.Event{Type: events.ConnectionsChanged, UserID: &uid, Data: map[string]string{"channel": ch}})
	}
	return err
}

// Channel is a channel of the user as GET /me shows it.
type Channel struct {
	Type     string    `json:"type"`
	Account  string    `json:"account"`
	LinkedAt time.Time `json:"linkedAt"`
}

// Of lists the user's linked channels.
func (l *Links) Of(ctx context.Context, uid uuid.UUID) ([]Channel, error) {
	rows, err := l.Pool.Query(ctx, `SELECT channel, COALESCE(account,''), linked_at FROM channel_links WHERE user_id = $1 ORDER BY channel`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Channel{}
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.Type, &c.Account, &c.LinkedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Routes mounts /channels (tech §3.5).
func (l *Links) Routes(r chi.Router) {
	r.Get("/channels", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		linked, err := l.Of(r.Context(), p.UserID)
		if err != nil {
			return err
		}
		avail := []map[string]string{}
		for _, ch := range []string{domain.ChannelTelegram, domain.ChannelVKWS} {
			if l.Enabled[ch] {
				avail = append(avail, map[string]string{"type": ch, "bot": l.BotNames[ch]})
			}
		}
		httpx.JSON(w, 200, map[string]any{"linked": linked, "available": avail})
		return nil
	}))
	r.Post("/channels/{type}/link-code", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		code, exp, err := l.NewCode(r.Context(), p.UserID, chi.URLParam(r, "type"))
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"code": code, "expiresAt": exp, "bot": l.BotNames[chi.URLParam(r, "type")]})
		return nil
	}))
	r.Delete("/channels/{type}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		if err := l.Unlink(r.Context(), p.UserID, chi.URLParam(r, "type")); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}

type uuidLike = uuid.UUID

func errorsAs(err error, target any) bool { return errors.As(err, target) }
