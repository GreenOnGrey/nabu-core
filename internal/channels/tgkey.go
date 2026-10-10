package channels

import (
	"context"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// keyAlphabet is base32 without the look-alike 0, O, 1, I, L (tech §7).
const keyAlphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

// Attempts of binding (R13): five wrong keys in an hour block the account for an hour.
const (
	MaxBindAttempts = 5
	BindBlock       = time.Hour
)

// Keys are the personal keys of Telegram and the bindings (R13; arch §5.1).
type Keys struct {
	Pool   *pgxpool.Pool
	Pepper []byte
	Events events.Publisher
	now    func() time.Time
}

func (k *Keys) clock() time.Time {
	if k.now != nil {
		return k.now()
	}
	return time.Now()
}

// DerivePepper derives KEY_PEPPER from SECRETS_KEY when it is not set.
func DerivePepper(secretsKey []byte) []byte {
	p, _ := hkdf.Key(sha256.New, secretsKey, nil, "nabu telegram key pepper", 32)
	return p
}

// NormalizeKey drops case, dashes and spaces and the NB prefix (TG-04).
func NormalizeKey(s string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(s) {
		if r == '-' || r == ' ' || r == '\t' || r == '\n' {
			continue
		}
		b.WriteRune(r)
	}
	n := b.String()
	if len(n) == 14 && strings.HasPrefix(n, "NB") {
		n = n[2:]
	}
	return n
}

// LooksLikeKey reports whether a message may be a key.
func LooksLikeKey(s string) bool {
	n := NormalizeKey(s)
	if len(n) != 12 {
		return false
	}
	for _, r := range n {
		if !strings.ContainsRune(keyAlphabet, r) {
			return false
		}
	}
	return true
}

// Hash is HMAC-SHA256(KEY_PEPPER, normalized key) (TG-02).
func (k *Keys) Hash(key string) []byte {
	m := hmac.New(sha256.New, k.Pepper)
	m.Write([]byte(NormalizeKey(key)))
	return m.Sum(nil)
}

// NewKey makes NB-XXXX-XXXX-XXXX: 12 characters of 31 (≈59 bits).
func NewKey() string {
	b := make([]byte, 12)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(keyAlphabet))))
		b[i] = keyAlphabet[n.Int64()]
	}
	s := string(b)
	return "NB-" + s[:4] + "-" + s[4:8] + "-" + s[8:]
}

// Issue issues (or reissues) the key of a user; the key is returned only
// here and stored as a hash (TG-01).
func (k *Keys) Issue(ctx context.Context, uid uuid.UUID) (string, error) {
	for i := 0; i < 3; i++ {
		key := NewKey()
		_, err := k.Pool.Exec(ctx, `INSERT INTO telegram_keys (user_id, key_hash, issued_at) VALUES ($1,$2,now())
			ON CONFLICT (user_id) DO UPDATE SET key_hash = EXCLUDED.key_hash, issued_at = now()`, uid, k.Hash(key))
		if postgres.IsUniqueViolation(err) {
			continue
		}
		return key, err
	}
	return "", apperr.Conflict("key_collision", "try again")
}

// HasKey reports whether a key was issued.
func (k *Keys) HasKey(ctx context.Context, uid uuid.UUID) (bool, time.Time) {
	var at time.Time
	err := k.Pool.QueryRow(ctx, `SELECT issued_at FROM telegram_keys WHERE user_id = $1`, uid).Scan(&at)
	return err == nil, at
}

// BindResult is the outcome of a key sent to the bot.
type BindResult struct {
	UserID uuid.UUID
	// Previous is the Telegram account unbound by this binding (TG-06).
	Previous int64
	// Blocked: too many wrong keys, the key was not checked (TG-05).
	Blocked bool
	OK      bool
}

// Bind checks a key sent by a Telegram account and binds it.
func (k *Keys) Bind(ctx context.Context, tgID int64, username, key string) (BindResult, error) {
	var res BindResult
	since := k.clock().Add(-BindBlock)
	var n int
	if err := k.Pool.QueryRow(ctx, `SELECT count(*) FROM telegram_bind_attempts WHERE tg_user_id = $1 AND at > $2`, tgID, since).Scan(&n); err != nil {
		return res, err
	}
	if n >= MaxBindAttempts {
		res.Blocked = true
		return res, nil
	}
	err := postgres.InTx(ctx, k.Pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT user_id FROM telegram_keys WHERE key_hash = $1`, k.Hash(key)).Scan(&res.UserID)
		if postgres.IsNoRows(err) {
			_, err = tx.Exec(ctx, `INSERT INTO telegram_bind_attempts (tg_user_id, at) VALUES ($1,$2)`, tgID, k.clock())
			return err
		}
		if err != nil {
			return err
		}
		var prev int64
		if err := tx.QueryRow(ctx, `SELECT tg_user_id FROM telegram_bindings WHERE user_id = $1`, res.UserID).Scan(&prev); err == nil && prev != tgID {
			res.Previous = prev
		}
		// one account belongs to one user and one user has one account
		if _, err := tx.Exec(ctx, `DELETE FROM telegram_bindings WHERE tg_user_id = $1 OR user_id = $2`, tgID, res.UserID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO telegram_bindings (user_id, tg_user_id, username) VALUES ($1,$2,NULLIF($3,''))`,
			res.UserID, tgID, username); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM telegram_bind_attempts WHERE tg_user_id = $1`, tgID)
		res.OK = true
		return err
	})
	if err == nil && res.OK && k.Events != nil {
		uid := res.UserID
		k.Events.Publish(ctx, events.Event{Type: events.ConnectionsChanged, UserID: &uid, Data: map[string]string{"channel": domain.ChannelTelegram}})
	}
	return res, err
}

// Unbind removes the binding of a user and returns the unbound account.
func (k *Keys) Unbind(ctx context.Context, uid uuid.UUID) (int64, error) {
	var tg int64
	err := k.Pool.QueryRow(ctx, `DELETE FROM telegram_bindings WHERE user_id = $1 RETURNING tg_user_id`, uid).Scan(&tg)
	if postgres.IsNoRows(err) {
		return 0, nil
	}
	if err == nil && k.Events != nil {
		k.Events.Publish(ctx, events.Event{Type: events.ConnectionsChanged, UserID: &uid, Data: map[string]string{"channel": domain.ChannelTelegram}})
	}
	return tg, err
}

// UserOf finds the user bound to a Telegram account.
func (k *Keys) UserOf(ctx context.Context, tgID int64) (uuid.UUID, bool, error) {
	var uid uuid.UUID
	err := k.Pool.QueryRow(ctx, `SELECT user_id FROM telegram_bindings WHERE tg_user_id = $1`, tgID).Scan(&uid)
	if postgres.IsNoRows(err) {
		return uuid.Nil, false, nil
	}
	return uid, err == nil, err
}

// ChatOf is the private chat of a user's bound account (its id equals the account id).
func (k *Keys) ChatOf(ctx context.Context, uid uuid.UUID) (string, bool) {
	var tg int64
	if err := k.Pool.QueryRow(ctx, `SELECT tg_user_id FROM telegram_bindings WHERE user_id = $1`, uid).Scan(&tg); err != nil {
		return "", false
	}
	return strconv.FormatInt(tg, 10), true
}

// BindingOf returns the binding of a user.
func (k *Keys) BindingOf(ctx context.Context, uid uuid.UUID) *Binding {
	var b Binding
	var name *string
	var tg int64
	if err := k.Pool.QueryRow(ctx, `SELECT tg_user_id, username, bound_at FROM telegram_bindings WHERE user_id = $1`, uid).Scan(&tg, &name, &b.BoundAt); err != nil {
		return nil
	}
	b.Account = accountName(tg, name)
	return &b
}

func accountName(tg int64, username *string) string {
	if username != nil && *username != "" {
		return "@" + strings.TrimPrefix(*username, "@")
	}
	return "id " + strconv.FormatInt(tg, 10)
}
