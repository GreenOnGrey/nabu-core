// Package comm keeps the memory of communications (FTR.NAB.CMN-0003): notes
// of mail threads and chats with people and topics, the commitments found in
// them, the table, the graph and the search over them.
//
// Every query runs in postgres.AsUser: the row policies of the comm_* tables
// show a connection only the rows of the user it named (R22, tech §11).
package comm

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Limits of a note (tech §5.1).
const (
	MaxTitle   = 200
	MaxSummary = 4000
	MaxTopics  = 5
	MaxText    = 500 // a commitment
	excerptLen = 240
)

// Tag kinds.
const (
	KindPerson = "person"
	KindTopic  = "topic"
)

// ConsentVersion names the text of the consent the user accepted (R21).
const ConsentVersion = "1"

// Settings are the instance settings of the memory of communications
// (R21, R23, R27, R29; settings key "comm").
type Settings struct {
	Enabled         bool    `json:"enabled"`
	RetentionDays   int     `json:"retentionDays"`
	Model           *Choice `json:"model"`
	Embeddings      *Choice `json:"embeddings"`
	MonthlyLimitUSD float64 `json:"monthlyLimitUsd"`
}

// Choice is a model of a connection.
type Choice struct {
	ConnectionID string `json:"connectionId"`
	Model        string `json:"model"`
}

// DefaultRetentionDays is the default retention of notes (R23).
const DefaultRetentionDays = 180

// Service is the store of the memory of communications.
type Service struct {
	Pool   *pgxpool.Pool
	Events events.Publisher
	// Embedder gives the model of the search by meaning the administrator
	// chose; nil or a nil result — the search is by words (R20).
	Embedder func(ctx context.Context) *Embedder
	vectors  atomic.Bool // the table of vectors exists (EnsureVectors)
	now      func() time.Time
}

func (s *Service) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Service) as(ctx context.Context, uid uuid.UUID, fn func(tx pgx.Tx) error) error {
	return postgres.AsUser(ctx, s.Pool, uid, fn)
}

func (s *Service) publish(ctx context.Context, typ string, uid uuid.UUID) {
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: typ, UserID: &uid, Data: map[string]any{}})
	}
}

// LoadSettings reads the instance settings.
func (s *Service) LoadSettings(ctx context.Context) (Settings, error) {
	st := Settings{RetentionDays: DefaultRetentionDays}
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT value FROM settings WHERE key = 'comm'`).Scan(&raw)
	if postgres.IsNoRows(err) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return st, err
	}
	if st.RetentionDays <= 0 {
		st.RetentionDays = DefaultRetentionDays
	}
	return st, nil
}

// SettingsPatch changes the instance settings; nil keeps a value.
type SettingsPatch struct {
	Enabled         *bool           `json:"enabled"`
	RetentionDays   *int            `json:"retentionDays"`
	Model           json.RawMessage `json:"model"`
	Embeddings      json.RawMessage `json:"embeddings"`
	MonthlyLimitUSD *float64        `json:"monthlyLimitUsd"`
}

// SaveSettings applies a patch of the instance settings (R29).
func (s *Service) SaveSettings(ctx context.Context, p SettingsPatch) (Settings, error) {
	var out Settings
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('nabu:settings:comm'))`); err != nil {
			return err
		}
		st, err := s.LoadSettings(ctx)
		if err != nil {
			return err
		}
		if p.Enabled != nil {
			st.Enabled = *p.Enabled
		}
		if p.RetentionDays != nil {
			if *p.RetentionDays < 1 || *p.RetentionDays > 3650 {
				return apperr.Unprocessable("invalid_retention", "retentionDays is between 1 and 3650").With("field", "retentionDays")
			}
			st.RetentionDays = *p.RetentionDays
		}
		if p.MonthlyLimitUSD != nil {
			if *p.MonthlyLimitUSD < 0 {
				return apperr.Unprocessable("invalid_limit", "monthlyLimitUsd is not negative").With("field", "monthlyLimitUsd")
			}
			st.MonthlyLimitUSD = *p.MonthlyLimitUSD
		}
		for _, f := range []struct {
			raw json.RawMessage
			dst **Choice
		}{{p.Model, &st.Model}, {p.Embeddings, &st.Embeddings}} {
			if len(f.raw) == 0 {
				continue
			}
			var c *Choice
			if err := json.Unmarshal(f.raw, &c); err != nil {
				return apperr.BadRequest("invalid_json", "invalid model choice")
			}
			if c != nil && (c.ConnectionID == "" || c.Model == "") {
				return apperr.Unprocessable("invalid_model", "connectionId and model are required").With("field", "model")
			}
			*f.dst = c
		}
		b, _ := json.Marshal(st)
		if _, err := tx.Exec(ctx, `INSERT INTO settings (key, value) VALUES ('comm', $1)
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, b); err != nil {
			return err
		}
		out = st
		return nil
	})
	return out, err
}

// Allowed fails with comm_disabled unless the administrator allowed the
// memory of communications (R21).
func (s *Service) Allowed(ctx context.Context) (Settings, error) {
	st, err := s.LoadSettings(ctx)
	if err != nil {
		return st, err
	}
	if !st.Enabled {
		return st, apperr.Forbidden("comm_disabled", "the memory of communications is not allowed in this instance")
	}
	return st, nil
}

// UserState is what the user consented to and how much is collected.
type UserState struct {
	ConsentAt       *time.Time `json:"consentAt"`
	Notes           int        `json:"notes"`
	OpenCommitments int        `json:"openCommitments"`
}

// State loads the consent and the counters of a user.
func (s *Service) State(ctx context.Context, uid uuid.UUID) (UserState, error) {
	var st UserState
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT consent_at, notes_count FROM comm_settings WHERE user_id = $1`, uid).Scan(&st.ConsentAt, &st.Notes)
		if err != nil && !postgres.IsNoRows(err) {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM commitments WHERE user_id = $1 AND state = 'open'`, uid).Scan(&st.OpenCommitments)
	})
	return st, err
}

// Consented reports whether the user accepted the consent (R21).
func (s *Service) Consented(ctx context.Context, uid uuid.UUID) (bool, error) {
	var at *time.Time
	err := s.Pool.QueryRow(ctx, `SELECT consent_at FROM comm_settings WHERE user_id = $1`, uid).Scan(&at)
	if postgres.IsNoRows(err) {
		return false, nil
	}
	return at != nil, err
}

// Consent records that the user accepted the consent.
func (s *Service) Consent(ctx context.Context, uid uuid.UUID) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO comm_settings (user_id, consent_at, consent_version) VALUES ($1, now(), $2)
		ON CONFLICT (user_id) DO UPDATE SET consent_at = COALESCE(comm_settings.consent_at, now()), consent_version = $2, updated_at = now()`,
		uid, ConsentVersion)
	if err == nil {
		s.publish(ctx, events.CommChanged, uid)
	}
	return err
}

// DeleteAll deletes everything collected for the user: notes, commitments,
// people, topics, cursors and the consent; the facts of the memory stay (R24).
func (s *Service) DeleteAll(ctx context.Context, uid uuid.UUID, confirm string) error {
	if confirm != "all" {
		return apperr.BadRequest("confirmation_required", `send {"confirm":"all"} to delete everything collected`)
	}
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		for _, q := range []string{
			`DELETE FROM comm_notes WHERE user_id = $1`, // messages, tags and commitments cascade
			`DELETE FROM comm_people WHERE user_id = $1`,
			`DELETE FROM comm_topics WHERE user_id = $1`,
			`DELETE FROM comm_tombstones WHERE user_id = $1`,
			`DELETE FROM comm_skipped WHERE user_id = $1`,
			`DELETE FROM comm_cursors WHERE user_id = $1`,
			`DELETE FROM comm_batches WHERE user_id = $1`,
			`UPDATE comm_settings SET consent_at = NULL, notes_count = 0, updated_at = now() WHERE user_id = $1`,
		} {
			if _, err := tx.Exec(ctx, q, uid); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		s.publish(ctx, events.CommChanged, uid)
		s.publish(ctx, events.CommitmentsChanged, uid)
	}
	return err
}

func clipRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func nameKey(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

func masked(s string) string {
	out, n := Mask(s)
	if n > 0 {
		metrics.MemoryMaskedSecrets.Add(float64(n))
	}
	return out
}
