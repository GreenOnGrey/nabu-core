package comm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Directions and states of a commitment (R11).
const (
	Mine   = "mine"
	Theirs = "theirs"

	Open      = "open"
	Done      = "done"
	Cancelled = "cancelled"

	ByUser      = "user"
	ByAgent     = "agent"
	ByCollector = "collector"
)

// Date is a calendar day in JSON: "2026-10-16".
type Date struct{ time.Time }

const dateLayout = "2006-01-02"

// MarshalJSON writes the day.
func (d Date) MarshalJSON() ([]byte, error) { return []byte(`"` + d.Format(dateLayout) + `"`), nil }

// UnmarshalJSON reads the day.
func (d *Date) UnmarshalJSON(b []byte) error {
	v, err := ParseDate(strings.Trim(string(b), `"`))
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// ParseDate reads "2026-10-16".
func ParseDate(s string) (Date, error) {
	t, err := time.Parse(dateLayout, strings.TrimSpace(s))
	return Date{t}, err
}

// Commitment is an obligation found in a note (R11).
type Commitment struct {
	ID        uuid.UUID  `json:"id"`
	Text      string     `json:"text"`
	Direction string     `json:"direction"`
	Person    *Tag       `json:"person"`
	Topics    []Tag      `json:"topics"`
	Due       *Date      `json:"due"`
	Overdue   bool       `json:"overdue"`
	State     string     `json:"state"`
	ClosedBy  *string    `json:"closedBy"`
	ClosedAt  *time.Time `json:"closedAt"`
	NoteID    uuid.UUID  `json:"noteId"`
	NoteTitle string     `json:"noteTitle"`
	// BasisURL opens the message the commitment comes from; CloseBasisURL —
	// the message the collector closed it by (R12).
	BasisURL      string    `json:"basisUrl"`
	CloseBasisURL string    `json:"closeBasisUrl,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
}

// CommitmentFilter narrows the commitments.
type CommitmentFilter struct {
	State     string // open (default) | closed | all
	Direction string
	Person    *uuid.UUID
	Topic     *uuid.UUID
	Note      *uuid.UUID
	DueBefore *time.Time
	// Today is the current day of the user: overdue is counted from it.
	Today time.Time
}

const commitmentCols = `c.id, c.text, c.direction, c.due, c.state, c.closed_by, c.closed_at, c.note_id, n.title, c.created_at,
	p.id, COALESCE(NULLIF(p.name,''), p.email, ''), COALESCE(p.email, ''),
	COALESCE((SELECT m.url FROM comm_note_messages m WHERE m.user_id = c.user_id AND m.note_id = c.note_id AND m.message_id = c.basis_message_id), ''),
	-- the message that closed it may belong to another thread of the user
	COALESCE((SELECT m.url FROM comm_note_messages m WHERE m.user_id = c.user_id AND m.message_id = c.close_basis_message_id
		ORDER BY (m.note_id = c.note_id) DESC LIMIT 1), '')`

const commitmentFrom = ` FROM commitments c JOIN comm_notes n ON n.user_id = c.user_id AND n.id = c.note_id
	LEFT JOIN comm_people p ON p.id = c.person_id`

func scanCommitment(row pgx.Row, today time.Time) (*Commitment, error) {
	var c Commitment
	var due *time.Time
	var pid *uuid.UUID
	var pLabel, pEmail string
	if err := row.Scan(&c.ID, &c.Text, &c.Direction, &due, &c.State, &c.ClosedBy, &c.ClosedAt, &c.NoteID, &c.NoteTitle, &c.CreatedAt,
		&pid, &pLabel, &pEmail, &c.BasisURL, &c.CloseBasisURL); err != nil {
		return nil, err
	}
	if due != nil {
		c.Due = &Date{*due}
		c.Overdue = c.State == Open && !today.IsZero() && due.Format(dateLayout) < today.Format(dateLayout)
	}
	if pid != nil {
		c.Person = &Tag{ID: *pid, Kind: KindPerson, Label: pLabel, Email: pEmail}
	}
	c.Topics = []Tag{}
	return &c, nil
}

func listCommitments(ctx context.Context, tx pgx.Tx, uid uuid.UUID, f CommitmentFilter, limit int) ([]Commitment, error) {
	args := []any{uid}
	var cond strings.Builder
	add := func(c string, v any) {
		args = append(args, v)
		cond.WriteString(" AND " + strings.ReplaceAll(c, "?", fmt.Sprintf("$%d", len(args))))
	}
	switch f.State {
	case "", Open:
		cond.WriteString(` AND c.state = 'open'`)
	case "closed":
		cond.WriteString(` AND c.state <> 'open'`)
	case "all":
	default:
		return nil, apperr.BadRequest("invalid_state", "state is open, closed or all")
	}
	switch f.Direction {
	case "":
	case Mine, Theirs:
		add(`c.direction = ?`, f.Direction)
	default:
		return nil, apperr.BadRequest("invalid_direction", "direction is mine or theirs")
	}
	if f.Person != nil {
		add(`c.person_id = ?`, *f.Person)
	}
	if f.Topic != nil {
		add(`EXISTS (SELECT 1 FROM comm_note_tags t WHERE t.user_id = c.user_id AND t.note_id = c.note_id AND t.kind = 'topic' AND t.tag_id = ?)`, *f.Topic)
	}
	if f.Note != nil {
		add(`c.note_id = ?`, *f.Note)
	}
	if f.DueBefore != nil {
		add(`c.due < ?`, *f.DueBefore)
	}
	args = append(args, limit)
	rows, err := tx.Query(ctx, `SELECT `+commitmentCols+commitmentFrom+` WHERE c.user_id = $1`+cond.String()+
		fmt.Sprintf(` ORDER BY (c.state <> 'open'), c.due NULLS LAST, c.created_at, c.id LIMIT $%d`, len(args)), args...)
	if err != nil {
		return nil, err
	}
	out := []Commitment{}
	by := map[uuid.UUID][]int{}
	for rows.Next() {
		c, err := scanCommitment(rows, f.Today)
		if err != nil {
			rows.Close()
			return nil, err
		}
		by[c.NoteID] = append(by[c.NoteID], len(out))
		out = append(out, *c)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, err
	}
	notes := make([]uuid.UUID, 0, len(by))
	for id := range by {
		notes = append(notes, id)
	}
	trows, err := tx.Query(ctx, `SELECT t.note_id, tp.id, tp.name FROM comm_note_tags t JOIN comm_topics tp ON tp.id = t.tag_id
		WHERE t.user_id = $1 AND t.kind = 'topic' AND t.note_id = ANY($2) ORDER BY tp.name`, uid, notes)
	if err != nil {
		return nil, err
	}
	defer trows.Close()
	for trows.Next() {
		var nid uuid.UUID
		t := Tag{Kind: KindTopic}
		if err := trows.Scan(&nid, &t.ID, &t.Label); err != nil {
			return nil, err
		}
		for _, i := range by[nid] {
			out[i].Topics = append(out[i].Topics, t)
		}
	}
	return out, trows.Err()
}

// Commitments lists the commitments of the user: the open ones by their due
// day, those without a day last.
func (s *Service) Commitments(ctx context.Context, uid uuid.UUID, f CommitmentFilter, limit int) ([]Commitment, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	var out []Commitment
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var err error
		out, err = listCommitments(ctx, tx, uid, f, limit)
		return err
	})
	return out, err
}

// SaveCommitment is a commitment the collector found in a thread.
type SaveCommitment struct {
	ItemID         uuid.UUID
	Source         string
	ThreadKey      string
	DayKey         string
	Text           string
	Direction      string
	PersonEmail    string
	PersonName     string
	Due            *Date
	BasisMessageID string
}

// SaveCommitment adds a commitment to the note of a thread (R11); the same
// text with the same basis is not added twice (tech §5.1).
func (s *Service) SaveCommitment(ctx context.Context, uid uuid.UUID, in SaveCommitment) (uuid.UUID, error) {
	in.Text = clipRunes(masked(strings.TrimSpace(in.Text)), MaxText)
	if in.Text == "" {
		return uuid.Nil, apperr.Unprocessable("invalid_commitment", "the text is required")
	}
	if in.Direction != Mine && in.Direction != Theirs {
		return uuid.Nil, apperr.Unprocessable("invalid_commitment", "direction is mine or theirs")
	}
	var id uuid.UUID
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var note uuid.UUID
		err := tx.QueryRow(ctx, `SELECT id FROM comm_notes WHERE user_id = $1 AND item_id = $2 AND source = $3 AND thread_key = $4 AND day_key = $5`,
			uid, in.ItemID, in.Source, in.ThreadKey, in.DayKey).Scan(&note)
		if postgres.IsNoRows(err) {
			return apperr.NotFound("note_missing", "save the note of the thread first")
		}
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `SELECT id FROM commitments WHERE user_id = $1 AND note_id = $2 AND lower(text) = lower($3)
			AND COALESCE(basis_message_id,'') = $4`, uid, note, in.Text, in.BasisMessageID).Scan(&id)
		if err == nil {
			return nil // already there, in any state: a closed one is not reopened by a repeated batch
		}
		if !postgres.IsNoRows(err) {
			return err
		}
		var person *uuid.UUID
		if email := strings.ToLower(strings.TrimSpace(in.PersonEmail)); strings.Contains(email, "@") {
			pid, err := upsertPerson(ctx, tx, uid, email, in.PersonName)
			if err != nil {
				return err
			}
			person = &pid
		}
		var due *time.Time
		if in.Due != nil {
			due = &in.Due.Time
		}
		return tx.QueryRow(ctx, `INSERT INTO commitments (user_id, note_id, text, direction, person_id, due, basis_message_id)
			VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,'')) RETURNING id`, uid, note, in.Text, in.Direction, person, due, in.BasisMessageID).Scan(&id)
	})
	if err == nil {
		s.publish(ctx, events.CommitmentsChanged, uid)
	}
	return id, err
}

// CommitmentPatch changes a commitment: its state, its due day (R12).
type CommitmentPatch struct {
	State *string
	// SetDue with a nil Due clears the day.
	SetDue bool
	Due    *Date
	// CloseBasisMessageID is the message the collector closed it by.
	CloseBasisMessageID string
}

// UpdateCommitment closes, cancels or returns a commitment and changes its
// due day; by is who did it (R12).
func (s *Service) UpdateCommitment(ctx context.Context, uid, id uuid.UUID, p CommitmentPatch, by string) (*Commitment, error) {
	var out *Commitment
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, `SELECT state FROM commitments WHERE user_id = $1 AND id = $2 FOR UPDATE`, uid, id).Scan(&state)
		if postgres.IsNoRows(err) {
			return apperr.NotFound("not_found", "commitment not found")
		}
		if err != nil {
			return err
		}
		if p.State != nil {
			switch *p.State {
			case Open:
				_, err = tx.Exec(ctx, `UPDATE commitments SET state = 'open', closed_by = NULL, closed_at = NULL, close_basis_message_id = NULL, updated_at = now()
					WHERE user_id = $1 AND id = $2`, uid, id)
			case Done, Cancelled:
				if state == Open { // closing a closed commitment keeps who closed it first
					_, err = tx.Exec(ctx, `UPDATE commitments SET state = $3, closed_by = $4, closed_at = now(), close_basis_message_id = NULLIF($5,''), updated_at = now()
						WHERE user_id = $1 AND id = $2`, uid, id, *p.State, by, p.CloseBasisMessageID)
				} else if state != *p.State {
					_, err = tx.Exec(ctx, `UPDATE commitments SET state = $3, updated_at = now() WHERE user_id = $1 AND id = $2`, uid, id, *p.State)
				}
			default:
				return apperr.Unprocessable("invalid_state", "state is open, done or cancelled").With("field", "state")
			}
			if err != nil {
				return err
			}
		}
		if p.SetDue {
			var due *time.Time
			if p.Due != nil {
				due = &p.Due.Time
			}
			if _, err := tx.Exec(ctx, `UPDATE commitments SET due = $3, updated_at = now() WHERE user_id = $1 AND id = $2`, uid, id, due); err != nil {
				return err
			}
		}
		out, err = scanCommitment(tx.QueryRow(ctx, `SELECT `+commitmentCols+commitmentFrom+` WHERE c.user_id = $1 AND c.id = $2`, uid, id), s.clock())
		return err
	})
	if err == nil {
		s.publish(ctx, events.CommitmentsChanged, uid)
	}
	return out, err
}

// ─── digest (R16, tech §7) ──────────────────────────────────────────

// Digest groups of commitments.
const (
	GroupOverdue = "overdue"
	GroupDue     = "due"
	GroupTheirs  = "theirs"
	GroupUndated = "undated"
)

// DigestItem is a commitment of a digest with its group.
type DigestItem struct {
	Commitment
	Group string `json:"group"`
}

// Digest selects what to remind of: the overdue commitments, those due today
// (period "day") or within the current week Monday–Sunday (period "week"),
// then what is owed to the user; the weekly digest adds those without a day.
// today is the current day in the time zone of the user.
func (s *Service) Digest(ctx context.Context, uid uuid.UUID, period string, today time.Time) ([]DigestItem, error) {
	today = time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
	end := today // inclusive
	if period == "week" {
		end = today.AddDate(0, 0, (7-int(today.Weekday()))%7) // Sunday of this week
	}
	all, err := s.Commitments(ctx, uid, CommitmentFilter{State: Open, Today: today}, 500)
	if err != nil {
		return nil, err
	}
	groups := map[string][]DigestItem{}
	for _, c := range all {
		var g string
		switch {
		case c.Due == nil:
			if period != "week" {
				continue
			}
			g = GroupUndated
		case c.Due.After(end):
			continue
		case c.Direction == Theirs:
			g = GroupTheirs
		case c.Due.Before(today):
			g = GroupOverdue
		default:
			g = GroupDue
		}
		groups[g] = append(groups[g], DigestItem{Commitment: c, Group: g})
	}
	out := []DigestItem{}
	for _, g := range []string{GroupOverdue, GroupDue, GroupTheirs, GroupUndated} {
		out = append(out, groups[g]...)
	}
	return out, nil
}
