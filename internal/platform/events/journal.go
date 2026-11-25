package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The journal of the user stream (FTR.NAB.CMN-0001 tech §3.2, decision 27):
// Postgres NOTIFY carries an event to the api pods but keeps nothing, so the
// events published while a listener reconnects are lost. The rows of the
// journal let a reconnecting site replay what it missed.
const (
	// ReplayLimit is the most events one connection gets on reconnect: the
	// newest ones, so the end of a turn is never cut off.
	ReplayLimit = 1000
	// JournalRetention is how long an event waits for a reconnect: longer than
	// any browser stays away with the same tab, and short enough for the table.
	JournalRetention = time.Hour
)

// keep writes the event into the journal and returns its id: the connection of
// a browser replays the events it missed by this id (tech spec §3.2, decision
// 27). Deltas are not kept — a reconnected site reads the message itself — and
// an event without a user has no stream to replay to.
func (p *PGPublisher) keep(ctx context.Context, e Event) int64 {
	if e.UserID == nil || e.Type == MessageDelta {
		return 0
	}
	data, err := json.Marshal(e.Data)
	if err != nil {
		slog.ErrorContext(ctx, "encode event data", "err", err, "type", e.Type)
		return 0
	}
	var id int64
	// the journal is a help, not a condition of delivery: the live event still
	// goes out when the write fails
	if err := p.pool.QueryRow(context.WithoutCancel(ctx),
		`INSERT INTO events (user_id, type, data) VALUES ($1, $2, $3::jsonb) RETURNING id`,
		*e.UserID, e.Type, string(data)).Scan(&id); err != nil {
		slog.ErrorContext(ctx, "keep event", "err", err, "type", e.Type)
		return 0
	}
	return id
}

// Replay reads the newest events of one user after id, oldest first: the end of
// a turn matters more than its beginning when the gap is wider than the limit.
func Replay(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID, after int64, limit int) ([]Event, error) {
	rows, err := pool.Query(ctx, `SELECT id, type, data FROM (
			SELECT id, type, data FROM events WHERE user_id = $1 AND id > $2 ORDER BY id DESC LIMIT $3
		) t ORDER BY id`, userID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var (
			id   int64
			typ  string
			data []byte
		)
		if err := rows.Scan(&id, &typ, &data); err != nil {
			return nil, err
		}
		out = append(out, Event{Type: typ, UserID: &userID, Seq: id, Data: json.RawMessage(data)})
	}
	return out, rows.Err()
}

// Cleanup deletes the events older than the retention: the worker calls it.
func Cleanup(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `DELETE FROM events WHERE at < now() - make_interval(secs => $1)`, JournalRetention.Seconds())
	return err
}
