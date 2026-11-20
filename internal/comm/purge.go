package comm

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Purged is what a run of the retention removed.
type Purged struct {
	Notes, People, Topics, Tombstones, Batches int64
	// ChatCommitments are the closed commitments from the chat without a note (decision 57).
	ChatCommitments int64
}

// Purge applies the retention of notes (R23, tech §10; in months — FTR.NAB.MEM-0001 R16): a note whose last
// message is older than the retention goes with its messages, tags and closed
// commitments — unless it has an open commitment, which keeps it. People and
// topics left without notes follow, and so do old tombstones and batches. A commitment from the chat without a
// note goes once it has been closed longer than the retention (FTR.NAB.CMN-0003 R23, tech decision 57).
// It is the maintenance of the cleaner: it sees the rows of every user.
func (s *Service) Purge(ctx context.Context) (Purged, error) {
	var out Purged
	st, err := s.LoadSettings(ctx)
	if err != nil {
		return out, err
	}
	err = postgres.AsMaintenance(ctx, s.Pool, func(tx pgx.Tx) error {
		// FTR.NAB.MEM-0001 R16: the retention is in months; a record of a day or a week the compaction did not
		// replace goes by the same rule. The days of the records that go are taken first: the graph of those days
		// is recounted after them.
		const doomed = `n.last_at < now() - make_interval(months => $1)
			AND NOT EXISTS (SELECT 1 FROM commitments c WHERE c.user_id = n.user_id AND c.note_id = n.id AND c.state = 'open')`
		rows, err := tx.Query(ctx, `SELECT d.user_id, array_agg(DISTINCT d.day) FROM comm_note_days d
			JOIN comm_notes n ON n.user_id = d.user_id AND n.id = d.note_id WHERE `+doomed+` GROUP BY 1`, st.RetentionMonths)
		if err != nil {
			return err
		}
		days := map[uuid.UUID][]time.Time{}
		for rows.Next() {
			var uid uuid.UUID
			var ds []time.Time
			if err := rows.Scan(&uid, &ds); err != nil {
				rows.Close()
				return err
			}
			days[uid] = ds
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		// the counters of the users go down with their notes
		if err := tx.QueryRow(ctx, `WITH gone AS (
				DELETE FROM comm_notes n WHERE `+doomed+`
				RETURNING n.user_id),
			per AS (SELECT user_id, count(*) AS n FROM gone GROUP BY user_id),
			upd AS (UPDATE comm_settings cs SET notes_count = GREATEST(cs.notes_count - per.n, 0) FROM per WHERE cs.user_id = per.user_id)
			SELECT COALESCE(sum(n), 0) FROM per`, st.RetentionMonths).Scan(&out.Notes); err != nil {
			return err
		}
		for uid, ds := range days {
			if err := refreshDays(ctx, tx, uid, ds); err != nil {
				return err
			}
		}
		for _, q := range []struct {
			dst *int64
			sql string
		}{
			{&out.ChatCommitments, `DELETE FROM commitments WHERE note_id IS NULL AND state <> 'open' AND closed_at < now() - make_interval(months => $1)`},
			{&out.People, `DELETE FROM comm_people p WHERE NOT EXISTS (SELECT 1 FROM comm_note_tags t WHERE t.user_id = p.user_id AND t.kind = 'person' AND t.tag_id = p.id)
				AND NOT EXISTS (SELECT 1 FROM commitments c WHERE c.person_id = p.id)`},
			{&out.Topics, `DELETE FROM comm_topics p WHERE NOT EXISTS (SELECT 1 FROM comm_note_tags t WHERE t.user_id = p.user_id AND t.kind = 'topic' AND t.tag_id = p.id)`},
			{&out.Tombstones, `DELETE FROM comm_tombstones WHERE deleted_at < now() - make_interval(months => $1)`},
			{&out.Batches, `DELETE FROM comm_batches WHERE created_at < now() - interval '7 days'`},
			{new(int64), `DELETE FROM comm_skipped WHERE seen_at < now() - interval '90 days'`},
		} {
			var tag interface{ RowsAffected() int64 }
			var err error
			if q.dst == &out.Tombstones || q.dst == &out.ChatCommitments {
				tag, err = tx.Exec(ctx, q.sql, st.RetentionMonths)
			} else {
				tag, err = tx.Exec(ctx, q.sql)
			}
			if err != nil {
				return err
			}
			*q.dst = tag.RowsAffected()
		}
		return nil
	})
	return out, err
}
