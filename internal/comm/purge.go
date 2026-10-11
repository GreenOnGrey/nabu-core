package comm

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Purged is what a run of the retention removed.
type Purged struct {
	Notes, People, Topics, Tombstones, Batches int64
}

// Purge applies the retention of notes (R23, tech §10): a note whose last
// message is older than the retention goes with its messages, tags and closed
// commitments — unless it has an open commitment, which keeps it. People and
// topics left without notes follow, and so do old tombstones and batches.
// It is the maintenance of the cleaner: it sees the rows of every user.
func (s *Service) Purge(ctx context.Context) (Purged, error) {
	var out Purged
	st, err := s.LoadSettings(ctx)
	if err != nil {
		return out, err
	}
	err = postgres.AsMaintenance(ctx, s.Pool, func(tx pgx.Tx) error {
		// the counters of the users go down with their notes
		if err := tx.QueryRow(ctx, `WITH gone AS (
				DELETE FROM comm_notes n WHERE n.last_at < now() - make_interval(days => $1)
					AND NOT EXISTS (SELECT 1 FROM commitments c WHERE c.user_id = n.user_id AND c.note_id = n.id AND c.state = 'open')
				RETURNING n.user_id),
			per AS (SELECT user_id, count(*) AS n FROM gone GROUP BY user_id),
			upd AS (UPDATE comm_settings cs SET notes_count = GREATEST(cs.notes_count - per.n, 0) FROM per WHERE cs.user_id = per.user_id)
			SELECT COALESCE(sum(n), 0) FROM per`, st.RetentionDays).Scan(&out.Notes); err != nil {
			return err
		}
		for _, q := range []struct {
			dst *int64
			sql string
		}{
			{&out.People, `DELETE FROM comm_people p WHERE NOT EXISTS (SELECT 1 FROM comm_note_tags t WHERE t.user_id = p.user_id AND t.kind = 'person' AND t.tag_id = p.id)
				AND NOT EXISTS (SELECT 1 FROM commitments c WHERE c.person_id = p.id)`},
			{&out.Topics, `DELETE FROM comm_topics p WHERE NOT EXISTS (SELECT 1 FROM comm_note_tags t WHERE t.user_id = p.user_id AND t.kind = 'topic' AND t.tag_id = p.id)`},
			{&out.Tombstones, `DELETE FROM comm_tombstones WHERE deleted_at < now() - make_interval(days => $1)`},
			{&out.Batches, `DELETE FROM comm_batches WHERE created_at < now() - interval '7 days'`},
			{new(int64), `DELETE FROM comm_skipped WHERE seen_at < now() - interval '90 days'`},
		} {
			var tag interface{ RowsAffected() int64 }
			var err error
			if q.dst == &out.Tombstones {
				tag, err = tx.Exec(ctx, q.sql, st.RetentionDays)
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
