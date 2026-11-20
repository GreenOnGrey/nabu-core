package comm

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// The days of communication of the records and the graph by day (FTR.NAB.MEM-0001 R20, tech §7.1, arch A6).
// comm_refresh_notes and comm_refresh_days (migration 0026) do the work in the database; every change of records or
// tags calls them in its own transaction, so the aggregate never lags behind the records.

// Levels of a record (FTR.NAB.MEM-0001 R1).
const (
	LevelDay    = "day"
	LevelWeek   = "week"
	LevelMonth  = "month"
	LevelThread = "thread"
)

// refreshNotes recounts the days, the period of the records of a day or a thread and the graph of their days.
func refreshNotes(ctx context.Context, tx pgx.Tx, uid uuid.UUID, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `SELECT comm_refresh_notes($1, $2)`, uid, ids)
	return err
}

// daysOf are the days of the records that match cond (over comm_notes n, $1 is the user): taken before the records
// are deleted, so the graph of these days is recounted after.
func daysOf(ctx context.Context, tx pgx.Tx, uid uuid.UUID, cond string, args ...any) ([]time.Time, error) {
	var days []time.Time
	err := tx.QueryRow(ctx, `SELECT COALESCE(array_agg(DISTINCT d.day), '{}') FROM comm_note_days d
		JOIN comm_notes n ON n.user_id = d.user_id AND n.id = d.note_id WHERE n.user_id = $1 AND `+cond, append([]any{uid}, args...)...).Scan(&days)
	return days, err
}

// refreshDays recounts the graph of the given days of the user.
func refreshDays(ctx context.Context, tx pgx.Tx, uid uuid.UUID, days []time.Time) error {
	if len(days) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `SELECT comm_refresh_days($1, $2::date[])`, uid, days)
	return err
}

// notesWithTag are the records that carry a tag.
func notesWithTag(ctx context.Context, tx pgx.Tx, uid uuid.UUID, kind string, tag uuid.UUID) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	err := tx.QueryRow(ctx, `SELECT COALESCE(array_agg(note_id), '{}') FROM comm_note_tags WHERE user_id = $1 AND kind = $2 AND tag_id = $3`,
		uid, kind, tag).Scan(&ids)
	return ids, err
}

// capEdges keeps, of the links between the chosen nodes, those among the perNode strongest of either end, then
// the total strongest (FTR.NAB.MEM-0001 R24, tech §7.2): a node with weak links keeps its strongest one.
func capEdges(edges []Edge, perNode, total int) []Edge {
	if perNode <= 0 && total <= 0 {
		return edges
	}
	byDays := func(l []Edge) {
		sort.SliceStable(l, func(i, j int) bool {
			if l[i].Days != l[j].Days {
				return l[i].Days > l[j].Days
			}
			if l[i].A != l[j].A {
				return l[i].A.String() < l[j].A.String()
			}
			return l[i].B.String() < l[j].B.String()
		})
	}
	sorted := append([]Edge(nil), edges...)
	byDays(sorted)
	keep := make([]bool, len(sorted))
	if perNode > 0 {
		count := map[uuid.UUID]int{}
		for i, e := range sorted {
			if count[e.A] < perNode || count[e.B] < perNode {
				keep[i] = true
			}
			count[e.A]++
			count[e.B]++
		}
	} else {
		for i := range keep {
			keep[i] = true
		}
	}
	out := make([]Edge, 0, len(sorted))
	for i, e := range sorted {
		if keep[i] {
			out = append(out, e)
		}
	}
	if total > 0 && len(out) > total {
		out = out[:total]
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].A != out[j].A {
			return out[i].A.String() < out[j].A.String()
		}
		return out[i].B.String() < out[j].B.String()
	})
	return out
}

// LastCompaction is when the records of the user were last compacted (FTR.NAB.MEM-0001 R17); nil — never.
func (s *Service) LastCompaction(ctx context.Context, uid uuid.UUID) (*time.Time, error) {
	var at *time.Time
	err := s.Pool.QueryRow(ctx, `SELECT last_at FROM comm_compact_state WHERE user_id = $1`, uid).Scan(&at)
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return at, err
}

// SnapshotLevels writes the number of records of every user by level and the numbers of the commitments for the
// current day (UTC): the chart «Records by level» and the counters of the usage read it, since the administration sees no records (FTR.NAB.MEM-0001 R25,
// R26). A later snapshot of the same day replaces the earlier one, so a day ends with its last state. It is the
// maintenance of the worker: it sees the rows of every user.
func (s *Service) SnapshotLevels(ctx context.Context) error {
	return postgres.AsMaintenance(ctx, s.Pool, func(tx pgx.Tx) error {
		// every replica of the worker runs the housekeeping: one snapshot at a time
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('nabu:comm:levels'))`); err != nil {
			return err
		}
		day := dayOf(s.clock())
		if _, err := tx.Exec(ctx, `DELETE FROM comm_level_days WHERE day = $1`, day); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO comm_level_days (user_id, day, level, records)
			SELECT user_id, $1, level, count(*) FROM comm_notes GROUP BY user_id, level`, day); err != nil {
			return err
		}
		// the commitments as numbers: open, overdue and closed on the day
		_, err := tx.Exec(ctx, `INSERT INTO comm_level_days (user_id, day, level, records)
			SELECT user_id, $1, k, n FROM (
				SELECT user_id, 'commitments_open' AS k, count(*) FILTER (WHERE state = 'open') AS n FROM commitments GROUP BY user_id
				UNION ALL SELECT user_id, 'commitments_overdue', count(*) FILTER (WHERE state = 'open' AND due < $1) FROM commitments GROUP BY user_id
				UNION ALL SELECT user_id, 'commitments_closed', count(*) FILTER (WHERE state <> 'open' AND (closed_at AT TIME ZONE 'UTC')::date = $1) FROM commitments GROUP BY user_id
			) x WHERE n > 0`, day)
		return err
	})
}

// foldE folds a text for a search by a part of it: case by the collation of the search, and «ё» as «е»
// (FTR.NAB.MEM-0001 R22): the collation alone keeps «ё» apart.
func foldE(expr string) string {
	return "replace(lower(" + postgres.Fold(expr) + "), 'ё', 'е')"
}

// localDay is the date of a time (an SQL expression) in the time zone of the user $1: the dates of the records are
// in it (comm_refresh_notes).
func localDay(at string) string {
	return "(" + at + " AT TIME ZONE comm_safe_tz((SELECT u.timezone FROM users u WHERE u.id = $1)))::date"
}
