package comm

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/collect"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Batch is what one run of the collector agent processes (arch §3.1): the
// threads that changed, by identifiers — never the text.
type Batch struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	TaskRunID *uuid.UUID
	ItemID    uuid.UUID
	Source    string
	Backfill  bool
	Threads   []collect.Thread
	Status    string
	RunID     *uuid.UUID
}

func (b *Batch) thread(key, dayKey string) *collect.Thread {
	for i := range b.Threads {
		if b.Threads[i].Key == key && b.Threads[i].DayKey == dayKey {
			return &b.Threads[i]
		}
	}
	return nil
}

// Cursor is the cursor of a source of the user ("" before the first collection).
func (s *Service) Cursor(ctx context.Context, uid, item uuid.UUID, source string) (string, error) {
	var c string
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT cursor FROM comm_cursors WHERE user_id = $1 AND item_id = $2 AND source = $3`, uid, item, source).Scan(&c)
		if postgres.IsNoRows(err) {
			return nil
		}
		return err
	})
	return c, err
}

// NewThreads leaves the threads worth a run of the agent: not deleted by the
// user (R10) and with a message that is not in their note yet (tech §6.2).
func (s *Service) NewThreads(ctx context.Context, uid, item uuid.UUID, source string, threads []collect.Thread) ([]collect.Thread, error) {
	var out []collect.Thread
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		for _, t := range threads {
			var gone bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM comm_tombstones WHERE user_id = $1 AND item_id = $2 AND source = $3 AND thread_key = $4 AND day_key = $5)`,
				uid, item, source, t.Key, t.DayKey).Scan(&gone); err != nil {
				return err
			}
			if gone {
				continue
			}
			ids := make([]string, len(t.Messages))
			for i, m := range t.Messages {
				ids[i] = m.ID
			}
			// known: in the note of the thread, or read by a run that saved nothing for it
			var known int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM (
					SELECT m.message_id FROM comm_note_messages m JOIN comm_notes n ON n.user_id = m.user_id AND n.id = m.note_id
					WHERE n.user_id = $1 AND n.item_id = $2 AND n.source = $3 AND n.thread_key = $4 AND n.day_key = $5 AND m.message_id = ANY($6)
					UNION
					SELECT s.message_id FROM comm_skipped s
					WHERE s.user_id = $1 AND s.item_id = $2 AND s.source = $3 AND s.thread_key = $4 AND s.day_key = $5 AND s.message_id = ANY($6)) k`,
				uid, item, source, t.Key, t.DayKey, ids).Scan(&known); err != nil {
				return err
			}
			if known < len(ids) {
				out = append(out, t)
			}
		}
		return nil
	})
	return out, err
}

// CreateBatches stores the threads of a tick in batches of size and remembers
// the cursor to commit when all of them are done.
func (s *Service) CreateBatches(ctx context.Context, uid uuid.UUID, taskRun *uuid.UUID, item uuid.UUID, source string, threads []collect.Thread,
	backfill bool, size int, cursor string) ([]uuid.UUID, error) {
	if size <= 0 {
		size = 20
	}
	var ids []uuid.UUID
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO comm_cursors (user_id, item_id, source, cursor, pending_cursor) VALUES ($1,$2,$3,'',$4)
			ON CONFLICT (user_id, item_id, source) DO UPDATE SET pending_cursor = EXCLUDED.pending_cursor`, uid, item, source, cursor); err != nil {
			return err
		}
		for i := 0; i < len(threads); i += size {
			end := min(i+size, len(threads))
			b, _ := json.Marshal(threads[i:end])
			var id uuid.UUID
			if err := tx.QueryRow(ctx, `INSERT INTO comm_batches (user_id, task_run_id, item_id, source, backfill, threads) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
				uid, taskRun, item, source, backfill, b).Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	})
	return ids, err
}

const batchCols = `id, user_id, task_run_id, item_id, source, backfill, threads, status, run_id`

func scanBatch(row pgx.Row) (*Batch, error) {
	var b Batch
	var threads []byte
	if err := row.Scan(&b.ID, &b.UserID, &b.TaskRunID, &b.ItemID, &b.Source, &b.Backfill, &threads, &b.Status, &b.RunID); err != nil {
		return nil, err
	}
	return &b, json.Unmarshal(threads, &b.Threads)
}

// Batch loads a batch of the user (nil when there is none).
func (s *Service) Batch(ctx context.Context, uid, id uuid.UUID) (*Batch, error) {
	var b *Batch
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var err error
		b, err = scanBatch(tx.QueryRow(ctx, `SELECT `+batchCols+` FROM comm_batches WHERE user_id = $1 AND id = $2`, uid, id))
		if postgres.IsNoRows(err) {
			b = nil
			return nil
		}
		return err
	})
	return b, err
}

// RunningBatch is the batch a run of the collector works on: the tools of the
// run and the MCP proxy accept only a run that is still going (tech §6.4).
func (s *Service) RunningBatch(ctx context.Context, uid, run uuid.UUID) (*Batch, error) {
	var b *Batch
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		var err error
		b, err = scanBatch(tx.QueryRow(ctx, `SELECT `+batchCols+` FROM comm_batches WHERE user_id = $1 AND run_id = $2 AND status = 'running'`, uid, run))
		if postgres.IsNoRows(err) {
			b = nil
			return nil
		}
		return err
	})
	return b, err
}

// StartBatch takes a queued (or failed, on a repeat) batch for a run.
func (s *Service) StartBatch(ctx context.Context, uid, id, run uuid.UUID) (bool, error) {
	var ok bool
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE comm_batches SET status = 'running', run_id = $3, started_at = now(), error_class = NULL WHERE user_id = $1 AND id = $2 AND status = 'queued'`, uid, id, run)
		ok = err == nil && tag.RowsAffected() == 1
		return err
	})
	return ok, err
}

// FinishBatch closes a batch. The messages of a successful batch that did not
// get into a note are remembered as read: the thread is not queued again
// until it has a new message (tech §6.2).
func (s *Service) FinishBatch(ctx context.Context, uid, id uuid.UUID, ok bool, errClass string) error {
	status := "done"
	if !ok {
		status = "failed"
	}
	return s.as(ctx, uid, func(tx pgx.Tx) error {
		b, err := scanBatch(tx.QueryRow(ctx, `UPDATE comm_batches SET status = $3, error_class = NULLIF($4,''), finished_at = now()
			WHERE user_id = $1 AND id = $2 AND status = 'running' RETURNING `+batchCols, uid, id, status, errClass))
		if postgres.IsNoRows(err) {
			return nil
		}
		if err != nil || !ok {
			return err
		}
		for _, t := range b.Threads {
			for _, m := range t.Messages {
				if _, err := tx.Exec(ctx, `INSERT INTO comm_skipped (user_id, item_id, source, thread_key, day_key, message_id)
					SELECT $1, $2, $3, $4, $5, $6 WHERE NOT EXISTS (
						SELECT 1 FROM comm_note_messages nm JOIN comm_notes n ON n.user_id = nm.user_id AND n.id = nm.note_id
						WHERE n.user_id = $1 AND n.item_id = $2 AND n.source = $3 AND n.thread_key = $4 AND n.day_key = $5 AND nm.message_id = $6)
					ON CONFLICT DO NOTHING`, uid, b.ItemID, b.Source, t.Key, t.DayKey, m.ID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// StaleBatches fails the batches whose run did not finish: the worker was
// stopped in the middle (tech §6.3). It returns the runs of the collection
// task to close and their users.
func (s *Service) StaleBatches(ctx context.Context, running, queued time.Duration) (map[uuid.UUID]uuid.UUID, error) {
	out := map[uuid.UUID]uuid.UUID{}
	err := postgres.AsMaintenance(ctx, s.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `UPDATE comm_batches SET status = 'failed', error_class = 'unavailable', finished_at = now()
			WHERE (status = 'running' AND COALESCE(started_at, created_at) < now() - make_interval(secs => $1))
			   OR (status = 'queued' AND created_at < now() - make_interval(secs => $2))
			RETURNING task_run_id, user_id`, running.Seconds(), queued.Seconds())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var run *uuid.UUID
			var uid uuid.UUID
			if err := rows.Scan(&run, &uid); err != nil {
				return err
			}
			if run != nil {
				out[*run] = uid
			}
		}
		return rows.Err()
	})
	return out, err
}

func (s *Service) countBatch(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID, notes, messages int) error {
	_, err := tx.Exec(ctx, `UPDATE comm_batches SET notes = notes + $3, messages = messages + $4 WHERE user_id = $1 AND id = $2`, uid, id, notes, messages)
	return err
}

// ClassBudget marks the batches left unprocessed because the monthly limit of
// the user was reached in the middle of a tick (R17, R27): not a failure.
const ClassBudget = "budget"

// Tick is the state of the batches of one run of the collection task.
type Tick struct {
	Pending, Failed int
	// OverBudget counts the batches skipped for the monthly limit; they are
	// not in Failed.
	OverBudget      int
	Notes, Messages int
	ErrorClass      string
}

// TickState sums the batches of a run of the task.
func (s *Service) TickState(ctx context.Context, uid, taskRun uuid.UUID) (Tick, error) {
	var t Tick
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status IN ('queued','running')),
				count(*) FILTER (WHERE status = 'failed' AND error_class IS DISTINCT FROM 'budget'),
				count(*) FILTER (WHERE status = 'failed' AND error_class = 'budget'),
				COALESCE(sum(notes), 0), COALESCE(sum(messages), 0),
				COALESCE(max(error_class) FILTER (WHERE status = 'failed' AND error_class IS DISTINCT FROM 'budget'), '')
			FROM comm_batches WHERE user_id = $1 AND task_run_id = $2`, uid, taskRun).Scan(&t.Pending, &t.Failed, &t.OverBudget, &t.Notes, &t.Messages, &t.ErrorClass)
	})
	return t, err
}

// CommitCursors ends a tick: with every batch done the pending cursors become
// the cursors; otherwise they are dropped and the next tick repeats from the
// old ones (R15: nothing is lost, TASK-12).
func (s *Service) CommitCursors(ctx context.Context, uid uuid.UUID, ok bool) error {
	return s.as(ctx, uid, func(tx pgx.Tx) error {
		q := `UPDATE comm_cursors SET pending_cursor = NULL WHERE user_id = $1 AND pending_cursor IS NOT NULL`
		if ok {
			q = `UPDATE comm_cursors SET cursor = pending_cursor, pending_cursor = NULL, last_success_at = now() WHERE user_id = $1 AND pending_cursor IS NOT NULL`
		}
		_, err := tx.Exec(ctx, q, uid)
		return err
	})
}

// Backfill is the progress of the first collection: the threads of the
// history batches processed and in total; nil when none is going (R15).
func (s *Service) Backfill(ctx context.Context, uid uuid.UUID) (*Progress, error) {
	var done, total, pending int
	err := s.as(ctx, uid, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT COALESCE(sum(jsonb_array_length(threads)) FILTER (WHERE status IN ('done','failed')), 0),
				COALESCE(sum(jsonb_array_length(threads)), 0), count(*) FILTER (WHERE status IN ('queued','running'))
			FROM comm_batches WHERE user_id = $1 AND backfill AND created_at > now() - interval '2 days'`, uid).Scan(&done, &total, &pending)
	})
	if err != nil || pending == 0 {
		return nil, err
	}
	return &Progress{Done: done, Total: total}, nil
}

func monthOf(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// AddUsage adds a run of the collector and its cost to the month of the user (R27).
func (s *Service) AddUsage(ctx context.Context, uid uuid.UUID, costUSD float64) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO comm_usage (user_id, month, runs, cost_usd) VALUES ($1, $2, 1, $3)
		ON CONFLICT (user_id, month) DO UPDATE SET runs = comm_usage.runs + 1, cost_usd = comm_usage.cost_usd + EXCLUDED.cost_usd`, uid, monthOf(s.clock()), costUSD)
	return err
}

// MonthCost is what the collection of the user cost this month.
func (s *Service) MonthCost(ctx context.Context, uid uuid.UUID) (float64, error) {
	var c float64
	err := s.Pool.QueryRow(ctx, `SELECT COALESCE((SELECT cost_usd::float8 FROM comm_usage WHERE user_id = $1 AND month = $2), 0)`, uid, monthOf(s.clock())).Scan(&c)
	return c, err
}

// OverBudget reports whether the collection of the user reached the monthly
// limit (R27); without a limit it never does.
func (s *Service) OverBudget(ctx context.Context, uid uuid.UUID) (bool, error) {
	st, err := s.LoadSettings(ctx)
	if err != nil || st.MonthlyLimitUSD <= 0 {
		return false, err
	}
	c, err := s.MonthCost(ctx, uid)
	return c >= st.MonthlyLimitUSD, err
}

// SkipBatch closes a queued batch that will not run: the monthly limit is
// reached. Its threads stay uncollected, the cursor does not move.
func (s *Service) SkipBatch(ctx context.Context, uid, id uuid.UUID, class string) error {
	return s.as(ctx, uid, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE comm_batches SET status = 'failed', error_class = $3, finished_at = now() WHERE user_id = $1 AND id = $2 AND status = 'queued'`, uid, id, class)
		return err
	})
}

// LostRuns lists the runs of the collection task that are still running an
// hour after they began and have no batch: the worker stopped before it
// queued them. It is maintenance — comm_batches is read for every user.
func (s *Service) LostRuns(ctx context.Context) ([]uuid.UUID, error) {
	var out []uuid.UUID
	err := postgres.AsMaintenance(ctx, s.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT r.id FROM task_runs r JOIN scheduled_tasks t ON t.id = r.task_id
			WHERE t.kind = 'memory_collect' AND r.status = 'running' AND r.started_at < now() - interval '1 hour'
			  AND NOT EXISTS (SELECT 1 FROM comm_batches b WHERE b.task_run_id = r.id)`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return err
			}
			out = append(out, id)
		}
		return rows.Err()
	})
	return out, err
}

// BudgetUsedPct is the share of the monthly limit the user spent (0 without a limit).
func (s *Service) BudgetUsedPct(ctx context.Context, uid uuid.UUID) (int, error) {
	st, err := s.LoadSettings(ctx)
	if err != nil || st.MonthlyLimitUSD <= 0 {
		return 0, err
	}
	c, err := s.MonthCost(ctx, uid)
	return int(c / st.MonthlyLimitUSD * 100), err
}

// AdapterOf is the adapter of a catalog item marked as a source.
func (s *Service) AdapterOf(ctx context.Context, item uuid.UUID) (collect.Adapter, bool) {
	var kind *string
	var on bool
	if err := s.Pool.QueryRow(ctx, `SELECT adapter, comm_source FROM catalog_items WHERE id = $1`, item).Scan(&kind, &on); err != nil || kind == nil || !on {
		return nil, false
	}
	return collect.Get(*kind)
}

// ProxyGuard checks a call a run of the collector makes through the MCP
// proxy (tech §6.4, R26): the run is going, the item is the item of its
// batch, the tool is a read tool of the adapter and the thread it reads is a
// thread of the batch. It returns the tools the run may see, or the refusal.
func (s *Service) ProxyGuard(ctx context.Context, uid, run, item uuid.UUID, method, tool string, args json.RawMessage) ([]string, string) {
	refuse := func(msg string) ([]string, string) {
		metrics.MemoryDeniedTools.Inc()
		return nil, msg
	}
	b, err := s.RunningBatch(ctx, uid, run)
	if err != nil || b == nil {
		return refuse("the run of the collector is over")
	}
	if b.ItemID != item {
		return refuse("this server is not the source of the batch")
	}
	ad, ok := s.AdapterOf(ctx, item)
	if !ok {
		return refuse("the server is no longer a source of communications")
	}
	tools := ad.ReadTools()
	switch {
	case method == "tools/call":
	case method == "initialize" || method == "ping" || method == "tools/list" || strings.HasPrefix(method, "notifications/"):
		return tools, ""
	default:
		// anything else — resources, prompts, a batch of requests — is not what a run needs
		return refuse("the collector may only list and call the read tools of the source")
	}
	allowed := false
	for _, t := range tools {
		allowed = allowed || t == tool
	}
	if !allowed {
		return refuse("the collector only reads: the tool " + tool + " is not available to it")
	}
	key, ok := ad.ThreadOf(tool, args)
	if !ok {
		return refuse("the call does not name a thread of the batch")
	}
	for _, t := range b.Threads {
		if t.Key == key {
			return tools, ""
		}
	}
	return refuse("the thread " + key + " is not in the batch of this run")
}
