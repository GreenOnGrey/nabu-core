package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Task is a scheduled task as the API shows it (tech §3.2a).
type Task struct {
	ID          uuid.UUID  `json:"id"`
	Title       string     `json:"title"`
	Instruction string     `json:"instruction"`
	Schedule    Schedule   `json:"schedule"`
	Channel     string     `json:"channel"`
	Status      string     `json:"status"`
	NextRunAt   *time.Time `json:"nextRunAt"`
	Failures    int        `json:"failures"`
	PauseReason *string    `json:"pauseReason"`
	LastRun     *LastRun   `json:"lastRun"`
	CreatedAt   time.Time  `json:"createdAt"`
	userID      uuid.UUID
}

// LastRun is the outcome of the latest run.
type LastRun struct {
	At      time.Time `json:"at"`
	Status  string    `json:"status"`
	Summary *string   `json:"summary"`
	Error   *string   `json:"error,omitempty"`
}

// Run is one run of a task.
type Run struct {
	ID         uuid.UUID  `json:"id"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt"`
	Status     string     `json:"status"`
	Summary    *string    `json:"summary"`
	MessageID  *uuid.UUID `json:"messageId"`
	// DeliveryNote: channel_unavailable — the channel was not available, the
	// result went to the web (R5, CH-07).
	DeliveryNote *string    `json:"deliveryNote"`
	ErrorClass   *string    `json:"errorClass"`
	ErrorText    *string    `json:"errorText"`
	ScheduledFor *time.Time `json:"scheduledFor"`
}

// RunMessage is the payload of nabu.task.run.
type RunMessage struct {
	RunID  uuid.UUID `json:"runId"`
	TaskID uuid.UUID `json:"taskId"`
	UserID uuid.UUID `json:"userId"`
}

// Service manages tasks and plans their runs.
type Service struct {
	Pool    *pgxpool.Pool
	Events  events.Publisher
	Bus     kafka.Publisher
	Rules   Rules
	Catchup time.Duration
	Tick    time.Duration
	// MaxFailures in a row pause a task (R38).
	MaxFailures int
	// DefaultTimezone is used when the profile has none.
	DefaultTimezone string
	// Deliverable reports whether results can be delivered to the channel of
	// the user now (FTR.NAB.CMN-0002 R5): the channel is available, Telegram is
	// bound, the user wrote to the VK Teams bot; the reason explains a refusal.
	Deliverable func(ctx context.Context, uid uuid.UUID, channel string) (bool, string)
	now         func() time.Time
}

func (s *Service) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

const taskCols = `t.id, t.user_id, t.title, t.instruction, t.schedule_kind, t.run_at, COALESCE(t.cron,''), t.timezone, t.channel, t.status,
	t.next_run_at, t.failures, t.pause_reason, t.created_at,
	lr.started_at, lr.status, lr.summary, lr.error_text`

const taskFrom = ` FROM scheduled_tasks t LEFT JOIN LATERAL (SELECT started_at, status, summary, error_text FROM task_runs r
	WHERE r.task_id = t.id ORDER BY started_at DESC LIMIT 1) lr ON true`

func scanTask(row pgx.Row) (*Task, error) {
	var t Task
	var lrAt *time.Time
	var lrStatus, lrSummary, lrErr *string
	if err := row.Scan(&t.ID, &t.userID, &t.Title, &t.Instruction, &t.Schedule.Kind, &t.Schedule.At, &t.Schedule.Cron, &t.Schedule.Timezone,
		&t.Channel, &t.Status, &t.NextRunAt, &t.Failures, &t.PauseReason, &t.CreatedAt, &lrAt, &lrStatus, &lrSummary, &lrErr); err != nil {
		return nil, err
	}
	t.Schedule.Human = Human(t.Schedule.Kind, t.Schedule.Cron, t.Schedule.At, t.Schedule.Timezone)
	if lrAt != nil {
		t.LastRun = &LastRun{At: *lrAt, Status: *lrStatus, Summary: lrSummary, Error: lrErr}
	}
	return &t, nil
}

// Get loads a task of the user.
func (s *Service) Get(ctx context.Context, uid, id uuid.UUID) (*Task, error) {
	t, err := scanTask(s.Pool.QueryRow(ctx, `SELECT `+taskCols+taskFrom+` WHERE t.id = $1 AND t.user_id = $2`, id, uid))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "task not found")
	}
	return t, err
}

// List lists the tasks of the user; status active also shows paused ones.
func (s *Service) List(ctx context.Context, uid uuid.UUID, status string, page httpx.Page) (httpx.List[Task], error) {
	var statuses []string
	switch status {
	case "", "active":
		statuses = []string{"active", "paused"}
	case "done", "cancelled", "paused":
		statuses = []string{status}
	case "all":
		statuses = []string{"active", "paused", "done", "cancelled"}
	default:
		return httpx.List[Task]{}, apperr.BadRequest("invalid_status", "status is active, done or cancelled")
	}
	args := []any{uid, statuses, page.Limit + 1}
	cond := ""
	if page.Cursor != nil {
		cond = ` AND (t.created_at, t.id) < ($4, $5)`
		args = append(args, page.Cursor.T, page.Cursor.ID)
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+taskCols+taskFrom+` WHERE t.user_id = $1 AND t.status = ANY($2)`+cond+
		` ORDER BY t.created_at DESC, t.id DESC LIMIT $3`, args...)
	if err != nil {
		return httpx.List[Task]{}, err
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return httpx.List[Task]{}, err
		}
		out = append(out, *t)
	}
	return httpx.NewList(out, page.Limit, func(t Task) (time.Time, string) { return t.CreatedAt, t.ID.String() }), rows.Err()
}

// Runs lists the runs of a task.
func (s *Service) Runs(ctx context.Context, uid, id uuid.UUID, page httpx.Page) (httpx.List[Run], error) {
	if _, err := s.Get(ctx, uid, id); err != nil {
		return httpx.List[Run]{}, err
	}
	args := []any{id, page.Limit + 1}
	cond := ""
	if page.Cursor != nil {
		cond = ` AND (started_at, id) < ($3, $4)`
		args = append(args, page.Cursor.T, page.Cursor.ID)
	}
	rows, err := s.Pool.Query(ctx, `SELECT id, started_at, finished_at, status, summary, message_id, error_class, error_text, scheduled_for, delivery_note
		FROM task_runs WHERE task_id = $1`+cond+` ORDER BY started_at DESC, id DESC LIMIT $2`, args...)
	if err != nil {
		return httpx.List[Run]{}, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.ID, &r.StartedAt, &r.FinishedAt, &r.Status, &r.Summary, &r.MessageID, &r.ErrorClass, &r.ErrorText, &r.ScheduledFor, &r.DeliveryNote); err != nil {
			return httpx.List[Run]{}, err
		}
		out = append(out, r)
	}
	return httpx.NewList(out, page.Limit, func(r Run) (time.Time, string) { return r.StartedAt, r.ID.String() }), rows.Err()
}

func (s *Service) publish(ctx context.Context, typ string, uid uuid.UUID, data any) {
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: typ, UserID: &uid, Data: data})
	}
}

// CreateInput are the arguments of task_create.
type CreateInput struct {
	Title       string `json:"title"`
	Instruction string `json:"instruction"`
	Schedule    struct {
		Once string `json:"once"`
		Cron string `json:"cron"`
	} `json:"schedule"`
	Channel string `json:"channel"`
}

// Create validates and creates a task (R35, R38). Validation errors are
// returned as text for the agent to ask the user (tech §3.2a).
func (s *Service) Create(ctx context.Context, uid uuid.UUID, in CreateInput, defaultChannel string, fromMessage *uuid.UUID) (*Task, error) {
	in.Title, in.Instruction = strings.TrimSpace(in.Title), strings.TrimSpace(in.Instruction)
	if in.Title == "" || in.Instruction == "" {
		return nil, fmt.Errorf("title and instruction are required")
	}
	var tz string
	if err := s.Pool.QueryRow(ctx, `SELECT timezone FROM users WHERE id = $1`, uid).Scan(&tz); err != nil {
		return nil, err
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		tz = s.DefaultTimezone
		loc, _ = time.LoadLocation(tz)
	}
	ch := in.Channel
	if ch == "" {
		ch = defaultChannel
	}
	if strings.HasPrefix(ch, "client:") || strings.HasPrefix(ch, "task:") || ch == "" {
		ch = domain.ChannelWeb
	}
	ch = domain.ChannelOf(ch)
	if in.Channel == "" && ch == domain.ChannelEmail {
		ch = domain.ChannelWeb // R11: mail delivers only when the user asked for it
	}
	switch ch {
	case domain.ChannelWeb:
	case domain.ChannelTelegram, domain.ChannelVKTeams, domain.ChannelEmail:
		if s.Deliverable != nil {
			if ok, why := s.Deliverable(ctx, uid, ch); !ok {
				return nil, fmt.Errorf("results cannot be delivered to %s: %s; choose web", ch, why)
			}
		}
	default:
		return nil, fmt.Errorf("unknown channel %q: use web, telegram, vkteams or email", ch)
	}
	now := s.clock()
	var kind, cronExpr string
	var runAt *time.Time
	var next time.Time
	switch {
	case in.Schedule.Once != "" && in.Schedule.Cron != "":
		return nil, fmt.Errorf("give either schedule.once or schedule.cron")
	case in.Schedule.Once != "":
		at, err := ParseOnce(in.Schedule.Once, loc)
		if err != nil {
			return nil, err
		}
		if !at.After(now) {
			return nil, fmt.Errorf("the time %s is in the past (now %s in %s)", at.In(loc).Format("2006-01-02 15:04"), now.In(loc).Format("2006-01-02 15:04"), tz)
		}
		kind, runAt, next = "once", &at, at
	case in.Schedule.Cron != "":
		first, err := s.Rules.ValidateCron(in.Schedule.Cron, loc, now)
		if err != nil {
			return nil, err
		}
		kind, cronExpr, next = "cron", strings.Join(strings.Fields(in.Schedule.Cron), " "), first
	default:
		return nil, fmt.Errorf("the schedule is missing: ask the user when to run it")
	}
	var id uuid.UUID
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		// Serialize per user for the limit.
		if _, err := tx.Exec(ctx, `SELECT 1 FROM users WHERE id = $1 FOR UPDATE`, uid); err != nil {
			return err
		}
		var active int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM scheduled_tasks WHERE user_id = $1 AND status IN ('active','paused')`, uid).Scan(&active); err != nil {
			return err
		}
		if active >= s.Rules.MaxActive {
			return fmt.Errorf("the user already has %d active tasks, the limit is %d; cancel one first", active, s.Rules.MaxActive)
		}
		var c *string
		if cronExpr != "" {
			c = &cronExpr
		}
		return tx.QueryRow(ctx, `INSERT INTO scheduled_tasks (user_id, title, instruction, schedule_kind, run_at, cron, timezone, channel, next_run_at, created_from_message)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`, uid, in.Title, in.Instruction, kind, runAt, c, tz, ch, next, fromMessage).Scan(&id)
	})
	if err != nil {
		return nil, err
	}
	t, err := s.Get(ctx, uid, id)
	if err == nil {
		s.publish(ctx, events.TaskCreated, uid, t)
	}
	return t, err
}

// Cancel cancels a task (R36, TSK-05).
func (s *Service) Cancel(ctx context.Context, uid, id uuid.UUID) (*Task, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE scheduled_tasks SET status = 'cancelled', cancelled_at = now(), next_run_at = NULL
		WHERE id = $1 AND user_id = $2 AND status IN ('active','paused')`, id, uid)
	if err != nil {
		return nil, err
	}
	t, err := s.Get(ctx, uid, id)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 && t.Status != "cancelled" {
		return nil, apperr.Conflict("task_not_active", "the task is not active")
	}
	s.publish(ctx, events.TaskUpdated, uid, t)
	return t, nil
}

// Resume resumes a paused task: the failure counter is reset and the next
// run is computed from now (tech §3.2a).
func (s *Service) Resume(ctx context.Context, uid, id uuid.UUID) (*Task, error) {
	t, err := s.Get(ctx, uid, id)
	if err != nil {
		return nil, err
	}
	if t.Status != "paused" {
		return nil, apperr.Conflict("task_not_paused", "only a paused task can be resumed")
	}
	now := s.clock()
	next := Next(t.Schedule.Kind, t.Schedule.Cron, t.Schedule.Timezone, now)
	if t.Schedule.Kind == "once" {
		next = now
		if t.Schedule.At != nil && t.Schedule.At.After(now) {
			next = *t.Schedule.At
		}
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE scheduled_tasks SET status = 'active', failures = 0, pause_reason = NULL, next_run_at = $3
		WHERE id = $1 AND user_id = $2`, id, uid, next); err != nil {
		return nil, err
	}
	t, err = s.Get(ctx, uid, id)
	if err == nil {
		s.publish(ctx, events.TaskUpdated, uid, t)
	}
	return t, err
}

// ─── scheduler (tech §9a) ───────────────────────────────────────────

// Due plans the runs that are due: each occurrence becomes exactly one run
// even with several workers (FOR UPDATE SKIP LOCKED, unique (task, time):
// TSK-11). A run missed by more than the catch-up window is skipped; of
// several missed occurrences only the latest within the window runs (TSK-12).
func (s *Service) Due(ctx context.Context) (int, error) {
	now := s.clock()
	var msgs []RunMessage
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT t.id, t.user_id, t.schedule_kind, COALESCE(t.cron,''), t.timezone, t.next_run_at, u.status
			FROM scheduled_tasks t JOIN users u ON u.id = t.user_id
			WHERE t.status = 'active' AND t.next_run_at <= $1 ORDER BY t.next_run_at
			FOR UPDATE OF t SKIP LOCKED LIMIT 50`, now)
		if err != nil {
			return err
		}
		type due struct {
			id, uid                   uuid.UUID
			kind, cron, tz, userState string
			at                        time.Time
		}
		var list []due
		for rows.Next() {
			var d due
			if err := rows.Scan(&d.id, &d.uid, &d.kind, &d.cron, &d.tz, &d.at, &d.userState); err != nil {
				rows.Close()
				return err
			}
			list = append(list, d)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, d := range list {
			occurrence := d.at
			if d.kind == "cron" {
				if last := LatestBefore(d.cron, d.tz, d.at, now); !last.IsZero() {
					occurrence = last
				}
			}
			next := Next(d.kind, d.cron, d.tz, now)
			run := now.Sub(occurrence) <= s.Catchup && d.userState != "blocked" // TSK-10: blocked users' tasks do not run
			switch {
			case d.kind == "once" && (run || d.userState != "blocked"):
				if _, err := tx.Exec(ctx, `UPDATE scheduled_tasks SET next_run_at = NULL, status = 'done' WHERE id = $1`, d.id); err != nil {
					return err
				}
			case d.kind == "once":
				// a blocked user's one-off task waits: shifted by a tick
				if _, err := tx.Exec(ctx, `UPDATE scheduled_tasks SET next_run_at = $2 WHERE id = $1`, d.id, now.Add(time.Hour)); err != nil {
					return err
				}
			default:
				if _, err := tx.Exec(ctx, `UPDATE scheduled_tasks SET next_run_at = $2 WHERE id = $1`, d.id, nullTime(next)); err != nil {
					return err
				}
			}
			if !run {
				slog.InfoContext(ctx, "task occurrence skipped", "task", d.id, "occurrence", occurrence, "user_status", d.userState)
				continue
			}
			var runID uuid.UUID
			err := tx.QueryRow(ctx, `INSERT INTO task_runs (task_id, scheduled_for, status) VALUES ($1,$2,'running')
				ON CONFLICT (task_id, scheduled_for) DO NOTHING RETURNING id`, d.id, occurrence).Scan(&runID)
			if postgres.IsNoRows(err) {
				continue
			}
			if err != nil {
				return err
			}
			msgs = append(msgs, RunMessage{RunID: runID, TaskID: d.id, UserID: d.uid})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, m := range msgs {
		b, _ := json.Marshal(m)
		if err := s.Bus.Publish(ctx, kafka.TopicTaskRun, m.UserID.String(), b); err != nil {
			slog.ErrorContext(ctx, "publish task run", "run", m.RunID, "err", err)
			_ = s.Finish(ctx, m.RunID, false, "", "unavailable", "the run could not be queued: "+err.Error(), nil)
		}
	}
	return len(msgs), nil
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// Run runs the scheduler until ctx ends.
func (s *Service) Run(ctx context.Context) {
	t := time.NewTicker(s.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.Due(ctx); err != nil && ctx.Err() == nil {
				slog.Error("task scheduler", "err", err)
			} else if n > 0 {
				slog.Info("task runs planned", "count", n)
			}
		}
	}
}

// RunInfo is what the turn engine needs to perform a run.
type RunInfo struct {
	RunID, TaskID, UserID uuid.UUID
	Title, Instruction    string
	Channel, Status       string
}

// LoadRun loads a run that is still running.
func (s *Service) LoadRun(ctx context.Context, runID uuid.UUID) (*RunInfo, error) {
	var ri RunInfo
	err := s.Pool.QueryRow(ctx, `SELECT r.id, t.id, t.user_id, t.title, t.instruction, t.channel, r.status
		FROM task_runs r JOIN scheduled_tasks t ON t.id = r.task_id WHERE r.id = $1`, runID).
		Scan(&ri.RunID, &ri.TaskID, &ri.UserID, &ri.Title, &ri.Instruction, &ri.Channel, &ri.Status)
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return &ri, err
}

// Cancelled reports whether the task was cancelled after the run was planned.
func (s *Service) Cancelled(ctx context.Context, taskID uuid.UUID) bool {
	var st string
	_ = s.Pool.QueryRow(ctx, `SELECT status FROM scheduled_tasks WHERE id = $1`, taskID).Scan(&st)
	return st == "cancelled"
}

// Finish records the outcome of a run: success resets the failure counter;
// MaxFailures failures in a row pause the task (R38, TSK-08). It returns the
// pause reason when the task was paused just now.
func (s *Service) Finish(ctx context.Context, runID uuid.UUID, ok bool, summary, errClass, errText string, messageID *uuid.UUID) error {
	status := "succeeded"
	if !ok {
		status = "failed"
	}
	var taskID, uid uuid.UUID
	var failures int
	var paused bool
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `UPDATE task_runs SET status = $2, summary = NULLIF($3,''), error_class = NULLIF($4,''), error_text = NULLIF($5,''),
			message_id = $6, finished_at = now() WHERE id = $1 AND status = 'running' RETURNING task_id`,
			runID, status, clip(summary, 300), errClass, clip(errText, 500), messageID).Scan(&taskID); err != nil {
			return err
		}
		if ok {
			return tx.QueryRow(ctx, `UPDATE scheduled_tasks SET failures = 0 WHERE id = $1 RETURNING user_id, failures`, taskID).Scan(&uid, &failures)
		}
		if err := tx.QueryRow(ctx, `UPDATE scheduled_tasks SET failures = failures + 1 WHERE id = $1 RETURNING user_id, failures`, taskID).Scan(&uid, &failures); err != nil {
			return err
		}
		if failures >= s.MaxFailures {
			tag, err := tx.Exec(ctx, `UPDATE scheduled_tasks SET status = 'paused', pause_reason = $2, next_run_at = NULL WHERE id = $1 AND status = 'active'`,
				taskID, clip(errText, 500))
			paused = err == nil && tag.RowsAffected() > 0
			return err
		}
		return nil
	})
	if postgres.IsNoRows(err) {
		return nil // already finished
	}
	if err != nil {
		return err
	}
	metrics.TaskRuns.WithLabelValues(status).Inc()
	s.publish(ctx, events.TaskRunFinished, uid, map[string]any{"taskId": taskID, "runId": runID, "status": status, "paused": paused})
	return nil
}

// NoteDelivery marks a run whose channel was not available: the result went
// to the web and the run stays successful (R5).
func (s *Service) NoteDelivery(ctx context.Context, runID uuid.UUID, note string) {
	_, _ = s.Pool.Exec(ctx, `UPDATE task_runs SET delivery_note = $2 WHERE id = $1`, runID, note)
}

// PausedNow reports whether the task got paused by its latest failure, with the reason.
func (s *Service) PausedNow(ctx context.Context, taskID uuid.UUID) (bool, string) {
	var st string
	var reason *string
	var failures int
	_ = s.Pool.QueryRow(ctx, `SELECT status, pause_reason, failures FROM scheduled_tasks WHERE id = $1`, taskID).Scan(&st, &reason, &failures)
	if st == "paused" && failures >= s.MaxFailures && reason != nil {
		return true, *reason
	}
	return false, ""
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// ─── HTTP and tools ─────────────────────────────────────────────────

// Routes mounts /tasks (tech §3.2a).
func (s *Service) Routes(r chi.Router) {
	r.Get("/tasks", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		l, err := s.List(r.Context(), p.UserID, r.URL.Query().Get("status"), page)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, l)
		return nil
	}))
	r.Get("/tasks/{id}/runs", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		l, err := s.Runs(r.Context(), p.UserID, id, page)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, l)
		return nil
	}))
	action := func(fn func(context.Context, uuid.UUID, uuid.UUID) (*Task, error)) http.HandlerFunc {
		return httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
			p, err := httpx.MustPrincipal(r)
			if err != nil {
				return err
			}
			id, err := httpx.ParamUUID(r, "id")
			if err != nil {
				return err
			}
			t, err := fn(r.Context(), p.UserID, id)
			if err != nil {
				return err
			}
			httpx.JSON(w, 200, t)
			return nil
		})
	}
	r.Post("/tasks/{id}/cancel", action(s.Cancel))
	r.Post("/tasks/{id}/resume", action(s.Resume))
}

func describe(t *Task) string {
	next := "—"
	if t.NextRunAt != nil {
		loc, _ := time.LoadLocation(t.Schedule.Timezone)
		if loc == nil {
			loc = time.UTC
		}
		next = t.NextRunAt.In(loc).Format("2006-01-02 15:04") + " " + t.Schedule.Timezone
	}
	return fmt.Sprintf("Task %s %q: %s; schedule: %s; next run: %s; channel: %s", t.ID, t.Title, t.Status, t.Schedule.Human, next, t.Channel)
}

// Tools are task_create, task_list and task_cancel of the built-in MCP.
func (s *Service) Tools() []mcp.Tool {
	return []mcp.Tool{
		{Name: "task_create", Description: "Create a scheduled task: a one-off reminder or a regular job the platform runs with your memory and connections, even when the chat is closed. " +
			"Times are in the user's time zone. Ask the user for a missing time or channel before calling. After creating, confirm the schedule, the first run and the channel.",
			InputSchema: mcp.Schema(map[string]any{
				"title":       map[string]any{"type": "string", "description": "Short title"},
				"instruction": map[string]any{"type": "string", "description": "What to do on each run, as an instruction to yourself"},
				"schedule": map[string]any{"type": "object", "description": `Either {"once": "2026-10-09T11:30"} (local time) or {"cron": "0 10 * * 1"} (5 fields; at least 15 minutes between runs)`,
					"properties": map[string]any{"once": map[string]any{"type": "string"}, "cron": map[string]any{"type": "string"}}},
				"channel": map[string]any{"type": "string", "enum": []string{"web", "telegram", "vkteams", "email"}, "description": "Where to deliver the result; default — the channel of the current message. Use email only when the user explicitly asked to send results by mail; a task created from a letter delivers to web by default — tell the user so"},
			}, "title", "instruction", "schedule"),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in CreateInput
				if err := json.Unmarshal(args, &in); err != nil {
					return "", &mcp.ToolError{Msg: "invalid arguments: " + err.Error()}
				}
				if g.TaskID != uuid.Nil {
					return "", &mcp.ToolError{Msg: "tasks cannot be created from a task run"}
				}
				t, err := s.Create(ctx, g.UserID, in, g.Channel, nil)
				if err != nil {
					return "", &mcp.ToolError{Msg: "The task was not created: " + err.Error()}
				}
				return "Created. " + describe(t), nil
			}},
		{Name: "task_list", Description: "List the user's scheduled tasks.", ReadOnly: true,
			InputSchema: mcp.Schema(map[string]any{"status": map[string]any{"type": "string", "enum": []string{"active", "done", "cancelled", "all"}}}),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in struct {
					Status string `json:"status"`
				}
				_ = json.Unmarshal(args, &in)
				l, err := s.List(ctx, g.UserID, in.Status, httpx.Page{Limit: 50})
				if err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				if len(l.Items) == 0 {
					return "No tasks.", nil
				}
				var b strings.Builder
				for i := range l.Items {
					b.WriteString("- " + describe(&l.Items[i]) + "\n")
				}
				return b.String(), nil
			}},
		{Name: "task_cancel", Description: "Cancel a scheduled task by id.",
			InputSchema: mcp.Schema(map[string]any{"id": map[string]any{"type": "string"}}, "id"),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in struct {
					ID string `json:"id"`
				}
				_ = json.Unmarshal(args, &in)
				id, err := uuid.Parse(in.ID)
				if err != nil {
					return "", &mcp.ToolError{Msg: "invalid id"}
				}
				t, err := s.Cancel(ctx, g.UserID, id)
				if err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				return "Cancelled. " + describe(t), nil
			}},
	}
}
