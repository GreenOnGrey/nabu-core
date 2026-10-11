package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Kinds of tasks (FTR.NAB.CMN-0003 R13, tech §11): a task the agent created
// in the chat, or one of the three prepared memory tasks. A user has at most
// one task of each memory kind; switching it off and on keeps the row and
// the history of its runs.
const (
	KindAgent      = "agent"
	KindCollect    = "memory_collect"
	KindDigestDay  = "memory_digest_day"
	KindDigestWeek = "memory_digest_week"
)

// Why a memory task is paused (R17, tech §6.3).
const (
	PauseUser         = "user"
	PauseSourceAccess = "source_access"
	PauseBudget       = "budget"
)

// The hours of collection unless the user chose round the clock (R13).
const (
	collectFromHour = 8
	collectToHour   = 20
)

var memoryKinds = map[string]string{"collect": KindCollect, "digest_day": KindDigestDay, "digest_week": KindDigestWeek}

var memoryTitles = map[string]string{KindCollect: "Communications collection", KindDigestDay: "Daily digest", KindDigestWeek: "Weekly digest"}

// SourceRef names a source of communications: a catalog item and one of the
// sources its adapter gives.
type SourceRef struct {
	ItemID uuid.UUID `json:"itemId"`
	Source string    `json:"source"`
}

// Exclusions are what the user asked not to collect (R4).
type Exclusions struct {
	Senders []string `json:"senders"`
	Folders []string `json:"folders"`
	Chats   []string `json:"chats"`
}

// CollectSettings are the settings of «Сбор коммуникаций».
type CollectSettings struct {
	Sources       []SourceRef `json:"sources"`
	Interval      string      `json:"interval"` // 15m | 1h | 4h | 1d
	RoundTheClock bool        `json:"roundTheClock"`
	Exclusions    Exclusions  `json:"exclusions"`
}

// DigestDaySettings are the settings of «Сводка на день».
type DigestDaySettings struct {
	Time    string `json:"time"` // 09:00
	Days    []int  `json:"days"` // 1 — Monday … 7 — Sunday
	Channel string `json:"channel"`
}

// DigestWeekSettings are the settings of «Сводка на неделю».
type DigestWeekSettings struct {
	Weekday int    `json:"weekday"` // 1 — Monday … 7 — Sunday
	Time    string `json:"time"`
	Channel string `json:"channel"`
}

func defaultSettings(kind string) any {
	switch kind {
	case KindCollect:
		return &CollectSettings{Sources: []SourceRef{}, Interval: "1h", Exclusions: Exclusions{Senders: []string{}, Folders: []string{}, Chats: []string{}}}
	case KindDigestDay:
		return &DigestDaySettings{Time: "09:00", Days: []int{1, 2, 3, 4, 5}, Channel: domain.ChannelWeb}
	}
	return &DigestWeekSettings{Weekday: 1, Time: "09:00", Channel: domain.ChannelWeb}
}

// Offset is the shift of a user inside an interval of minutes: the runs of
// different users do not start in the same minute (R28, tech §6.3).
func Offset(uid uuid.UUID, minutes int) int {
	h := fnv.New32a()
	_, _ = h.Write(uid[:])
	return int(h.Sum32() % uint32(minutes))
}

// CollectCron is the schedule of the collection of a user: the interval with
// the user's shift, within the working hours unless round the clock.
func CollectCron(uid uuid.UUID, interval string, roundTheClock bool) (string, error) {
	from, hours := collectFromHour, collectToHour-collectFromHour
	hourField := fmt.Sprintf("%d-%d", collectFromHour, collectToHour-1)
	if roundTheClock {
		from, hours, hourField = 0, 24, "*"
	}
	switch interval {
	case "15m":
		m := Offset(uid, 15)
		return fmt.Sprintf("%d,%d,%d,%d %s * * *", m, m+15, m+30, m+45, hourField), nil
	case "1h":
		return fmt.Sprintf("%d %s * * *", Offset(uid, 60), hourField), nil
	case "4h":
		off := Offset(uid, 240)
		var hs []string
		for h := from + off/60; h < from+hours; h += 4 {
			hs = append(hs, strconv.Itoa(h))
		}
		return fmt.Sprintf("%d %s * * *", off%60, strings.Join(hs, ",")), nil
	case "1d":
		off := Offset(uid, hours*60)
		return fmt.Sprintf("%d %d * * *", off%60, from+off/60), nil
	}
	return "", apperr.Unprocessable("invalid_interval", "interval is 15m, 1h, 4h or 1d").With("field", "interval")
}

func parseClock(s string) (int, int, error) {
	t, err := time.Parse("15:04", strings.TrimSpace(s))
	if err != nil {
		return 0, 0, apperr.Unprocessable("invalid_time", "time is HH:MM").With("field", "time")
	}
	return t.Hour(), t.Minute(), nil
}

// cronDay turns 1 — Monday … 7 — Sunday into the cron day of week.
func cronDay(d int) (int, error) {
	if d < 1 || d > 7 {
		return 0, apperr.Unprocessable("invalid_day", "a day is 1 (Monday) to 7 (Sunday)").With("field", "days")
	}
	return d % 7, nil
}

// memoryPlan validates the settings of a memory task and gives its cron,
// channel and the settings to store.
func (s *Service) memoryPlan(ctx context.Context, uid uuid.UUID, kind string, raw json.RawMessage, cur json.RawMessage) (cronExpr, channel string, stored []byte, err error) {
	set := defaultSettings(kind)
	for _, b := range []json.RawMessage{cur, raw} { // the patch replaces the fields it names
		if len(b) > 0 && string(b) != "{}" {
			if err := json.Unmarshal(b, set); err != nil {
				return "", "", nil, apperr.BadRequest("invalid_json", "invalid settings")
			}
		}
	}
	channel = domain.ChannelWeb
	checkChannel := func(ch *string) error {
		if *ch == "" {
			*ch = domain.ChannelWeb
		}
		switch *ch {
		case domain.ChannelWeb:
		case domain.ChannelTelegram, domain.ChannelVKTeams, domain.ChannelEmail:
			if s.Deliverable != nil {
				if ok, why := s.Deliverable(ctx, uid, *ch); !ok {
					return apperr.Unprocessable("channel_unavailable", "results cannot be delivered to "+*ch+": "+why).With("field", "channel")
				}
			}
		default:
			return apperr.Unprocessable("invalid_channel", "channel is web, telegram, vkteams or email").With("field", "channel")
		}
		channel = *ch
		return nil
	}
	switch v := set.(type) {
	case *CollectSettings:
		if cronExpr, err = CollectCron(uid, v.Interval, v.RoundTheClock); err != nil {
			return
		}
		if v.Sources == nil {
			v.Sources = []SourceRef{}
		}
		for _, l := range []*[]string{&v.Exclusions.Senders, &v.Exclusions.Folders, &v.Exclusions.Chats} {
			*l = cleanList(*l)
			for _, x := range *l {
				// R4: an exclusion is applied as a term of the source's search —
				// a value that cannot be one would be stored and silently ignored
				if strings.ContainsAny(x, " \t\n\"'(){}") {
					return "", "", nil, apperr.Unprocessable("invalid_exclusion", "an exclusion is an address or an identifier without spaces, quotes and brackets: "+x).With("field", "exclusions")
				}
			}
		}
	case *DigestDaySettings:
		h, m, e := parseClock(v.Time)
		if e != nil {
			return "", "", nil, e
		}
		if len(v.Days) == 0 {
			return "", "", nil, apperr.Unprocessable("invalid_day", "choose at least one day").With("field", "days")
		}
		seen := map[int]bool{}
		var days []int
		for _, d := range v.Days {
			cd, e := cronDay(d)
			if e != nil {
				return "", "", nil, e
			}
			if !seen[cd] {
				seen[cd] = true
				days = append(days, cd)
			}
		}
		sort.Ints(days)
		parts := make([]string, len(days))
		for i, d := range days {
			parts[i] = strconv.Itoa(d)
		}
		dow := strings.Join(parts, ",")
		if dow == "1,2,3,4,5" {
			dow = "1-5"
		}
		if len(days) == 7 {
			dow = "*"
		}
		cronExpr = fmt.Sprintf("%d %d * * %s", m, h, dow)
		if err = checkChannel(&v.Channel); err != nil {
			return
		}
	case *DigestWeekSettings:
		h, m, e := parseClock(v.Time)
		if e != nil {
			return "", "", nil, e
		}
		cd, e := cronDay(v.Weekday)
		if e != nil {
			return "", "", nil, e
		}
		cronExpr = fmt.Sprintf("%d %d * * %d", m, h, cd)
		if err = checkChannel(&v.Channel); err != nil {
			return
		}
	}
	stored, _ = json.Marshal(set)
	return cronExpr, channel, stored, nil
}

func cleanList(in []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, v := range in {
		v = strings.TrimSpace(v)
		if k := strings.ToLower(v); v != "" && !seen[k] && len(out) < 200 {
			seen[k] = true
			out = append(out, v)
		}
	}
	return out
}

// MemoryTask is a prepared memory task as the Memory section shows it
// (tech §3.2).
type MemoryTask struct {
	Kind      string          `json:"kind"` // collect | digest_day | digest_week
	Enabled   bool            `json:"enabled"`
	TaskID    *uuid.UUID      `json:"taskId"`
	Status    string          `json:"status"` // off | active | paused
	PauseKind *string         `json:"pauseKind"`
	NextRunAt *time.Time      `json:"nextRunAt"`
	LastRun   *LastRun        `json:"lastRun"`
	Schedule  *Schedule       `json:"schedule"`
	Settings  json.RawMessage `json:"settings"`
}

func (s *Service) memoryTask(ctx context.Context, uid uuid.UUID, short string) (*MemoryTask, *Task, error) {
	kind := memoryKinds[short]
	if kind == "" {
		return nil, nil, apperr.NotFound("not_found", "the memory tasks are collect, digest_day and digest_week")
	}
	t, err := scanTask(s.Pool.QueryRow(ctx, `SELECT `+taskCols+taskFrom+` WHERE t.user_id = $1 AND t.kind = $2`, uid, kind))
	if postgres.IsNoRows(err) {
		b, _ := json.Marshal(defaultSettings(kind))
		return &MemoryTask{Kind: short, Status: "off", Settings: b}, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	mt := &MemoryTask{Kind: short, TaskID: &t.ID, Status: "off", PauseKind: t.PauseKind, NextRunAt: t.NextRunAt, LastRun: t.LastRun, Schedule: &t.Schedule}
	if t.Status == "active" || t.Status == "paused" {
		mt.Enabled, mt.Status = true, t.Status
	}
	// the stored settings over the defaults: a field added later has a value
	set := defaultSettings(kind)
	_ = json.Unmarshal(t.Settings, set)
	mt.Settings, _ = json.Marshal(set)
	return mt, t, nil
}

// MemoryTasks lists the three prepared tasks of the user.
func (s *Service) MemoryTasks(ctx context.Context, uid uuid.UUID) ([]MemoryTask, error) {
	out := make([]MemoryTask, 0, 3)
	for _, k := range []string{"collect", "digest_day", "digest_week"} {
		mt, _, err := s.memoryTask(ctx, uid, k)
		if err != nil {
			return nil, err
		}
		out = append(out, *mt)
	}
	return out, nil
}

// MemoryTaskOf loads one prepared task (kind is the stored kind).
func (s *Service) MemoryTaskOf(ctx context.Context, uid uuid.UUID, kind string) (*MemoryTask, error) {
	for short, k := range memoryKinds {
		if k == kind {
			mt, _, err := s.memoryTask(ctx, uid, short)
			return mt, err
		}
	}
	return nil, apperr.NotFound("not_found", "unknown memory task")
}

// MemoryPatch switches a memory task on or off and changes its settings.
type MemoryPatch struct {
	Enabled  *bool           `json:"enabled"`
	Settings json.RawMessage `json:"settings"`
}

// SetMemoryTask switches a prepared task on or off and stores its settings
// (R13, R14). Gate is called before a task is switched on: the consent and
// the sources of the collection are checked there.
func (s *Service) SetMemoryTask(ctx context.Context, uid uuid.UUID, short string, p MemoryPatch) (*MemoryTask, error) {
	cur, task, err := s.memoryTask(ctx, uid, short)
	if err != nil {
		return nil, err
	}
	kind := memoryKinds[short]
	enabled := cur.Enabled
	if p.Enabled != nil {
		enabled = *p.Enabled
	}
	var tz string
	if err := s.Pool.QueryRow(ctx, `SELECT timezone FROM users WHERE id = $1`, uid).Scan(&tz); err != nil {
		return nil, err
	}
	if _, err := time.LoadLocation(tz); err != nil || tz == "" {
		tz = s.DefaultTimezone
	}
	cronExpr, channel, stored, err := s.memoryPlan(ctx, uid, kind, p.Settings, cur.Settings)
	if err != nil {
		return nil, err
	}
	if enabled && s.MemoryGate != nil {
		if err := s.MemoryGate(ctx, uid, kind, stored); err != nil {
			return nil, err
		}
	}
	now := s.clock()
	next := nullTime(Next("cron", cronExpr, tz, now))
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		switch {
		case task == nil && !enabled:
			// settings of a task that was never on are not kept
			return nil
		case task == nil:
			_, err := tx.Exec(ctx, `INSERT INTO scheduled_tasks (user_id, title, instruction, schedule_kind, cron, timezone, channel, next_run_at, kind, settings)
				VALUES ($1,$2,'','cron',$3,$4,$5,$6,$7,$8)`, uid, memoryTitles[kind], cronExpr, tz, channel, next, kind, stored)
			return err
		case !enabled:
			_, err := tx.Exec(ctx, `UPDATE scheduled_tasks SET status = 'cancelled', cancelled_at = now(), next_run_at = NULL, pause_kind = NULL, pause_reason = NULL,
				cron = $2, timezone = $3, channel = $4, settings = $5 WHERE id = $1`, task.ID, cronExpr, tz, channel, stored)
			return err
		case task.Status == "paused" && (p.Enabled == nil || cur.Enabled):
			// a paused task keeps waiting; only its settings change
			_, err := tx.Exec(ctx, `UPDATE scheduled_tasks SET cron = $2, timezone = $3, channel = $4, settings = $5 WHERE id = $1`, task.ID, cronExpr, tz, channel, stored)
			return err
		default:
			_, err := tx.Exec(ctx, `UPDATE scheduled_tasks SET status = 'active', cancelled_at = NULL, failures = 0, pause_kind = NULL, pause_reason = NULL,
				cron = $2, timezone = $3, channel = $4, settings = $5, next_run_at = $6 WHERE id = $1`, task.ID, cronExpr, tz, channel, stored, next)
			return err
		}
	})
	if err != nil {
		return nil, err
	}
	out, t, err := s.memoryTask(ctx, uid, short)
	if err == nil && t != nil {
		s.publish(ctx, events.TaskUpdated, uid, t)
	}
	return out, err
}

// DisableMemoryTasks switches every memory task of the user off: the user
// deleted everything collected (R24).
func (s *Service) DisableMemoryTasks(ctx context.Context, uid uuid.UUID) error {
	_, err := s.Pool.Exec(ctx, `UPDATE scheduled_tasks SET status = 'cancelled', cancelled_at = now(), next_run_at = NULL, pause_kind = NULL, pause_reason = NULL
		WHERE user_id = $1 AND kind <> 'agent' AND status IN ('active','paused')`, uid)
	return err
}

// Pause pauses an active task until the user resumes it (R14: «Приостановить»).
func (s *Service) Pause(ctx context.Context, uid, id uuid.UUID) (*Task, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE scheduled_tasks SET status = 'paused', pause_kind = 'user', pause_reason = NULL, next_run_at = NULL
		WHERE id = $1 AND user_id = $2 AND status = 'active'`, id, uid)
	if err != nil {
		return nil, err
	}
	t, err := s.Get(ctx, uid, id)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, apperr.Conflict("task_not_active", "the task is not active")
	}
	s.publish(ctx, events.TaskUpdated, uid, t)
	return t, nil
}

// PauseMemory pauses a memory task for a reason that is not a failure of the
// user: no access to the source, the monthly budget is spent (R17). The
// failure counter stays as it is. It reports whether the task was paused now.
func (s *Service) PauseMemory(ctx context.Context, taskID uuid.UUID, pauseKind, reason string) (bool, error) {
	var uid uuid.UUID
	err := s.Pool.QueryRow(ctx, `UPDATE scheduled_tasks SET status = 'paused', pause_kind = $2, pause_reason = $3, next_run_at = NULL
		WHERE id = $1 AND status = 'active' AND kind <> 'agent' RETURNING user_id`, taskID, pauseKind, clip(reason, 500)).Scan(&uid)
	if postgres.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if t, err := s.Get(ctx, uid, taskID); err == nil {
		s.publish(ctx, events.TaskUpdated, uid, t)
	}
	return true, nil
}

// ResumeMemory resumes the memory tasks paused for a reason that is over:
// the access to the source is back, a new month began (R17). With a nil user
// it resumes them for everybody.
func (s *Service) ResumeMemory(ctx context.Context, uid *uuid.UUID, pauseKind string) (int, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, user_id, COALESCE(cron,''), timezone FROM scheduled_tasks
		WHERE status = 'paused' AND kind <> 'agent' AND pause_kind = $1 AND ($2::uuid IS NULL OR user_id = $2)`, pauseKind, uid)
	if err != nil {
		return 0, err
	}
	type row struct {
		id, uid  uuid.UUID
		cron, tz string
	}
	var list []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.uid, &r.cron, &r.tz); err != nil {
			rows.Close()
			return 0, err
		}
		list = append(list, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	now := s.clock()
	n := 0
	for _, r := range list {
		tag, err := s.Pool.Exec(ctx, `UPDATE scheduled_tasks SET status = 'active', pause_kind = NULL, pause_reason = NULL, next_run_at = $2
			WHERE id = $1 AND status = 'paused' AND pause_kind = $3`, r.id, nullTime(Next("cron", r.cron, r.tz, now)), pauseKind)
		if err != nil {
			return n, err
		}
		if tag.RowsAffected() > 0 {
			n++
			if t, err := s.Get(ctx, r.uid, r.id); err == nil {
				s.publish(ctx, events.TaskUpdated, r.uid, t)
			}
		}
	}
	return n, nil
}

// MemoryRoutes mounts /memory-tasks (tech §3.2). allowed fails when the
// memory of communications is not allowed in the instance.
func (s *Service) MemoryRoutes(r chi.Router, allowed func(ctx context.Context) error) {
	r.Get("/memory-tasks", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		if err := allowed(r.Context()); err != nil {
			return err
		}
		l, err := s.MemoryTasks(r.Context(), p.UserID)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Patch("/memory-tasks/{kind}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		if err := allowed(r.Context()); err != nil {
			return err
		}
		var in MemoryPatch
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		t, err := s.SetMemoryTask(r.Context(), p.UserID, chi.URLParam(r, "kind"), in)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, t)
		return nil
	}))
}
