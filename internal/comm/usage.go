package comm

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
)

// The usage of the memory of communications in the administration (FTR.NAB.MEM-0001 R25, R26, tech §4). It is
// built from tables without content — comm_settings, comm_usage, comm_usage_days, comm_usage_source_days,
// comm_level_days, comm_compact_state, task_runs — so the administrator sees numbers only (FTR.NAB.CMN-0003 R22).

// Levels counts records by level.
type Levels struct {
	Day    int `json:"day"`
	Week   int `json:"week"`
	Month  int `json:"month"`
	Thread int `json:"thread"`
}

func (l *Levels) add(level string, n int) {
	switch level {
	case LevelDay:
		l.Day += n
	case LevelWeek:
		l.Week += n
	case LevelMonth:
		l.Month += n
	case LevelThread:
		l.Thread += n
	}
}

// UsageRow is a user in the table of the usage.
type UsageRow struct {
	UserID          uuid.UUID    `json:"userId"`
	Email           string       `json:"email"`
	Notes           int          `json:"notes"`
	Levels          Levels       `json:"levels"`
	Runs            int          `json:"runs"`
	LastCollect     *LastCollect `json:"lastCollect"`
	CostUSD         float64      `json:"costUsd"`
	CostCollect     float64      `json:"costCollect"`
	CostCompact     float64      `json:"costCompact"`
	LimitUsedPct    int          `json:"limitUsedPct"`
	LastCompactAt   *time.Time   `json:"lastCompactAt"`
	CompactBroken   bool         `json:"compactBroken"`
	OpenCommitments int          `json:"openCommitments"`
}

// LastCollect is the outcome of the last tick of the collection.
type LastCollect struct {
	At      time.Time `json:"at"`
	Outcome string    `json:"outcome"`
}

// outcomeSQL classifies a run of the task memory_collect: the summary of a successful tick names busy and the rate
// limit (FTR.NAB.CMN-0003 tech, decisions 48, 53), a pause names the missing access, a failure its class.
const outcomeSQL = `CASE
	WHEN tr.status = 'failed' AND tr.error_class = 'timeout' THEN 'timeout'
	WHEN tr.status = 'failed' THEN 'error'
	WHEN tr.summary = 'busy' THEN 'busy'
	WHEN tr.summary LIKE 'rate_limited%' THEN 'rate_limited'
	WHEN tr.summary = 'paused:source_access' THEN 'source_access'
	ELSE 'ok' END`

// BrokenNights is how many nights in a row the compaction of a user failed before the administration marks it
// (FTR.NAB.MEM-0001 R15).
const BrokenNights = 3

func usageMonth(r *http.Request) (time.Time, error) {
	month := time.Now().UTC()
	if v := r.URL.Query().Get("month"); v != "" {
		m, err := time.Parse("2006-01", v)
		if err != nil {
			return time.Time{}, apperr.BadRequest("invalid_month", "month is YYYY-MM")
		}
		month = m
	}
	return time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC), nil
}

// Usage is the usage of a month: the cards, the collection and the compaction, the commitments and the users.
func (s *Service) Usage(ctx context.Context, first time.Time, brokenOnly bool) (map[string]any, error) {
	next := first.AddDate(0, 1, 0)
	st, err := s.LoadSettings(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"month": first.Format("2006-01"), "monthlyLimitUsd": st.MonthlyLimitUSD}
	var enabled, active, notes, brokenUsers, okN, failedN int
	var cost, compactCost, charsIn, charsOut float64
	var lastNight *time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM scheduled_tasks WHERE kind = 'memory_collect' AND status IN ('active','paused')),
			(SELECT count(*) FROM comm_usage WHERE month = $1 AND runs > 0),
			(SELECT COALESCE(sum(notes_count), 0) FROM comm_settings),
			(SELECT COALESCE(sum(cost_usd), 0)::float8 FROM comm_usage WHERE month = $1),
			(SELECT COALESCE(sum(compact_cost_usd), 0)::float8 FROM comm_usage WHERE month = $1),
			(SELECT COALESCE(sum(chars_in), 0)::float8 FROM comm_usage_days WHERE day >= $1 AND day < $2),
			(SELECT COALESCE(sum(chars_out), 0)::float8 FROM comm_usage_days WHERE day >= $1 AND day < $2),
			(SELECT COALESCE(sum(compact_ok), 0) FROM comm_usage_days WHERE day >= $1 AND day < $2),
			(SELECT COALESCE(sum(compact_failed), 0) FROM comm_usage_days WHERE day >= $1 AND day < $2),
			(SELECT max(last_at) FROM comm_compact_state),
			(SELECT count(*) FROM comm_compact_state WHERE failed_nights >= $3)`, first, next, BrokenNights).
		Scan(&enabled, &active, &notes, &cost, &compactCost, &charsIn, &charsOut, &okN, &failedN, &lastNight, &brokenUsers); err != nil {
		return nil, err
	}
	compaction := map[string]any{"charsIn": int64(charsIn), "charsOut": int64(charsOut), "ratio": nil, "ok": okN, "failed": failedN,
		"lastNightAt": lastNight, "brokenUsers": brokenUsers}
	if charsOut > 0 {
		compaction["ratio"] = float64(int(charsIn/charsOut*10)) / 10
	}
	out["enabledUsers"], out["activeUsers"], out["notes"], out["costUsd"] = enabled, active, notes, cost
	out["cost"] = map[string]float64{"collect": cost - compactCost, "compact": compactCost}
	out["compaction"] = compaction

	// the records by level and the commitments: the last snapshot of the month (the current one — the latest)
	levels := Levels{}
	commitments := map[string]int{"open": 0, "closedInMonth": 0, "overdue": 0}
	rows, err := s.Pool.Query(ctx, `WITH last AS (SELECT max(day) AS day FROM comm_level_days WHERE day >= $1 AND day < $2)
		SELECT l.level, sum(l.records)::int FROM comm_level_days l, last WHERE l.day = last.day GROUP BY 1
		UNION ALL SELECT 'commitments_closed_month', COALESCE(sum(records), 0)::int FROM comm_level_days WHERE day >= $1 AND day < $2 AND level = 'commitments_closed'`, first, next)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var level string
		var n int
		if err := rows.Scan(&level, &n); err != nil {
			rows.Close()
			return nil, err
		}
		switch level {
		case "commitments_open":
			commitments["open"] = n
		case "commitments_overdue":
			commitments["overdue"] = n
		case "commitments_closed_month":
			commitments["closedInMonth"] = n
		default:
			levels.add(level, n)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out["levels"], out["commitments"] = levels, commitments

	// the outcomes of the ticks of the collection and the messages by source
	outcomes := map[string]int{"ok": 0, "busy": 0, "rate_limited": 0, "source_access": 0, "timeout": 0, "error": 0}
	rows, err = s.Pool.Query(ctx, `SELECT `+outcomeSQL+`, count(*) FROM task_runs tr JOIN scheduled_tasks t ON t.id = tr.task_id
		WHERE t.kind = 'memory_collect' AND tr.started_at >= $1 AND tr.started_at < $2 AND tr.status <> 'running' GROUP BY 1`, first, next)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			rows.Close()
			return nil, err
		}
		outcomes[k] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	type bySource struct {
		ItemID   uuid.UUID `json:"itemId"`
		Source   string    `json:"source"`
		Title    string    `json:"title"`
		Messages int       `json:"messages"`
	}
	sources := []bySource{}
	rows, err = s.Pool.Query(ctx, `SELECT d.item_id, d.source, COALESCE(NULLIF(c.title, ''), c.name, d.source), sum(d.messages)::int
		FROM comm_usage_source_days d LEFT JOIN catalog_items c ON c.id = d.item_id
		WHERE d.day >= $1 AND d.day < $2 GROUP BY 1, 2, 3 ORDER BY 4 DESC`, first, next)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var x bySource
		if err := rows.Scan(&x.ItemID, &x.Source, &x.Title, &x.Messages); err != nil {
			rows.Close()
			return nil, err
		}
		sources = append(sources, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out["collect"] = map[string]any{"outcomes": outcomes, "messagesBySource": sources}

	items, err := s.usageRows(ctx, first, next, st.MonthlyLimitUSD, brokenOnly)
	if err != nil {
		return nil, err
	}
	out["items"] = items
	return out, nil
}

func (s *Service) usageRows(ctx context.Context, first, next time.Time, limit float64, brokenOnly bool) ([]UsageRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT u.id, u.email, cs.notes_count, COALESCE(cu.runs, 0), COALESCE(cu.cost_usd, 0)::float8,
			COALESCE(cu.compact_cost_usd, 0)::float8, cc.last_at, COALESCE(cc.failed_nights, 0) >= $2,
			lc.at, lc.outcome
		FROM comm_settings cs JOIN users u ON u.id = cs.user_id
		LEFT JOIN comm_usage cu ON cu.user_id = cs.user_id AND cu.month = $1
		LEFT JOIN comm_compact_state cc ON cc.user_id = cs.user_id
		LEFT JOIN LATERAL (SELECT tr.started_at AS at, `+outcomeSQL+` AS outcome FROM task_runs tr JOIN scheduled_tasks t ON t.id = tr.task_id
			WHERE t.user_id = cs.user_id AND t.kind = 'memory_collect' AND tr.status <> 'running' ORDER BY tr.started_at DESC LIMIT 1) lc ON true
		WHERE (cs.notes_count > 0 OR cu.runs > 0 OR cc.user_id IS NOT NULL) AND (NOT $3 OR COALESCE(cc.failed_nights, 0) >= $2)
		ORDER BY 5 DESC, 3 DESC, u.email LIMIT 500`, first, BrokenNights, brokenOnly)
	if err != nil {
		return nil, err
	}
	items := []UsageRow{}
	by := map[uuid.UUID]int{}
	for rows.Next() {
		var x UsageRow
		var at *time.Time
		var outcome *string
		if err := rows.Scan(&x.UserID, &x.Email, &x.Notes, &x.Runs, &x.CostUSD, &x.CostCompact, &x.LastCompactAt, &x.CompactBroken, &at, &outcome); err != nil {
			rows.Close()
			return nil, err
		}
		x.CostCollect = x.CostUSD - x.CostCompact
		if limit > 0 {
			x.LimitUsedPct = int(x.CostUSD / limit * 100)
		}
		if at != nil && outcome != nil {
			x.LastCollect = &LastCollect{At: *at, Outcome: *outcome}
		}
		by[x.UserID] = len(items)
		items = append(items, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return items, nil
	}
	ids := make([]uuid.UUID, 0, len(items))
	for _, x := range items {
		ids = append(ids, x.UserID)
	}
	// the levels and the open commitments of each user: the last snapshot of the month
	rows, err = s.Pool.Query(ctx, `SELECT l.user_id, l.level, l.records FROM comm_level_days l
		WHERE l.user_id = ANY($3) AND l.day = (SELECT max(day) FROM comm_level_days m WHERE m.user_id = l.user_id AND m.day >= $1 AND m.day < $2)`,
		first, next, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var uid uuid.UUID
		var level string
		var n int
		if err := rows.Scan(&uid, &level, &n); err != nil {
			return nil, err
		}
		x := &items[by[uid]]
		if level == "commitments_open" {
			x.OpenCommitments = n
		} else {
			x.Levels.add(level, n)
		}
	}
	return items, rows.Err()
}

// UsageDay is a day of the charts of the usage.
type UsageDay struct {
	Day         string  `json:"day"`
	CostCollect float64 `json:"costCollect"`
	CostCompact float64 `json:"costCompact"`
	Messages    int     `json:"messages"`
	Created     int     `json:"created"`
	Levels      Levels  `json:"levels"`
}

// UsageDaily gives every day of a month for the charts (FTR.NAB.MEM-0001 R26): of the instance, or of one user.
func (s *Service) UsageDaily(ctx context.Context, first time.Time, user *uuid.UUID) ([]UsageDay, error) {
	next := first.AddDate(0, 1, 0)
	days := []UsageDay{}
	index := map[string]int{}
	for d := first; d.Before(next); d = d.AddDate(0, 0, 1) {
		index[d.Format(time.DateOnly)] = len(days)
		days = append(days, UsageDay{Day: d.Format(time.DateOnly)})
	}
	at := func(t time.Time) *UsageDay { return &days[index[t.Format(time.DateOnly)]] }
	rows, err := s.Pool.Query(ctx, `SELECT day, sum(cost_collect)::float8, sum(cost_compact)::float8, sum(created)::int FROM comm_usage_days
		WHERE day >= $1 AND day < $2 AND ($3::uuid IS NULL OR user_id = $3) GROUP BY 1`, first, next, user)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d time.Time
		var x UsageDay
		if err := rows.Scan(&d, &x.CostCollect, &x.CostCompact, &x.Created); err != nil {
			rows.Close()
			return nil, err
		}
		p := at(d)
		p.CostCollect, p.CostCompact, p.Created = x.CostCollect, x.CostCompact, x.Created
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = s.Pool.Query(ctx, `SELECT day, sum(messages)::int FROM comm_usage_source_days
		WHERE day >= $1 AND day < $2 AND ($3::uuid IS NULL OR user_id = $3) GROUP BY 1`, first, next, user)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d time.Time
		var n int
		if err := rows.Scan(&d, &n); err != nil {
			rows.Close()
			return nil, err
		}
		at(d).Messages = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = s.Pool.Query(ctx, `SELECT day, level, sum(records)::int FROM comm_level_days
		WHERE day >= $1 AND day < $2 AND ($3::uuid IS NULL OR user_id = $3) AND level IN ('day','week','month','thread') GROUP BY 1, 2`, first, next, user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var d time.Time
		var level string
		var n int
		if err := rows.Scan(&d, &level, &n); err != nil {
			return nil, err
		}
		at(d).Levels.add(level, n)
	}
	return days, rows.Err()
}
