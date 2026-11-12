package catalogusage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// dailyRow is one row of catalog_usage_daily.
type dailyRow struct {
	Day                 time.Time
	ItemID              uuid.UUID
	ItemName, ItemTitle string
	ItemType            string
	GroupID             uuid.UUID
	GroupName           string
	GroupBuiltin        *string
	CallerKind, Caller  string
	Channel             string
	Mode                *string
	Available           bool
	Calls, OK           int64
	Users, Sessions     int64
	Admin, Service      int64
	Errors, Latency     map[string]int64
}

// Recompute is the tick of the worker (tech §«Агрегация в worker», R5): it
// recomputes the open days — the current one and no more than three past
// ones — and closes the past days after CATALOG_USAGE_CLOSE_AFTER. The goal
// metric and the monthly slice follow a closed day. Backfill does not exist:
// the first day is the day of the rollout (A10).
func (s *Service) Recompute(ctx context.Context) error {
	start := time.Now()
	defer func() { metrics.CatalogUsageRecomputeSeconds.Observe(time.Since(start).Seconds()) }()
	now := s.clock()
	today := Today(now)
	var lastClosed *time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT max(day)::timestamp AT TIME ZONE 'UTC' FROM catalog_usage_state WHERE closed_at IS NOT NULL`).Scan(&lastClosed); err != nil {
		return err
	}
	// MIG-04: the first run of the rollout computes the current day only; the past days
	// are recomputed later within three days (tech §«Агрегация в worker» п. 1).
	var lastDay *time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT max(day)::timestamp AT TIME ZONE 'UTC' FROM catalog_usage_state`).Scan(&lastDay); err != nil {
		return err
	}
	from := today
	switch {
	case lastDay == nil:
		from = today
	case lastClosed != nil:
		from = Today(*lastClosed).AddDate(0, 0, 1)
	default:
		from = today.AddDate(0, 0, -3)
	}
	if from.After(today) {
		from = today
	}
	var open []time.Time
	for d := from; !d.After(today); d = d.AddDate(0, 0, 1) {
		var closed bool
		if err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM catalog_usage_state WHERE day = $1 AND closed_at IS NOT NULL)`, d).Scan(&closed); err != nil {
			return err
		}
		if !closed {
			open = append(open, d)
		}
	}
	for _, d := range open {
		if err := s.computeDay(ctx, d); err != nil {
			return fmt.Errorf("day %s: %w", d.Format("2006-01-02"), err)
		}
	}
	// a past day stays as it is after the close time of the next morning (R5)
	closeAt, err := closeTime(now, s.Cfg.CloseAfter)
	if err != nil {
		return err
	}
	for _, d := range open {
		if !d.Before(today) || now.Before(closeAt) {
			continue
		}
		if err := s.closeDay(ctx, d); err != nil {
			return err
		}
	}
	s.lag(ctx, now)
	return nil
}

func closeTime(now time.Time, hhmm string) (time.Time, error) {
	var h, m int
	if _, err := fmt.Sscanf(hhmm, "%d:%d", &h, &m); err != nil {
		return now, fmt.Errorf("CATALOG_USAGE_CLOSE_AFTER: %w", err)
	}
	t := Today(now).Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
	return t, nil
}

func (s *Service) lag(ctx context.Context, now time.Time) {
	var computed *time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT max(computed_at) FROM catalog_usage_state`).Scan(&computed); err == nil && computed != nil {
		metrics.CatalogUsageLag.WithLabelValues("recompute").Set(now.Sub(*computed).Seconds())
	}
	var lastClosed *time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT max(closed_at) FROM catalog_usage_state`).Scan(&lastClosed); err == nil && lastClosed != nil {
		metrics.CatalogUsageLag.WithLabelValues("close").Set(now.Sub(*lastClosed).Seconds())
	}
	var daily, monthly int64
	_ = s.Pool.QueryRow(ctx, `SELECT count(*) FROM catalog_usage_daily`).Scan(&daily)
	_ = s.Pool.QueryRow(ctx, `SELECT count(*) FROM catalog_usage_monthly`).Scan(&monthly)
	metrics.CatalogUsageRows.WithLabelValues("daily").Set(float64(daily))
	metrics.CatalogUsageRows.WithLabelValues("monthly").Set(float64(monthly))
}

// computeDay replaces the rows of a day in one transaction (tech §«Агрегация
// в worker» п. 7): DELETE and INSERT together, then the watermark.
func (s *Service) computeDay(ctx context.Context, day time.Time) error {
	rows, err := s.collect(ctx, day)
	if err != nil {
		return err
	}
	return postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM catalog_usage_daily WHERE day = $1`, day); err != nil {
			return err
		}
		for _, r := range rows {
			errorsRaw, _ := json.Marshal(r.Errors)
			latRaw, _ := json.Marshal(r.Latency)
			if _, err := tx.Exec(ctx, `INSERT INTO catalog_usage_daily (day, item_id, item_name, item_title, item_type, group_id, group_name,
					group_builtin, caller_kind, caller, channel, mode, available, calls, ok, errors_by_code, users, sessions,
					admin_calls, service_calls, latency_buckets, computed_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,now())
				ON CONFLICT DO NOTHING`, day, r.ItemID, r.ItemName, r.ItemTitle, r.ItemType, r.GroupID, r.GroupName, r.GroupBuiltin,
				r.CallerKind, r.Caller, r.Channel, r.Mode, r.Available, r.Calls, r.OK, errorsRaw, r.Users, r.Sessions, r.Admin,
				r.Service, latRaw); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO catalog_usage_state (day, computed_at, rows) VALUES ($1, now(), $2)
			ON CONFLICT (day) DO UPDATE SET computed_at = now(), rows = $2`, day, len(rows))
		return err
	})
}

// closeDay marks a day as closed and computes what follows it: the monthly
// slice at the end of a month and the goal metric (R14, tech §«Агрегация»).
func (s *Service) closeDay(ctx context.Context, day time.Time) error {
	if err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE catalog_usage_state SET closed_at = now() WHERE day = $1`, day)
		return err
	}); err != nil {
		return err
	}
	if day.AddDate(0, 0, 1).Month() != day.Month() {
		if err := s.ComputeMonth(ctx, Date(day.Year(), day.Month(), 1)); err != nil {
			return err
		}
	}
	if err := s.ComputeGoal(ctx, day); err != nil {
		return err
	}
	return nil
}

// collect reads the audit of a day and builds the rows: the calls of the
// day, the group slices, the zero rows of the published items and the
// telemetry of skills (tech §«Агрегация в worker» п. 2–6).
func (s *Service) collect(ctx context.Context, day time.Time) ([]dailyRow, error) {
	from := day
	to := day.AddDate(0, 0, 1)
	rows, err := s.callRows(ctx, from, to)
	if err != nil {
		return nil, err
	}
	groups, err := s.groupRows(ctx, from, to)
	if err != nil {
		return nil, err
	}
	skills, err := s.skillRows(ctx, day)
	if err != nil {
		return nil, err
	}
	zero, err := s.zeroRows(ctx, day)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := make([]dailyRow, 0, len(rows)+len(groups)+len(skills)+len(zero))
	for _, r := range append(append(append(rows, groups...), skills...), zero...) {
		key := r.key()
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, r)
	}
	return out, nil
}

func (r dailyRow) key() string {
	b := ""
	if r.GroupBuiltin != nil {
		b = *r.GroupBuiltin
	}
	m := ""
	if r.Mode != nil {
		m = *r.Mode
	}
	return r.ItemID.String() + "|" + r.GroupID.String() + "|" + r.CallerKind + "|" + r.Caller + "|" + r.Channel + "|" + b + "|" + m
}

// callRows aggregates the audit of the day at the item level (tech §3 п. 3–4).
func (s *Service) callRows(ctx context.Context, from, to time.Time) ([]dailyRow, error) {
	const src = `WITH calls AS (
		SELECT coalesce(a.item_id, '` + ZeroUUID + `'::uuid) AS item_id, coalesce(a.server, '') AS server,
			CASE WHEN a.client_id IS NOT NULL THEN 'client' WHEN a.agent_kind = 'service' THEN 'service' ELSE 'personal' END AS caller_kind,
			CASE WHEN a.client_id IS NOT NULL THEN a.client_id::text WHEN a.agent_kind = 'service' THEN a.agent ELSE '' END AS caller,
			CASE WHEN a.client_id IS NOT NULL OR a.agent_kind = 'service' THEN '' ELSE coalesce(a.channel, '') END AS channel,
			a.user_id, a.session_id, coalesce(a.result_code, 'ok') AS code, a.duration_ms,
			coalesce(u.is_admin, false) AS is_admin,
			(i.published AND NOT i.in_development) AS available
		FROM audit a
		LEFT JOIN users u ON u.id = a.user_id
		LEFT JOIN catalog_items i ON i.id = a.item_id
		WHERE a.at >= $1 AND a.at < $2
	)`
	base := map[string]*dailyRow{}
	get := func(item uuid.UUID, server, callerKind, caller, channel string, available bool) *dailyRow {
		key := item.String() + "|" + callerKind + "|" + caller + "|" + channel
		r := base[key]
		if r == nil {
			r = &dailyRow{Day: from, ItemID: item, ItemName: server, ItemType: "unknown", GroupID: Zero,
				CallerKind: callerKind, Caller: caller, Channel: channel, Available: available}
			base[key] = r
		}
		return r
	}
	rows, err := s.Pool.Query(ctx, src+`
		SELECT item_id, server, caller_kind, caller, channel, available,
			count(*)::bigint, count(*) FILTER (WHERE code = 'ok')::bigint,
			count(DISTINCT user_id)::bigint, count(DISTINCT session_id)::bigint,
			count(*) FILTER (WHERE is_admin AND caller_kind = 'personal')::bigint,
			count(*) FILTER (WHERE caller_kind <> 'personal')::bigint
		FROM calls GROUP BY item_id, server, caller_kind, caller, channel, available`, from, to)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item, callerKind, caller, channel, server string
		var available bool
		var calls, ok, users, sessions, admin, service int64
		if err := rows.Scan(&item, &server, &callerKind, &caller, &channel, &available, &calls, &ok, &users, &sessions, &admin, &service); err != nil {
			rows.Close()
			return nil, err
		}
		id, _ := uuid.Parse(item)
		r := get(id, server, callerKind, caller, channel, available)
		r.Calls += calls
		r.OK += ok
		r.Users += users
		r.Sessions += sessions
		r.Admin += admin
		r.Service += service
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// errors by code
	rows, err = s.Pool.Query(ctx, src+`
		SELECT item_id, server, caller_kind, caller, channel, available, code, count(*)::bigint
		FROM calls GROUP BY item_id, server, caller_kind, caller, channel, available, code`, from, to)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item, server, callerKind, caller, channel, code string
		var available bool
		var n int64
		if err := rows.Scan(&item, &server, &callerKind, &caller, &channel, &available, &code, &n); err != nil {
			rows.Close()
			return nil, err
		}
		id, _ := uuid.Parse(item)
		r := get(id, server, callerKind, caller, channel, available)
		if code != "ok" {
			if r.Errors == nil {
				r.Errors = map[string]int64{}
			}
			r.Errors[code] += n
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// latency buckets: every call counts in the bucket of its own duration, the boundaries
	// are those of the proxy histogram (R4, AGG-07)
	rows, err = s.Pool.Query(ctx, src+`,
		bucketed AS (
			SELECT item_id, server, caller_kind, caller, channel, available,
				CASE
					WHEN duration_ms <= 100 THEN 100
					WHEN duration_ms <= 250 THEN 250
					WHEN duration_ms <= 500 THEN 500
					WHEN duration_ms <= 1000 THEN 1000
					WHEN duration_ms <= 2500 THEN 2500
					WHEN duration_ms <= 5000 THEN 5000
					WHEN duration_ms <= 10000 THEN 10000
					WHEN duration_ms <= 30000 THEN 30000
					WHEN duration_ms <= 60000 THEN 60000
					WHEN duration_ms <= 120000 THEN 120000
					WHEN duration_ms <= 300000 THEN 300000
					ELSE 600000
				END AS bound
			FROM calls WHERE duration_ms IS NOT NULL
		)
		SELECT item_id, server, caller_kind, caller, channel, available, bound, count(*)::bigint
		FROM bucketed GROUP BY item_id, server, caller_kind, caller, channel, available, bound`, from, to)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item, server, callerKind, caller, channel string
		var available bool
		var bound, n int64
		if err := rows.Scan(&item, &server, &callerKind, &caller, &channel, &available, &bound, &n); err != nil {
			rows.Close()
			return nil, err
		}
		id, _ := uuid.Parse(item)
		r := get(id, server, callerKind, caller, channel, available)
		if r.Latency == nil {
			r.Latency = map[string]int64{}
		}
		r.Latency[fmt.Sprint(bound)] += n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// names and types of the items come from the catalog of the day
	if err := s.fillItems(ctx, base); err != nil {
		return nil, err
	}
	out := make([]dailyRow, 0, len(base))
	for _, r := range base {
		if r.Errors == nil {
			r.Errors = map[string]int64{}
		}
		if r.Latency == nil {
			r.Latency = map[string]int64{}
		}
		out = append(out, *r)
	}
	return out, nil
}

// fillItems copies the snapshot of the item into the rows.
func (s *Service) fillItems(ctx context.Context, rows map[string]*dailyRow) error {
	items, err := s.items(ctx)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if it := items[r.ItemID]; it != nil {
			r.ItemName, r.ItemTitle, r.ItemType, r.Mode = it.Name, it.Title, it.Type, it.Mode
			if r.ItemName == "" {
				r.ItemName = it.Name
			}
		}
	}
	return nil
}

// groupRows builds the group slices of personal calls: a call of an item
// granted to several groups is counted in each of them (R18).
func (s *Service) groupRows(ctx context.Context, from, to time.Time) ([]dailyRow, error) {
	const calls = `WITH calls AS (
		SELECT a.item_id, a.user_id, a.session_id, coalesce(a.channel, '') AS channel,
			coalesce(a.result_code, 'ok') AS code, a.duration_ms, coalesce(u.is_admin, false) AS is_admin
		FROM audit a LEFT JOIN users u ON u.id = a.user_id
		WHERE a.at >= $1 AND a.at < $2 AND a.item_id IS NOT NULL AND a.agent_kind = 'personal' AND a.client_id IS NULL),
	expanded AS (
		SELECT c.item_id, c.user_id, c.session_id, c.channel, c.code, c.duration_ms, c.is_admin, g.id AS group_id, g.name, g.builtin
		FROM calls c
		JOIN access_grants ag ON ag.kind = 'catalog_item' AND ag.item_id = c.item_id
		JOIN user_groups g ON g.id = ag.group_id
		WHERE g.builtin = 'all' OR (g.builtin = 'admins' AND c.is_admin)
			OR EXISTS (SELECT 1 FROM user_group_members m WHERE m.group_id = g.id AND m.user_id = c.user_id))`
	out := map[string]*dailyRow{}
	get := func(item, group uuid.UUID, name string, builtin *string, channel string) *dailyRow {
		key := item.String() + "|" + group.String() + "|" + channel
		r := out[key]
		if r == nil {
			r = &dailyRow{Day: from, ItemID: item, GroupID: group, GroupName: name, GroupBuiltin: builtin,
				CallerKind: CallerPersonal, Channel: channel, Available: true, Errors: map[string]int64{}, Latency: map[string]int64{}}
			out[key] = r
		}
		return r
	}
	rows, err := s.Pool.Query(ctx, calls+`
		SELECT item_id, group_id, name, builtin, channel, count(*)::bigint, count(*) FILTER (WHERE code = 'ok')::bigint,
			count(DISTINCT user_id)::bigint, count(DISTINCT session_id)::bigint, count(*) FILTER (WHERE is_admin)::bigint
		FROM expanded GROUP BY item_id, group_id, name, builtin, channel`, from, to)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item, group, name, channel string
		var builtin *string
		var n, ok, users, sessions, admin int64
		if err := rows.Scan(&item, &group, &name, &builtin, &channel, &n, &ok, &users, &sessions, &admin); err != nil {
			rows.Close()
			return nil, err
		}
		i, _ := uuid.Parse(item)
		g, _ := uuid.Parse(group)
		r := get(i, g, name, builtin, channel)
		r.Calls, r.OK, r.Users, r.Sessions, r.Admin = n, ok, users, sessions, admin
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = s.Pool.Query(ctx, calls+`
		SELECT item_id, group_id, name, builtin, channel, code, count(*)::bigint
		FROM expanded GROUP BY item_id, group_id, name, builtin, channel, code`, from, to)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item, group, name, channel, code string
		var builtin *string
		var n int64
		if err := rows.Scan(&item, &group, &name, &builtin, &channel, &code, &n); err != nil {
			rows.Close()
			return nil, err
		}
		i, _ := uuid.Parse(item)
		g, _ := uuid.Parse(group)
		if code != "ok" {
			get(i, g, name, builtin, channel).Errors[code] += n
		}
	}
	rows.Close()
	outRows := make([]dailyRow, 0, len(out))
	for _, r := range out {
		outRows = append(outRows, *r)
	}
	return outRows, rows.Err()
}

// skillRows builds the rows of skills from skill_usage_events (R24): calls —
// activations, sessions — sessions the skill was collected into or activated.
func (s *Service) skillRows(ctx context.Context, day time.Time) ([]dailyRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT item_id, max(item_name), coalesce(channel, ''),
			count(*) FILTER (WHERE kind = 'activated')::bigint,
			count(*) FILTER (WHERE kind = 'activated')::bigint,
			count(DISTINCT session_id)::bigint,
			count(DISTINCT user_id)::bigint
		FROM skill_usage_events WHERE at >= $1 AND at < $2
		GROUP BY item_id, channel`, day, day.AddDate(0, 0, 1))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []dailyRow{}
	for rows.Next() {
		var id uuid.UUID
		var name, channel string
		var calls, ok, sessions, users int64
		if err := rows.Scan(&id, &name, &channel, &calls, &ok, &sessions, &users); err != nil {
			return nil, err
		}
		out = append(out, dailyRow{Day: day, ItemID: id, ItemName: name, ItemType: "skill", GroupID: Zero,
			CallerKind: CallerPersonal, Channel: channel, Available: true, Calls: calls, OK: ok, Sessions: sessions, Users: users,
			Errors: map[string]int64{}, Latency: map[string]int64{}})
	}
	return out, rows.Err()
}

// zeroRows writes the zero rows of every published item for every day of its
// availability: «0 calls in 30 days» must differ from «no aggregates» (R6, R8).
func (s *Service) zeroRows(ctx context.Context, day time.Time) ([]dailyRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT i.id, i.name, coalesce(i.title, ''), i.type, i.mode
		FROM catalog_items i JOIN catalog_item_publications p ON p.item_id = i.id
		WHERE i.published AND NOT i.in_development AND i.type = 'mcp'
		  AND $1::date >= p.published_at::date AND (p.unpublished_at IS NULL OR $1::date < p.unpublished_at::date)`, day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []dailyRow{}
	for rows.Next() {
		var r dailyRow
		if err := rows.Scan(&r.ItemID, &r.ItemName, &r.ItemTitle, &r.ItemType, &r.Mode); err != nil {
			return nil, err
		}
		r.Day, r.GroupID, r.CallerKind, r.Available = day, Zero, CallerPersonal, true
		r.Errors, r.Latency = map[string]int64{}, map[string]int64{}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ComputeMonth rebuilds the monthly slice from the daily rows (tech §8).
func (s *Service) ComputeMonth(ctx context.Context, month time.Time) error {
	start := Date(month.Year(), month.Month(), 1)
	end := start.AddDate(0, 1, -1)
	return postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM catalog_usage_monthly WHERE month = $1`, start); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO catalog_usage_monthly (month, item_id, item_name, item_title, item_type, group_id, group_name,
				group_builtin, caller_kind, caller, channel, mode, available_days, calls, ok, errors_by_code, users, sessions,
				admin_calls, service_calls, latency_buckets, computed_at)
			SELECT $1, d.item_id, max(d.item_name), max(d.item_title), max(d.item_type), d.group_id, max(d.group_name),
				max(d.group_builtin), d.caller_kind, d.caller, d.channel, max(d.mode),
				count(*) FILTER (WHERE d.available)::int, sum(d.calls), sum(d.ok),
				(SELECT jsonb_object_agg(code, n) FROM (
					SELECT e.key AS code, sum(e.value::bigint) AS n FROM catalog_usage_daily d2
						CROSS JOIN LATERAL jsonb_each_text(d2.errors_by_code) e
					WHERE d2.day >= $1 AND d2.day <= $2 AND d2.item_id = d.item_id AND d2.group_id = d.group_id
					  AND d2.caller_kind = d.caller_kind AND d2.caller = d.caller AND d2.channel = d.channel
					GROUP BY e.key) x),
				sum(d.users), sum(d.sessions), sum(d.admin_calls), sum(d.service_calls),
				(SELECT jsonb_object_agg(bound, n) FROM (
					SELECT e.key AS bound, sum(e.value::bigint) AS n FROM catalog_usage_daily d2
						CROSS JOIN LATERAL jsonb_each_text(d2.latency_buckets) e
					WHERE d2.day >= $1 AND d2.day <= $2 AND d2.item_id = d.item_id AND d2.group_id = d.group_id
					  AND d2.caller_kind = d.caller_kind AND d2.caller = d.caller AND d2.channel = d.channel
					GROUP BY e.key) x),
				now()
			FROM catalog_usage_daily d WHERE d.day >= $1 AND d.day <= $2
			GROUP BY d.item_id, d.group_id, d.caller_kind, d.caller, d.channel`, start, end)
		return err
	})
}

// ComputeGoal computes the row of the goal metric of the window that ends on
// day (R14, arch A16): the only place of the 90-day request over the daily
// aggregates. The baseline is fixed once at the end of the first full window.
func (s *Service) ComputeGoal(ctx context.Context, day time.Time) error {
	from := day.AddDate(0, 0, -(s.Cfg.ReferenceDays - 1))
	var dead, total int
	if err := s.Pool.QueryRow(ctx, `
		WITH win AS (
			SELECT p.item_id FROM catalog_item_publications p
			JOIN generate_series($1::date, $2::date, interval '1 day') d(day)
			  ON d.day::date >= p.published_at::date AND (p.unpublished_at IS NULL OR d.day::date < p.unpublished_at::date)
			GROUP BY p.item_id),
		base AS (
			SELECT i.id, coalesce((SELECT sum(x.calls - x.admin_calls - x.service_calls)
				FROM catalog_usage_daily x
				WHERE x.item_id = i.id AND x.group_id = '`+ZeroUUID+`'::uuid AND x.caller_kind = 'personal'
				  AND x.available AND x.day BETWEEN $1 AND $2), 0) AS human_calls
			FROM catalog_items i JOIN win ON win.item_id = i.id
			WHERE i.type = 'mcp' AND i.published AND NOT i.in_development AND coalesce(i.status, '') <> 'error'
			  AND EXISTS (SELECT 1 FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
				WHERE ag.kind = 'catalog_item' AND ag.item_id = i.id AND g.builtin IS DISTINCT FROM 'admins'))
		SELECT count(*) FILTER (WHERE human_calls = 0)::int, count(*)::int FROM base`, from, day).Scan(&dead, &total); err != nil {
		return err
	}
	share := 0.0
	if total > 0 {
		share = round1(100 * float64(dead) / float64(total))
	}
	var dropped, droppedWithCalls int
	if err := s.Pool.QueryRow(ctx, `
		SELECT count(*)::int, count(*) FILTER (WHERE calls > 0)::int FROM (
			SELECT p.item_id, coalesce((SELECT sum(x.calls) FROM catalog_usage_daily x
				WHERE x.item_id = p.item_id AND x.group_id = '`+ZeroUUID+`'::uuid AND x.day BETWEEN $1 AND $2), 0) AS calls
			FROM catalog_item_publications p
			LEFT JOIN catalog_items c ON c.id = p.item_id
			LEFT JOIN catalog_item_tombstones t ON t.item_id = p.item_id
			WHERE p.published_at::date <= $2 AND (t.item_id IS NOT NULL OR NOT c.published OR c.in_development OR c.status = 'error')
			GROUP BY p.item_id) x`, from, day).Scan(&dropped, &droppedWithCalls); err != nil {
		return err
	}
	// the baseline is fixed once, at the end of the first full window (R14)
	baseline, baselineDay, err := s.fixBaseline(ctx, from, day, share)
	if err != nil {
		return err
	}
	delta := share
	if baseline != nil {
		delta = round1(share - *baseline)
	}
	metrics.CatalogUsageGoalShare.Set(share)
	return postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM catalog_usage_goal_daily WHERE day = $1`, day); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO catalog_usage_goal_daily (day, window_from, window_to, dead, total, share, baseline,
				baseline_day, delta, dropped_total, dropped_with_calls, computed_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())`, day, from, day, dead, total, share, baseline, baselineDay, delta,
			dropped, droppedWithCalls)
		return err
	})
}

func (s *Service) fixBaseline(ctx context.Context, from, day time.Time, share float64) (*float64, *time.Time, error) {
	var raw []byte
	if err := s.Pool.QueryRow(ctx, `SELECT value FROM settings WHERE key = 'catalog_usage_goal'`).Scan(&raw); err != nil {
		if postgres.IsNoRows(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	var st struct {
		Baseline   *float64 `json:"baseline"`
		BaselineTo *string  `json:"baselineTo"`
	}
	_ = json.Unmarshal(raw, &st)
	if st.Baseline != nil {
		var d *time.Time
		if st.BaselineTo != nil {
			if v, err := time.Parse("2006-01-02", *st.BaselineTo); err == nil {
				d = &v
			}
		}
		return st.Baseline, d, nil
	}
	var dataFrom *time.Time
	_ = s.Pool.QueryRow(ctx, `SELECT min(day)::timestamp AT TIME ZONE 'UTC' FROM catalog_usage_state`).Scan(&dataFrom)
	if dataFrom == nil || from.Before(Today(*dataFrom)) {
		return nil, nil, nil // the first full window is not over yet
	}
	v, _ := json.Marshal(map[string]any{"baseline": share, "baselineTo": day.Format("2006-01-02"), "verifiedManuallyAt": nil})
	if _, err := s.Pool.Exec(ctx, `UPDATE settings SET value = $1, updated_at = now() WHERE key = 'catalog_usage_goal'`, v); err != nil {
		return nil, nil, err
	}
	metrics.CatalogUsageGoalShare.Set(share)
	return &share, &day, nil
}

func round1(v float64) float64 {
	if v >= 0 {
		return float64(int64(v*10+0.5)) / 10
	}
	return float64(int64(v*10-0.5)) / 10
}

// Sweep removes the expired rows (FTR.NAB.CHAT-0005 tech §«Ретеншен»):
// aggregates first, then the intervals, tombstones and pins of items whose
// history is gone.
func (s *Service) Sweep(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	run := func(name, q string, args ...any) error {
		tag, err := s.Pool.Exec(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		out[name] = tag.RowsAffected()
		return nil
	}
	if err := run("catalog usage daily", `DELETE FROM catalog_usage_daily WHERE day < current_date - $1::int`, s.Cfg.RetentionDays); err != nil {
		return out, err
	}
	if err := run("catalog usage monthly", `DELETE FROM catalog_usage_monthly WHERE month < (current_date - make_interval(months => $1::int))`, s.Cfg.MonthlyMonths); err != nil {
		return out, err
	}
	if err := run("catalog usage goal", `DELETE FROM catalog_usage_goal_daily WHERE day < current_date - $1::int`, s.Cfg.RetentionDays); err != nil {
		return out, err
	}
	if err := run("catalog usage state", `DELETE FROM catalog_usage_state WHERE day < current_date - $1::int`, s.Cfg.RetentionDays); err != nil {
		return out, err
	}
	if err := run("skill usage events", `DELETE FROM skill_usage_events WHERE at < now() - interval '90 days'`); err != nil {
		return out, err
	}
	if err := run("catalog usage pins", `DELETE FROM catalog_item_pins
		WHERE coalesce(released_at, review_at) < now() - interval '400 days'`); err != nil {
		return out, err
	}
	// intervals and tombstones live until the last aggregate of the item is gone (R23)
	if err := run("catalog item publications", `DELETE FROM catalog_item_publications p
		WHERE NOT EXISTS (SELECT 1 FROM catalog_items c WHERE c.id = p.item_id)
		  AND NOT EXISTS (SELECT 1 FROM catalog_usage_daily d WHERE d.item_id = p.item_id)
		  AND NOT EXISTS (SELECT 1 FROM catalog_usage_monthly m WHERE m.item_id = p.item_id)`); err != nil {
		return out, err
	}
	if err := run("catalog item tombstones", `DELETE FROM catalog_item_tombstones t
		WHERE NOT EXISTS (SELECT 1 FROM catalog_usage_daily d WHERE d.item_id = t.item_id)
		  AND NOT EXISTS (SELECT 1 FROM catalog_usage_monthly m WHERE m.item_id = t.item_id)`); err != nil {
		return out, err
	}
	slog.InfoContext(ctx, "catalog usage retention", "rows", out)
	return out, nil
}
