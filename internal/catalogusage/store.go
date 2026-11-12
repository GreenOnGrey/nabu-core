package catalogusage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Service is the report and the aggregation of catalog usage.
type Service struct {
	Pool   *pgxpool.Pool
	Cfg    Config
	Events events.Publisher
	// Access is the access of users by groups: the cascade of unpublishing and
	// revoking a grant runs in the same transaction (R27, R28, R32).
	Access *access.Service
	// Now is the clock of tests; nil — time.Now.
	Now func() time.Time
}

func (s *Service) clock() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Filter is the filter of the flat table (R17).
type Filter struct {
	From, To                        time.Time
	Step                            string
	ItemID                          uuid.UUID
	Groups                          []uuid.UUID
	CallerKind, Channel, Mode, Hint string
	IncludeUnpublished              bool
	Limit                           int
	Cursor                          *httpx.Cursor
}

// Period is the period of the summary and of the card.
type Period struct {
	From, To           time.Time
	IncludeUnpublished bool
}

// Date is a date without the time of the day (UTC).
func Date(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

// Today is the day of t in UTC.
func Today(t time.Time) time.Time { return Date(t.Year(), t.Month(), t.Day()) }

// ParseDay reads a date of a query parameter; nil — not given.
func ParseDay(s string) (*time.Time, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// InvalidPeriod is 422 invalid_period of the contract.
func InvalidPeriod(msg string) error {
	if msg == "" {
		msg = "the period is invalid: from is after to"
	}
	e := badPeriod(msg)
	return e
}

// state is the read side of the aggregates: the aggregates, the reference
// slice and the metadata of an item.
func (s *Service) stateMeta(ctx context.Context) (*time.Time, *time.Time, error) {
	var from *time.Time
	var updated *time.Time
	row := s.Pool.QueryRow(ctx, `SELECT min(day)::timestamp AT TIME ZONE 'UTC', max(computed_at) FROM catalog_usage_state`)
	if err := row.Scan(&from, &updated); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	// dataFrom is the date of the rollout: the first day of the aggregates (R6)
	var out *time.Time
	if from != nil {
		d := Today(*from)
		out = &d
	}
	return out, updated, nil
}

// goalRow is the last row of catalog_usage_goal_daily with the manual check.
type goalRow struct {
	Day                time.Time
	Dead, Total        int
	Share              *float64
	Baseline           *float64
	Delta              *float64
	DroppedTotal       int
	DroppedWithCalls   int
	VerifiedManuallyAt *string
}

func (s *Service) goal(ctx context.Context) (*goalRow, error) {
	var g goalRow
	var share, baseline, delta *float64
	err := s.Pool.QueryRow(ctx, `SELECT day, dead, total, share::float8, baseline::float8, delta::float8,
		dropped_total, dropped_with_calls FROM catalog_usage_goal_daily ORDER BY day DESC LIMIT 1`).
		Scan(&g.Day, &g.Dead, &g.Total, &share, &baseline, &delta, &g.DroppedTotal, &g.DroppedWithCalls)
	if err != nil && err != pgx.ErrNoRows {
		return nil, err
	}
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	g.Share, g.Baseline, g.Delta = share, baseline, delta
	var v *string
	_ = s.Pool.QueryRow(ctx, `SELECT value->>'verifiedManuallyAt' FROM settings WHERE key = 'catalog_usage_goal'`).Scan(&v)
	g.VerifiedManuallyAt = v
	return &g, nil
}

// Meta builds the envelope of the report (tech §«API»).
func (s *Service) Meta(ctx context.Context) (Meta, error) {
	from, updated, err := s.stateMeta(ctx)
	if err != nil {
		return Meta{}, err
	}
	m := Meta{Windows: s.Cfg.Windows(), Thresholds: s.Cfg.Thresholds()}
	if from != nil {
		m.DataFrom = from.Format("2006-01-02")
		m.Ready = Readiness(*from, s.clock(), s.Cfg)
	}
	m.UpdatedAt = updated
	g, err := s.goal(ctx)
	if err != nil {
		return Meta{}, err
	}
	if g != nil {
		m.GoalMetric = GoalMetric{Value: g.Share, Baseline: g.Baseline, Delta: g.Delta, VerifiedManuallyAt: g.VerifiedManuallyAt}
	}
	return m, nil
}

// itemInfo is the catalog part of a row.
type itemInfo struct {
	ID                         uuid.UUID
	Name, Title, Type          string
	Mode                       *string
	Published, InDev           bool
	Status                     *string
	PublishedAt, UnpublishedAt *time.Time
	CheckedAt, LastCheckAt     *time.Time
	Allowed, Tools             []byte
	DeletedAt                  *time.Time
	TombName, TombTitle        *string
	TombType                   *string
	ExplicitGrant, Pinned      bool
}

func (s *Service) items(ctx context.Context) (map[uuid.UUID]*itemInfo, error) {
	rows, err := s.Pool.Query(ctx, `SELECT c.id, c.name, coalesce(c.title,''), c.type, c.mode, c.published, c.in_development, c.status,
			c.published_at, c.unpublished_at, c.checked_at, c.last_check_at,
			coalesce(c.allowed_tools, 'null'::jsonb), coalesce(c.tools, 'null'::jsonb),
			EXISTS (SELECT 1 FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
				WHERE ag.kind = 'catalog_item' AND ag.item_id = c.id AND g.builtin IS DISTINCT FROM 'admins'),
			EXISTS (SELECT 1 FROM catalog_item_pins p WHERE p.item_id = c.id AND p.released_at IS NULL AND p.review_at > now())
		FROM catalog_items c`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]*itemInfo{}
	for rows.Next() {
		var it itemInfo
		if err := rows.Scan(&it.ID, &it.Name, &it.Title, &it.Type, &it.Mode, &it.Published, &it.InDev, &it.Status,
			&it.PublishedAt, &it.UnpublishedAt, &it.CheckedAt, &it.LastCheckAt, &it.Allowed, &it.Tools,
			&it.ExplicitGrant, &it.Pinned); err != nil {
			return nil, err
		}
		out[it.ID] = &it
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// R23: a deleted item has no catalog row; its tombstone keeps the name and the date
	tombs, err := s.Pool.Query(ctx, `SELECT t.item_id, t.name, t.title, t.type, t.deleted_at FROM catalog_item_tombstones t
		WHERE NOT EXISTS (SELECT 1 FROM catalog_items c WHERE c.id = t.item_id)`)
	if err != nil {
		return nil, err
	}
	defer tombs.Close()
	for tombs.Next() {
		var it itemInfo
		var name, title, typ string
		if err := tombs.Scan(&it.ID, &name, &title, &typ, &it.DeletedAt); err != nil {
			return nil, err
		}
		it.Name, it.Title, it.Type = name, title, typ
		it.TombName, it.TombTitle, it.TombType = &name, &title, &typ
		out[it.ID] = &it
	}
	return out, tombs.Err()
}

// dayAgg is an aggregated row of the item level.
type dayAgg struct {
	ItemID                    uuid.UUID
	Name, Title, Type         string
	Mode                      *string
	Calls, OK, Admin, Service int64
	Users, Sessions           int64
	AvailableDays             int
	Errors                    map[string]int64
	Latency                   map[string]int64
	LastDay                   *time.Time
}

// aggregate reads the item-level rows of a period from the daily view, or
// from the monthly view for long periods (R4).
func (s *Service) aggregate(ctx context.Context, f Filter) (map[uuid.UUID]*dayAgg, error) {
	src := "catalog_usage_item_daily"
	col := "day"
	avail := "count(DISTINCT day) FILTER (WHERE available)::int"
	switch f.Step {
	case "month":
		src, col, avail = "catalog_usage_item_monthly", "month", "coalesce(sum(available_days), 0)::int"
	}
	args := []any{f.From, f.To}
	where := []string{col + " >= $1 AND " + col + " <= $2"}
	if f.CallerKind != "" {
		args = append(args, f.CallerKind)
		where = append(where, fmt.Sprintf("caller_kind = $%d", len(args)))
	}
	if f.Channel != "" {
		args = append(args, f.Channel)
		where = append(where, fmt.Sprintf("channel = $%d", len(args)))
	}
	if f.Mode != "" {
		args = append(args, f.Mode)
		where = append(where, fmt.Sprintf("mode = $%d", len(args)))
	}
	if f.ItemID != uuid.Nil {
		args = append(args, f.ItemID)
		where = append(where, fmt.Sprintf("item_id = $%d", len(args)))
	}
	if len(f.Groups) > 0 {
		args = append(args, f.Groups)
		where = append(where, fmt.Sprintf(`item_id IN (SELECT item_id FROM catalog_usage_daily
			WHERE group_id = ANY($%d) AND day >= $1 AND day <= $2)`, len(args)))
	}
	q := `WITH rows AS (SELECT * FROM ` + src + ` WHERE ` + strings.Join(where, " AND ") + `),
	agg AS (
		SELECT item_id, max(item_name) AS item_name, max(item_title) AS item_title, max(item_type) AS item_type,
			max(mode) AS mode, sum(calls)::bigint AS calls, sum(ok)::bigint AS ok,
			sum(admin_calls)::bigint AS admin_calls, sum(service_calls)::bigint AS service_calls,
			sum(users)::bigint AS users, sum(sessions)::bigint AS sessions,
			` + avail + ` AS available_days,
			max(` + col + `) FILTER (WHERE calls > 0) AS last_day
		FROM rows GROUP BY item_id),
	errs AS (
		SELECT item_id, jsonb_object_agg(code, n) AS errors FROM (
			SELECT r.item_id, e.key AS code, sum(e.value::bigint) AS n
			FROM rows r CROSS JOIN LATERAL jsonb_each_text(r.errors_by_code) e
			GROUP BY r.item_id, e.key) x GROUP BY item_id),
	lat AS (
		SELECT item_id, jsonb_object_agg(bound, n) AS buckets FROM (
			SELECT r.item_id, e.key AS bound, sum(e.value::bigint) AS n
			FROM rows r CROSS JOIN LATERAL jsonb_each_text(r.latency_buckets) e
			GROUP BY r.item_id, e.key) x GROUP BY item_id)
	SELECT a.item_id, a.item_name, a.item_title, a.item_type, a.mode, a.calls, a.ok, a.admin_calls, a.service_calls,
		a.users, a.sessions, a.available_days, coalesce(errs.errors, '{}'::jsonb), coalesce(lat.buckets, '{}'::jsonb), a.last_day
	FROM agg a LEFT JOIN errs ON errs.item_id = a.item_id LEFT JOIN lat ON lat.item_id = a.item_id`
	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]*dayAgg{}
	for rows.Next() {
		var a dayAgg
		var errorsRaw, latencyRaw []byte
		if err := rows.Scan(&a.ItemID, &a.Name, &a.Title, &a.Type, &a.Mode, &a.Calls, &a.OK, &a.Admin, &a.Service,
			&a.Users, &a.Sessions, &a.AvailableDays, &errorsRaw, &latencyRaw, &a.LastDay); err != nil {
			return nil, err
		}
		a.Errors = map[string]int64{}
		MergeErrors(a.Errors, errorsRaw)
		a.Latency = map[string]int64{}
		MergeLatency(a.Latency, latencyRaw)
		out[a.ItemID] = &a
	}
	return out, rows.Err()
}

// reference reads the reference slice: calls over the long window and the
// last day with calls (R11).
func (s *Service) reference(ctx context.Context, to time.Time) (map[uuid.UUID]*dayAgg, error) {
	from := to.AddDate(0, 0, -s.Cfg.ReferenceDays+1)
	return s.aggregate(ctx, Filter{From: from, To: to})
}

func (s *Service) rejectedConfirmations(ctx context.Context, from, to time.Time) (map[uuid.UUID]int64, error) {
	rows, err := s.Pool.Query(ctx, `SELECT coalesce(p.item_id, c.id) AS item, count(*)::bigint
		FROM pending_confirmations p JOIN catalog_items c ON c.id = p.item_id OR (p.item_id IS NULL AND c.name = p.server)
		WHERE p.status = 'rejected' AND p.created_at >= $1 AND p.created_at < $2
		GROUP BY coalesce(p.item_id, c.id)`, from, to.AddDate(0, 0, 1))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]int64{}
	for rows.Next() {
		var id uuid.UUID
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

// groupsOf returns the group slice of the given items (R18).
func (s *Service) groupsOf(ctx context.Context, f Filter, ids []uuid.UUID) (map[uuid.UUID][]GroupRef, error) {
	out := map[uuid.UUID][]GroupRef{}
	if len(ids) == 0 {
		return out, nil
	}
	args := []any{ids, f.From, f.To}
	where := []string{"item_id = ANY($1)", "day >= $2", "day <= $3", "group_id <> '" + ZeroUUID + "'::uuid"}
	if f.CallerKind != "" {
		args = append(args, f.CallerKind)
		where = append(where, fmt.Sprintf("caller_kind = $%d", len(args)))
	}
	if f.Channel != "" {
		args = append(args, f.Channel)
		where = append(where, fmt.Sprintf("channel = $%d", len(args)))
	}
	if len(f.Groups) > 0 {
		args = append(args, f.Groups)
		where = append(where, fmt.Sprintf("group_id = ANY($%d)", len(args)))
	}
	rows, err := s.Pool.Query(ctx, `SELECT item_id, group_id, max(group_name), max(group_builtin), sum(calls)::bigint
		FROM catalog_usage_daily WHERE `+strings.Join(where, " AND ")+`
		GROUP BY item_id, group_id ORDER BY item_id, sum(calls) DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item uuid.UUID
		var g GroupRef
		if err := rows.Scan(&item, &g.GroupID, &g.GroupName, &g.Builtin, &g.Calls); err != nil {
			return nil, err
		}
		out[item] = append(out[item], g)
	}
	return out, rows.Err()
}

// hintWindow returns the calls of the hint window (R8) and whether the
// window is complete: the position was available for the whole window.
func (s *Service) hintWindow(ctx context.Context, now time.Time) (map[uuid.UUID]*dayAgg, error) {
	from := Today(now).AddDate(0, 0, -s.Cfg.UnusedDays+1)
	return s.aggregate(ctx, Filter{From: from, To: Today(now)})
}

// Report is GET /catalog/usage: the flat table «item × period» (R17–R23).
func (s *Service) Report(ctx context.Context, f Filter) ([]UsageRow, *string, Meta, error) {
	meta, err := s.Meta(ctx)
	if err != nil {
		return nil, nil, Meta{}, err
	}
	if f.Limit <= 0 {
		f.Limit = 50
	}
	dataFrom, err := ParseDay(meta.DataFrom)
	if err != nil {
		return nil, nil, Meta{}, err
	}
	f.From, f.To, meta.WindowClamped = ClampPeriod(f.From, f.To, dataFrom)
	period, err := s.aggregate(ctx, Filter{From: f.From, To: f.To, Step: f.Step, Groups: f.Groups, CallerKind: f.CallerKind,
		Channel: f.Channel, Mode: f.Mode})
	if err != nil {
		return nil, nil, Meta{}, err
	}
	ref, err := s.reference(ctx, f.To)
	if err != nil {
		return nil, nil, Meta{}, err
	}
	hint, err := s.hintWindow(ctx, s.clock())
	if err != nil {
		return nil, nil, Meta{}, err
	}
	items, err := s.items(ctx)
	if err != nil {
		return nil, nil, Meta{}, err
	}
	rejected, err := s.rejectedConfirmations(ctx, f.From, f.To)
	if err != nil {
		return nil, nil, Meta{}, err
	}
	// R2, R8: the table shows every published item, including the one without a single
	// call in the period — a zero row of the availability days is the same thing.
	for id, it := range items {
		if _, ok := period[id]; ok {
			continue
		}
		if !it.Published || it.InDev || it.DeletedAt != nil {
			continue
		}
		period[id] = &dayAgg{ItemID: id, Name: it.Name, Title: it.Title, Type: it.Type, Mode: it.Mode,
			Errors: map[string]int64{}, Latency: map[string]int64{}}
	}
	ids := make([]uuid.UUID, 0, len(period))
	for id := range period {
		ids = append(ids, id)
	}
	groups, err := s.groupsOf(ctx, f, ids)
	if err != nil {
		return nil, nil, Meta{}, err
	}
	now := s.clock()
	out := make([]UsageRow, 0, len(period))
	for id, a := range period {
		it := items[id]
		if it != nil && it.DeletedAt == nil && !it.Published && !f.IncludeUnpublished && id != Zero {
			continue
		}
		if it != nil && it.InDev && !f.IncludeUnpublished {
			continue
		}
		row := UsageRow{ItemID: id, ItemName: a.Name, ItemType: a.Type, Mode: a.Mode,
			Calls: a.Calls, AdminCalls: a.Admin, ServiceCalls: a.Service, Users: a.Users, Sessions: a.Sessions,
			OK: a.OK, ErrorsByCode: a.Errors, AvailableDays: a.AvailableDays, Groups: groups[id]}
		if row.ErrorsByCode == nil {
			row.ErrorsByCode = map[string]int64{}
		}
		if row.Groups == nil {
			row.Groups = []GroupRef{}
		}
		if a.Title != "" {
			row.ItemName = a.Title
		}
		row.AuthCalls = a.Errors["auth"]
		row.DeniedCalls = a.Errors["denied"]
		row.P50Ms, row.P95Ms = Percentiles(a.Latency)
		if n, ok := rejected[id]; ok {
			row.ConfirmationsRejected = &n
		}
		if r := ref[id]; r != nil {
			row.Calls90 = r.Calls
			row.LastCallAt = r.LastDay
		}
		// the hint counts the days of availability of its own window (R8, R13);
		// the column of the table keeps the days of the requested period
		in := HintInput{ItemType: a.Type, Now: now, Cfg: s.Cfg, ReadyHints: meta.Ready.Hints, ReadyQuick: meta.Ready.Quick,
			AvailableDays: row.AvailableDays, Calls90: row.Calls90, ServiceCalls: a.Service, AuthCalls: row.AuthCalls}
		if it != nil {
			row.Published = it.Published && !it.InDev
			row.PublishedAt, row.UnpublishedAt = it.PublishedAt, it.UnpublishedAt
			row.DeletedAt = it.DeletedAt
			row.ToolCount = ToolsOf(it.Allowed, it.Tools)
			tomb := ""
			if it.TombTitle != nil {
				tomb = *it.TombTitle
			}
			// R4, R18: the name is the snapshot of the day; the catalog is the fallback
			row.ItemName = firstNonEmpty(a.Title, a.Name, tomb, it.Title, it.Name)
			if it.TombType != nil {
				row.ItemType = *it.TombType
			} else {
				row.ItemType = it.Type
			}
			in.ItemType, in.Published, in.InDev, in.Deleted = row.ItemType, row.Published, it.InDev, it.DeletedAt != nil
			in.ExplicitGrant, in.Pinned, in.PublishedAt = it.ExplicitGrant, it.Pinned, it.PublishedAt
			if it.Status != nil {
				in.Status = *it.Status
			}
		} else {
			// a row of calls outside the catalog: no groups, outside the tops (R23)
			row.Published = false
			in.Published = false
		}
		in.HumanCalls = a.Calls - a.Service - a.Admin
		in.HumanUsers = humanUsers(a.Users, a.Admin)
		if h := hint[id]; h != nil {
			in.AvailableDays = h.AvailableDays
		}
		in.ActualCalls = a.Calls - row.AuthCalls - row.DeniedCalls
		in.ActualOK = a.OK
		row.HumanUsers = in.HumanUsers
		row.ExplicitGrant = in.ExplicitGrant
		if in.ActualCalls > 0 {
			row.ErrShare = float64(in.ActualCalls-in.ActualOK) / float64(in.ActualCalls)
		}
		row.State, row.Hints = Evaluate(in)
		if f.Hint != "" && !contains(row.Hints, f.Hint) {
			continue
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		wi, wj := hintWeight(out[i]), hintWeight(out[j])
		if wi != wj {
			return wi < wj
		}
		if out[i].ErrShare != out[j].ErrShare {
			return out[i].ErrShare > out[j].ErrShare
		}
		return out[i].ItemName < out[j].ItemName
	})
	start := 0
	if f.Cursor != nil {
		start, _ = strconv.Atoi(f.Cursor.ID)
	}
	if start > len(out) {
		start = len(out)
	}
	end := start + f.Limit
	if end > len(out) {
		end = len(out)
	}
	var next *string
	if end < len(out) {
		c := httpx.EncodeCursor(time.Time{}, strconv.Itoa(end))
		next = &c
	}
	return out[start:end], next, meta, nil
}

// humanUsers is the number of people of a day: the administrators are not
// counted, and a day with administrator calls has them among the users.
func humanUsers(users, adminCalls int64) int64 {
	if adminCalls > 0 && users > 1 {
		return users - 1
	}
	if adminCalls > 0 && users == 1 {
		return 0
	}
	return users
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func hintWeight(r UsageRow) int {
	switch r.State {
	case StateDormant:
		return 0
	case StateUnused:
		return 1
	case StateErrorsOnly:
		return 2
	case StateSingleUser:
		return 3
	case StatePinned, StateService:
		return 4
	case StateUnavail, StateYoung, StateNotGranted, StateUnpublish:
		return 5
	}
	return 6
}

// Tools is the drill-down by tools: the only request of api to the raw audit,
// the window ≤ CATALOG_USAGE_DRILLDOWN_DAYS (R21). The index audit_item_at
// serves it; the statement times out in three seconds.
func (s *Service) Tools(ctx context.Context, id uuid.UUID, from, to time.Time) ([]ToolRow, Meta, error) {
	meta, err := s.Meta(ctx)
	if err != nil {
		return nil, Meta{}, err
	}
	var exists bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM catalog_items WHERE id = $1)
		OR EXISTS (SELECT 1 FROM catalog_item_tombstones WHERE item_id = $1)`, id).Scan(&exists); err != nil {
		return nil, meta, err
	}
	if !exists {
		return nil, meta, ErrItemNotFound
	}
	from, to, meta.WindowClamped = DrilldownWindow(from, to, s.Cfg.DrilldownDays)
	out := []ToolRow{}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '3s'`); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT tool, count(*)::bigint AS calls,
				count(*) FILTER (WHERE result_code = 'ok')::bigint AS ok,
				count(*) FILTER (WHERE result_code <> 'ok')::bigint AS errors,
				count(*) FILTER (WHERE result_code = 'auth')::bigint AS auth_calls,
				percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS p50,
				percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95
			FROM audit
			WHERE item_id = $1 AND at >= $2 AND at < $3
				AND result_code IS NOT NULL AND duration_ms IS NOT NULL
			GROUP BY tool ORDER BY calls DESC LIMIT 20`, id, from, to.AddDate(0, 0, 1))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r ToolRow
			var p50, p95 *float64
			if err := rows.Scan(&r.Tool, &r.Calls, &r.OK, &r.Errors, &r.AuthCalls, &p50, &p95); err != nil {
				return err
			}
			r.P50Ms, r.P95Ms = roundPtr(p50), roundPtr(p95)
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, meta, err
}

func roundPtr(v *float64) *int64 {
	if v == nil {
		return nil
	}
	n := int64(*v + 0.5)
	return &n
}

// ErrItemNotFound is 404 item_not_found of the contract.
var ErrItemNotFound = apperr.NotFound("item_not_found", "the item is not found")

func badPeriod(msg string) error { return apperr.Unprocessable("invalid_period", msg) }

// PinList is GET /catalog/usage/pins (R12).
func (s *Service) PinList(ctx context.Context, overdue bool, limit int, cursor *httpx.Cursor) ([]Pin, *string, error) {
	if limit <= 0 {
		limit = 50
	}
	where := "p.released_at IS NULL"
	if overdue {
		where += " AND p.review_at <= now()"
	}
	rows, err := s.Pool.Query(ctx, `SELECT p.id, p.item_id, p.reason, p.pinned_by, p.pinned_at, p.review_at, p.released_at,
			(p.released_at IS NULL AND p.review_at > now()), coalesce(c.title, c.name, c.name, '')
		FROM catalog_item_pins p LEFT JOIN catalog_items c ON c.id = p.item_id
		WHERE `+where+` ORDER BY p.review_at, p.id LIMIT $1 OFFSET $2`, limit+1, offset(cursor))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	out := []Pin{}
	for rows.Next() {
		var p Pin
		if err := rows.Scan(&p.ID, &p.ItemID, &p.Reason, &p.PinnedBy, &p.PinnedAt, &p.ReviewAt, &p.ReleasedAt, &p.Effective, &p.ItemName); err != nil {
			return nil, nil, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	var next *string
	if len(out) > limit {
		out = out[:limit]
		c := httpx.EncodeCursor(time.Time{}, strconv.Itoa(offset(cursor)+limit))
		next = &c
	}
	return out, next, nil
}

func offset(c *httpx.Cursor) int {
	if c == nil {
		return 0
	}
	n, _ := strconv.Atoi(c.ID)
	return n
}

// jsonInts decodes a JSONB counter map.
func jsonInts(raw []byte) map[string]int64 {
	out := map[string]int64{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return out
}
