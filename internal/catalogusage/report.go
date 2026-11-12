package catalogusage

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// OverlapRef is a help about the names of tools two items share (R22): no
// recommendation "this is a duplicate".
type OverlapRef struct {
	ItemID   uuid.UUID `json:"itemId"`
	ItemName string    `json:"itemName"`
	Tools    []string  `json:"tools"`
}

// Item is GET /catalog/usage/items/{id}: the card of an item (R11–R15, R21–R24).
func (s *Service) Item(ctx context.Context, id uuid.UUID, from, to time.Time) (ItemCard, []GroupSlice, []Interval, []OverlapRef, []Pin, []string, Meta, error) {
	meta, err := s.Meta(ctx)
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	dataFrom, _ := ParseDay(meta.DataFrom)
	from, to, _ = ClampPeriod(from, to, dataFrom)
	items, err := s.items(ctx)
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	it := items[id]
	if it == nil {
		return ItemCard{}, nil, nil, nil, nil, nil, meta, ErrItemNotFound
	}
	period, err := s.aggregate(ctx, Filter{From: from, To: to, ItemID: id})
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	ref, err := s.reference(ctx, to)
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	hint, err := s.hintWindow(ctx, s.clock())
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	card := ItemCard{ItemID: id, ItemType: it.Type, Mode: it.Mode, Published: it.Published && !it.InDev,
		PublishedAt: it.PublishedAt, UnpublishedAt: it.UnpublishedAt, LastCheckAt: it.LastCheckAt, LastOkAt: it.CheckedAt,
		Status: it.Status, ToolCount: ToolsOf(it.Allowed, it.Tools), ErrorsByCode: map[string]int64{}, UnresolvedTasks: []UnresolvedTask{}}
	card.ItemName = firstNonEmpty(it.Title, it.Name)
	if it.TombTitle != nil {
		card.ItemName = firstNonEmpty(*it.TombTitle, it.Name)
	}
	if it.TombType != nil {
		card.ItemType = *it.TombType
	}
	in := HintInput{ItemType: card.ItemType, Published: card.Published, InDev: it.InDev, Deleted: it.DeletedAt != nil,
		Pinned: it.Pinned, ExplicitGrant: it.ExplicitGrant, PublishedAt: it.PublishedAt, Now: s.clock(), Cfg: s.Cfg,
		ReadyHints: meta.Ready.Hints, ReadyQuick: meta.Ready.Quick}
	if it.Status != nil {
		in.Status = *it.Status
	}
	if a := period[id]; a != nil {
		card.Calls, card.AdminCalls, card.ServiceCalls = a.Calls, a.Admin, a.Service
		card.Users, card.Sessions, card.OK = a.Users, a.Sessions, a.OK
		card.ErrorsByCode = a.Errors
		card.P50Ms, card.P95Ms = Percentiles(a.Latency)
		card.AvailableDays = a.AvailableDays
		in.AuthCalls = a.Errors["auth"]
		in.ActualCalls = a.Calls - a.Errors["auth"] - a.Errors["denied"]
		in.ActualOK = a.OK
		in.HumanCalls = a.Calls - a.Service - a.Admin
		in.HumanUsers = humanUsers(a.Users, a.Admin)
	}
	if r := ref[id]; r != nil {
		card.Calls90, card.LastCallAt = r.Calls, r.LastDay
		in.Calls90 = r.Calls
	}
	// the hint counts the days of its own window; the card keeps the days of the period
	if h := hint[id]; h != nil {
		in.AvailableDays = h.AvailableDays
	}
	state, hints := Evaluate(in)
	card.States, card.Hints = []string{state}, hints
	if card.ItemType == "skill" {
		n := card.Calls
		card.SkillActivations = &n
		sessions := card.Sessions
		card.SkillSessions = &sessions
	}
	groups, err := s.groupSlices(ctx, from, to, id)
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	pubs, err := s.intervals(ctx, id, from, to)
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	card.PublishedEstimated = len(pubs) > 0 && pubs[len(pubs)-1].Estimated
	overlap, err := s.overlap(ctx, it, items)
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	pins, _, err := s.PinList(ctx, false, 200, nil)
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	mine := pins[:0]
	for _, p := range pins {
		if p.ItemID == id {
			mine = append(mine, p)
		}
	}
	tasks, err := s.unresolvedTasks(ctx, id)
	if err != nil {
		return ItemCard{}, nil, nil, nil, nil, nil, Meta{}, err
	}
	card.UnresolvedTasks = tasks
	return card, groups, pubs, overlap, mine, card.States, meta, nil
}

// State is the first state of the card.
func (c ItemCard) State() string {
	if len(c.States) > 0 {
		return c.States[0]
	}
	return StateActive
}

func (s *Service) groupSlices(ctx context.Context, from, to time.Time, id uuid.UUID) ([]GroupSlice, error) {
	rows, err := s.Pool.Query(ctx, `SELECT group_id, max(group_name), max(group_builtin), sum(calls)::bigint, sum(users)::bigint, sum(ok)::bigint,
			(SELECT jsonb_object_agg(code, n) FROM (
				SELECT e.key AS code, sum(e.value::bigint) AS n
				FROM catalog_usage_daily d2 CROSS JOIN LATERAL jsonb_each_text(d2.errors_by_code) e
				WHERE d2.item_id = $1 AND d2.group_id = d.group_id AND d2.day >= $2 AND d2.day <= $3 AND d2.group_id <> '`+ZeroUUID+`'::uuid
				GROUP BY e.key) x)
		FROM catalog_usage_daily d
		WHERE item_id = $1 AND day >= $2 AND day <= $3 AND group_id <> '`+ZeroUUID+`'::uuid
		GROUP BY group_id ORDER BY sum(calls) DESC`, id, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []GroupSlice{}
	for rows.Next() {
		var g GroupSlice
		var raw []byte
		if err := rows.Scan(&g.GroupID, &g.GroupName, &g.GroupBuiltin, &g.Calls, &g.Users, &g.OK, &raw); err != nil {
			return nil, err
		}
		g.ErrorsByCode = jsonInts(raw)
		out = append(out, g)
	}
	return out, rows.Err()
}

// intervals reads the publication intervals of an item with the days of
// availability and removal of each (R13).
func (s *Service) intervals(ctx context.Context, id uuid.UUID, from, to time.Time) ([]Interval, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, published_at, unpublished_at, estimated,
			(SELECT count(*)::int FROM generate_series($2::date, $3::date, interval '1 day') d(day)
				WHERE d.day::date >= p.published_at::date
				  AND (p.unpublished_at IS NULL OR d.day::date < p.unpublished_at::date)) AS available_days,
			(SELECT count(*)::int FROM generate_series($2::date, $3::date, interval '1 day') d(day)
				WHERE NOT (d.day::date >= p.published_at::date
				  AND (p.unpublished_at IS NULL OR d.day::date < p.unpublished_at::date))) AS removed_days
		FROM catalog_item_publications p WHERE item_id = $1 ORDER BY published_at`, id, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Interval{}
	for rows.Next() {
		var v Interval
		if err := rows.Scan(&v.ID, &v.PublishedAt, &v.UnpublishedAt, &v.Estimated, &v.AvailableDays, &v.RemovedDays); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// overlap finds the items that share names of tools with the item (R22).
func (s *Service) overlap(ctx context.Context, it *itemInfo, all map[uuid.UUID]*itemInfo) ([]OverlapRef, error) {
	out := []OverlapRef{}
	if it.Type != "mcp" {
		return out, nil
	}
	names := toolNames(it.Allowed, it.Tools)
	if len(names) == 0 {
		return out, nil
	}
	have := map[string]bool{}
	for _, n := range names {
		have[n] = true
	}
	for id, other := range all {
		if id == it.ID || other.Type != "mcp" || other.DeletedAt != nil {
			continue
		}
		shared := []string{}
		for _, n := range toolNames(other.Allowed, other.Tools) {
			if have[n] {
				shared = append(shared, n)
			}
		}
		if len(shared) > 0 {
			out = append(out, OverlapRef{ItemID: id, ItemName: firstNonEmpty(other.Title, other.Name), Tools: shared})
		}
	}
	return out, nil
}

func toolNames(allowed, tools []byte) []string {
	var names []string
	for _, raw := range [][]byte{allowed, tools} {
		if len(raw) == 0 {
			continue
		}
		if err := json.Unmarshal(raw, &names); err == nil && len(names) > 0 {
			return names
		}
	}
	return nil
}

// unresolvedTasks lists the tasks paused with source_access for the users
// that have the item (R29).
func (s *Service) unresolvedTasks(ctx context.Context, id uuid.UUID) ([]UnresolvedTask, error) {
	rows, err := s.Pool.Query(ctx, `SELECT t.id, t.title, coalesce(t.pause_kind, '')
		FROM scheduled_tasks t
		WHERE t.status = 'paused' AND t.pause_kind = 'source_access' AND (
			t.user_id IN (SELECT u.id FROM users u WHERE u.is_admin OR EXISTS (
				SELECT 1 FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
				WHERE ag.kind = 'catalog_item' AND ag.item_id = $1
				  AND (g.builtin = 'all' OR EXISTS (SELECT 1 FROM user_group_members m WHERE m.group_id = g.id AND m.user_id = u.id)))))
		ORDER BY t.created_at LIMIT 100`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UnresolvedTask{}
	for rows.Next() {
		var t UnresolvedTask
		if err := rows.Scan(&t.TaskID, &t.Title, &t.PauseKind); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Summary is GET /catalog/usage/summary: the tiles (R16, R14).
func (s *Service) Summary(ctx context.Context, from, to time.Time, includeUnpublished bool) (Tiles, Meta, error) {
	meta, err := s.Meta(ctx)
	if err != nil {
		return Tiles{}, Meta{}, err
	}
	dataFrom, _ := ParseDay(meta.DataFrom)
	from, to, _ = ClampPeriod(from, to, dataFrom)
	rows, _, _, err := s.rowsWithMeta(ctx, Filter{From: from, To: to, IncludeUnpublished: includeUnpublished, Limit: 1000})
	if err != nil {
		return Tiles{}, Meta{}, err
	}
	tiles := Tiles{TopCalls: []TopItem{}, TopErrors: []TopError{}, PublishedToolCount: ToolCount{Trend: []TrendVal{}},
		PlatformSuccess: Success{Trend: []TrendVal{}}, ResolvedCandidates: Resolved{Target: 0.3, Note: "не цель"}}
	for _, r := range rows {
		if r.ItemID == Zero {
			continue // rows «outside the catalog» stay outside the tops (R23)
		}
		if r.Hints != nil && contains(r.Hints, HintUnused) {
			tiles.UnusedCount++
		}
		if r.State == StatePinned && r.Calls == 0 {
			tiles.PinnedUnused++
		}
	}
	// the goal tile reads the precomputed row only (A16)
	g, err := s.goal(ctx)
	if err != nil {
		return Tiles{}, Meta{}, err
	}
	verified := meta.GoalMetric.VerifiedManuallyAt
	// R6, RDY-02: the tile is null until the goal window is collected
	if meta.Ready.Goal && g != nil {
		tiles.DeadShare = &DeadShare{Value: g.Share, Dead: g.Dead, Total: g.Total, Baseline: g.Baseline, Delta: g.Delta,
			VerifiedManuallyAt: verified, DroppedOut: Dropped{Total: g.DroppedTotal, WithCalls: g.DroppedWithCalls}}
	}
	// tools of the published items and their trend by the days of the period
	tools, trend, err := s.toolTrend(ctx, from, to)
	if err != nil {
		return Tiles{}, Meta{}, err
	}
	tiles.PublishedToolCount = ToolCount{Value: tools, Trend: trend}
	// the platform success: actual calls of the item-level rows, including admin calls
	// and the rows «outside the catalog»; auth and denied are outside the denominator (R16)
	var calls, ok, auth, denied int64
	for _, r := range rows {
		calls += r.Calls
		ok += r.OK
		auth += r.AuthCalls
		denied += r.DeniedCalls
	}
	tiles.PlatformSuccess = Success{Value: SuccessShare(ok, calls, auth, denied), Denied: denied, Auth: auth}
	successTrend, err := s.successTrend(ctx, from, to)
	if err != nil {
		return Tiles{}, Meta{}, err
	}
	tiles.PlatformSuccess.Trend = successTrend
	tiles.TopCalls = topCalls(rows)
	tiles.TopErrors = topErrors(rows)
	resolved, candidates, err := s.resolvedCandidates(ctx, from, to)
	if err != nil {
		return Tiles{}, Meta{}, err
	}
	tiles.ResolvedCandidates.Resolved, tiles.ResolvedCandidates.Candidates = resolved, candidates
	if candidates > 0 {
		tiles.ResolvedCandidates.Value = float64(resolved) / float64(candidates)
	}
	return tiles, meta, nil
}

func topCalls(rows []UsageRow) []TopItem {
	list := make([]TopItem, 0, len(rows))
	for _, r := range rows {
		if r.ItemID == Zero || r.Calls <= 0 || r.DeletedAt != nil { // R23: a deleted item is outside the tops
			continue
		}
		list = append(list, TopItem{ItemID: r.ItemID, ItemName: r.ItemName, Calls: r.Calls})
	}
	sortTop(list)
	out := []TopItem{}
	for i, v := range list {
		if i >= 10 {
			break
		}
		out = append(out, v)
	}
	return out
}

func sortTop(list []TopItem) {
	for i := 1; i < len(list); i++ {
		for j := i; j > 0 && list[j].Calls > list[j-1].Calls; j-- {
			list[j], list[j-1] = list[j-1], list[j]
		}
	}
}

func topErrors(rows []UsageRow) []TopError {
	type key struct {
		id   uuid.UUID
		code string
	}
	agg := map[key]*TopError{}
	for _, r := range rows {
		if r.ItemID == Zero || r.DeletedAt != nil {
			continue
		}
		for code, n := range r.ErrorsByCode {
			if code == "ok" || n <= 0 {
				continue
			}
			k := key{r.ItemID, code}
			if agg[k] == nil {
				agg[k] = &TopError{ItemID: r.ItemID, ItemName: r.ItemName, Code: code}
			}
			agg[k].Calls += n
		}
	}
	list := make([]TopError, 0, len(agg))
	for _, v := range agg {
		list = append(list, *v)
	}
	for i := 1; i < len(list); i++ {
		for j := i; j > 0 && list[j].Calls > list[j-1].Calls; j-- {
			list[j], list[j-1] = list[j-1], list[j]
		}
	}
	if len(list) > 10 {
		list = list[:10]
	}
	return list
}

// toolTrend returns the number of tools of the published items now and their
// number by the days of the period from the publication intervals (R16).
func (s *Service) toolTrend(ctx context.Context, from, to time.Time) (int, []TrendVal, error) {
	rows, err := s.Pool.Query(ctx, `SELECT d.day::date,
			coalesce(sum(coalesce(jsonb_array_length(c.allowed_tools), jsonb_array_length(c.tools), 0))::bigint, 0)
		FROM generate_series($1::date, $2::date, interval '1 day') d(day)
		LEFT JOIN catalog_item_publications p ON d.day::date >= p.published_at::date
			AND (p.unpublished_at IS NULL OR d.day::date < p.unpublished_at::date)
		LEFT JOIN catalog_items c ON c.id = p.item_id AND c.type = 'mcp' AND c.published AND NOT c.in_development
		GROUP BY d.day ORDER BY d.day`, from, to)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	out := []TrendVal{}
	last := 0
	for rows.Next() {
		var day time.Time
		var n int
		if err := rows.Scan(&day, &n); err != nil {
			return 0, nil, err
		}
		last = n
		out = append(out, TrendVal{Day: day.Format("2006-01-02"), Value: float64(n)})
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return last, out, nil
}

func (s *Service) successTrend(ctx context.Context, from, to time.Time) ([]TrendVal, error) {
	rows, err := s.Pool.Query(ctx, `SELECT day,
			coalesce(sum(ok), 0)::bigint,
			coalesce(sum(calls), 0)::bigint,
			coalesce(sum((errors_by_code->>'auth')::bigint), 0),
			coalesce(sum((errors_by_code->>'denied')::bigint), 0)
		FROM catalog_usage_item_daily WHERE day >= $1 AND day <= $2 GROUP BY day ORDER BY day`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TrendVal{}
	for rows.Next() {
		var day time.Time
		var ok, calls, auth, denied int64
		if err := rows.Scan(&day, &ok, &calls, &auth, &denied); err != nil {
			return nil, err
		}
		if v := SuccessShare(ok, calls, auth, denied); v != nil {
			out = append(out, TrendVal{Day: day.Format("2006-01-02"), Value: *v})
		}
	}
	return out, rows.Err()
}

// resolvedCandidates counts the candidates and those of them that got an
// action in the last 30 days (R16): admin_log and pins.
func (s *Service) resolvedCandidates(ctx context.Context, from, to time.Time) (int, int, error) {
	list, _, err := s.Candidates(ctx, Filter{From: from, To: to}, "", 1000, nil)
	if err != nil {
		return 0, 0, err
	}
	if len(list) == 0 {
		return 0, 0, nil
	}
	ids := make([]uuid.UUID, 0, len(list))
	for _, c := range list {
		ids = append(ids, c.ItemID)
	}
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT count(DISTINCT item)::int FROM (
			SELECT substring(target FROM 14)::uuid AS item FROM admin_log
			WHERE at >= now() - interval '30 days' AND target LIKE 'catalog_item:%'
			  AND action IN ('`+ActionUnpublish+`','`+ActionGrantRevoke+`','`+ActionPublish+`','catalog.update')
			UNION SELECT item_id AS item FROM catalog_item_pins WHERE pinned_at >= now() - interval '30 days') x
		WHERE item = ANY($1)`, ids).Scan(&n); err != nil {
		return 0, 0, err
	}
	return n, len(list), nil
}

// Candidates is GET /catalog/usage/candidates: static reasons plus the actual
// metrics of the period (R20). No hints are computed for the list.
func (s *Service) Candidates(ctx context.Context, f Filter, reason string, limit int, cursor *httpx.Cursor) ([]Candidate, *string, error) {
	if limit <= 0 {
		limit = 50
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		return nil, nil, err
	}
	dataFrom, _ := ParseDay(meta.DataFrom)
	f.From, f.To, _ = ClampPeriod(f.From, f.To, dataFrom)
	items, err := s.items(ctx)
	if err != nil {
		return nil, nil, err
	}
	period, err := s.aggregate(ctx, Filter{From: f.From, To: f.To})
	if err != nil {
		return nil, nil, err
	}
	now := s.clock()
	toolIndex := map[string][]uuid.UUID{}
	for id, it := range items {
		if it.Type != "mcp" || it.DeletedAt != nil || !it.Published || it.InDev {
			continue
		}
		for _, n := range toolNames(it.Allowed, it.Tools) {
			toolIndex[n] = append(toolIndex[n], id)
		}
	}
	out := []Candidate{}
	for id, it := range items {
		if it.DeletedAt != nil || !it.Published || it.InDev {
			continue
		}
		a := period[id]
		c := Candidate{ItemID: id, ItemName: firstNonEmpty(it.Title, it.Name), ItemType: it.Type, Reasons: []string{},
			LastOkAt: it.CheckedAt, Tools: ToolsOf(it.Allowed, it.Tools)}
		if a != nil {
			c.Calls = a.Calls
			c.AuthShare = AuthShareOf(a.Errors["auth"], a.Calls, a.Errors["denied"])
		}
		if it.Type == "mcp" && !it.ExplicitGrant {
			c.Reasons = append(c.Reasons, "not_granted")
		}
		if it.Type == "mcp" && c.Tools >= s.Cfg.ToolsCandidate {
			c.Reasons = append(c.Reasons, "tools_many")
		}
		if it.Type == "mcp" && sharesTools(toolIndex, it) {
			c.Reasons = append(c.Reasons, "tools_overlap")
		}
		if it.PublishedAt != nil && now.Sub(*it.PublishedAt) < time.Duration(s.Cfg.UnusedDays)*24*time.Hour {
			c.Reasons = append(c.Reasons, "new_position")
		}
		if it.Type == "mcp" && a != nil && a.Calls-a.Admin-a.Service > 0 && a.Service > 0 && a.Calls-a.Admin == a.Service {
			c.Reasons = append(c.Reasons, "service_only")
		}
		if c.Calls >= int64(s.Cfg.AuthMinCalls) && c.AuthShare >= s.Cfg.AuthShare {
			c.Reasons = append(c.Reasons, "many_auth")
		}
		if it.Status != nil && *it.Status == "error" {
			c.Reasons = append(c.Reasons, "unavailable")
		}
		if reason != "" && !contains(c.Reasons, reason) {
			continue
		}
		if len(c.Reasons) == 0 {
			continue
		}
		out = append(out, c)
	}
	sortCandidates(out)
	start := offset(cursor)
	if start > len(out) {
		start = len(out)
	}
	end := start + limit
	if end > len(out) {
		end = len(out)
	}
	var next *string
	if end < len(out) {
		c := httpx.EncodeCursor(time.Time{}, strconv.Itoa(end))
		next = &c
	}
	return out[start:end], next, nil
}

func sharesTools(index map[string][]uuid.UUID, it *itemInfo) bool {
	for _, n := range toolNames(it.Allowed, it.Tools) {
		for _, other := range index[n] {
			if other != it.ID {
				return true
			}
		}
	}
	return false
}

func sortCandidates(list []Candidate) {
	for i := 1; i < len(list); i++ {
		for j := i; j > 0 && list[j].Calls > list[j-1].Calls; j-- {
			list[j], list[j-1] = list[j-1], list[j]
		}
	}
}

// rowsWithMeta is the flat table without pagination: the summary and the
// export read the same rows as the screen (R25).
func (s *Service) rowsWithMeta(ctx context.Context, f Filter) ([]UsageRow, *string, Meta, error) {
	return s.Report(ctx, f)
}

// ItemExists is used by the action handlers.
func (s *Service) ItemExists(ctx context.Context, id uuid.UUID) (bool, error) {
	var ok bool
	err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM catalog_items WHERE id = $1)
		OR EXISTS (SELECT 1 FROM catalog_item_tombstones WHERE item_id = $1)`, id).Scan(&ok)
	return ok, err
}

func trim(s string) string { return strings.TrimSpace(s) }
