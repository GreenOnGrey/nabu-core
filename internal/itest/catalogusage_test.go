//go:build integration

package itest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/catalogusage"
)

// usageItem creates an MCP item of the catalog with an open publication and
// an explicit grant to «All users».
func usageItem(t *testing.T, name string, published bool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	var id uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO catalog_items (type, name, title, source, mode, published, in_development, status, allowed_tools)
		VALUES ('mcp', $1, $2, '{"kind":"url","url":"http://usage.invalid/mcp"}'::jsonb, 'platform', $3, false, 'ok', '["a","b"]'::jsonb)
		RETURNING id`, name, "Item "+name, published).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if published {
		if _, err := pool.Exec(ctx, `UPDATE catalog_items SET published_at = now() - interval '40 days' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO catalog_item_publications (item_id, published_at) VALUES ($1, now() - interval '40 days')`, id); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO access_grants (group_id, kind, item_id)
			SELECT id, 'catalog_item', $1 FROM user_groups WHERE builtin = 'all' ON CONFLICT DO NOTHING`, id); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM catalog_items WHERE id = $1`, id) })
	return id
}

func usageService() *catalogusage.Service {
	cfg := catalogusage.DefaultConfig()
	return &catalogusage.Service{Pool: pool, Cfg: cfg, Access: &access.Service{Pool: pool}}
}

// AGG-01, AGG-06, AGG-10: the calls of a day sum up, the groups duplicate the
// items, the zero rows of the availability days exist.
func TestCatalogUsageAggregates(t *testing.T) {
	ctx := context.Background()
	name := "agg-" + uuid.NewString()[:6]
	id := usageItem(t, name, true)
	user := newUser(t, name+"-u@x.org")
	other := newUser(t, name+"-o@x.org")
	admin := newUser(t, name+"-a@x.org")
	if _, err := pool.Exec(ctx, `UPDATE users SET is_admin = true WHERE id = $1`, admin); err != nil {
		t.Fatal(err)
	}
	day := catalogusage.Today(time.Now())
	if _, err := pool.Exec(ctx, `INSERT INTO audit (agent_kind, agent, user_id, channel, server, tool, result, result_code, duration_ms, item_id, session_id)
		SELECT 'personal', 'Nabu', $1::uuid, 'web', $2, 'a', 'ok', 'ok', 100, $3::uuid, 's-' || g
		FROM generate_series(1, 480) g`, user, name, id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO audit (agent_kind, agent, user_id, channel, server, tool, result, result_code, duration_ms, item_id, session_id)
		SELECT 'personal', 'Nabu', $1::uuid, 'web', $2, 'b', 'error', 'tool_error', 900, $3::uuid, 'e-' || g
		FROM generate_series(1, 17) g`, other, name, id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO audit (agent_kind, agent, user_id, channel, server, tool, result, result_code, duration_ms, item_id, session_id)
		SELECT 'personal', 'Nabu', $1::uuid, 'web', $2, 'b', 'error', 'tool_error', 900, $3::uuid, 'a-' || g
		FROM generate_series(1, 3) g`, admin, name, id); err != nil {
		t.Fatal(err)
	}
	if err := usageService().Recompute(ctx); err != nil {
		t.Fatal(err)
	}
	var calls, ok, adminCalls, serviceCalls, toolErrors int64
	if err := pool.QueryRow(ctx, `SELECT coalesce(sum(calls),0), coalesce(sum(ok),0), coalesce(sum(admin_calls),0),
			coalesce(sum(service_calls),0), coalesce(sum((errors_by_code->>'tool_error')::bigint),0)
		FROM catalog_usage_daily WHERE day = $1 AND item_id = $2 AND group_id = '`+catalogusage.ZeroUUID+`'`,
		day, id).Scan(&calls, &ok, &adminCalls, &serviceCalls, &toolErrors); err != nil {
		t.Fatal(err)
	}
	if calls != 500 || ok != 480 || adminCalls != 3 || toolErrors != 20 {
		t.Fatalf("calls=%d ok=%d admin=%d service=%d errors=%d", calls, ok, adminCalls, serviceCalls, toolErrors)
	}
	// AGG-10: a group row duplicates the calls of the item; the item row stays additive
	var groupCalls int64
	if err := pool.QueryRow(ctx, `SELECT sum(calls) FROM catalog_usage_daily WHERE day = $1 AND item_id = $2 AND group_id <> '`+
		catalogusage.ZeroUUID+`'`, day, id).Scan(&groupCalls); err != nil {
		t.Fatal(err)
	}
	if groupCalls != 500 {
		t.Fatalf("group calls = %d, want 500", groupCalls)
	}
	// AGG-06: zero rows of the days of availability before today
	var zeroDays int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM catalog_usage_daily WHERE item_id = $1 AND calls = 0
		AND group_id = '`+catalogusage.ZeroUUID+`' AND caller_kind = 'personal'`, id).Scan(&zeroDays); err != nil {
		t.Fatal(err)
	}
	if zeroDays == 0 {
		t.Fatal("no zero rows for the days of availability")
	}
}

// ACT-02, ACT-04, ACT-05, ACT-11, ACT-12, ACC-03: unpublishing removes the
// connections and pauses the tasks in one transaction; publishing opens a new
// interval and does not resume the tasks.
func TestCatalogUsageUnpublish(t *testing.T) {
	ctx := context.Background()
	name := "unp-" + uuid.NewString()[:6]
	id := usageItem(t, name, true)
	user := newUser(t, name+"@x.org")
	if _, err := pool.Exec(ctx, `INSERT INTO user_connections (user_id, item_id) VALUES ($1,$2)`, user, id); err != nil {
		t.Fatal(err)
	}
	var task uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO scheduled_tasks (user_id, title, instruction, schedule_kind, cron, timezone, channel, kind, settings)
		VALUES ($1, 'Сбор почты', 'collect', 'cron', '0 9 * * *', 'Europe/Moscow', 'web', 'memory_collect', '{}')
		RETURNING id`, user).Scan(&task); err != nil {
		t.Fatal(err)
	}
	svc := usageService()
	actor := catalogusage.Actor{ID: user, Email: "root@x.org"}
	// ACT-02: a reason is required
	if _, err := svc.Unpublish(ctx, actor, id, " ", name); !isCodeErr(err, "reason_required") {
		t.Fatalf("ACT-02: %v", err)
	}
	// ACT-03: the name of the item confirms the removal of the connection
	if _, err := svc.Unpublish(ctx, actor, id, "cleanup", "wrong"); !isCodeErr(err, "name_mismatch") {
		t.Fatalf("ACT-03: %v", err)
	}
	res, err := svc.Unpublish(ctx, actor, id, "cleanup", name)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedConnections != 1 || res.PausedCollectionTasks < 1 {
		t.Fatalf("result = %+v", res)
	}
	var published bool
	var open int
	if err := pool.QueryRow(ctx, `SELECT published FROM catalog_items WHERE id = $1`, id).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("the item is still published")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM catalog_item_publications WHERE item_id = $1 AND unpublished_at IS NULL`, id).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Fatal("the open interval is not closed")
	}
	var connections int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_connections WHERE item_id = $1`, id).Scan(&connections); err != nil {
		t.Fatal(err)
	}
	if connections != 0 {
		t.Fatal("the connection is not removed")
	}
	var status, pauseKind, action string
	if err := pool.QueryRow(ctx, `SELECT status, coalesce(pause_kind,'') FROM scheduled_tasks WHERE id = $1`, task).Scan(&status, &pauseKind); err != nil {
		t.Fatal(err)
	}
	if status != "paused" || pauseKind != access.PauseSourceAccess {
		t.Fatalf("the task is %s/%s, want paused/source_access", status, pauseKind)
	}
	if err := pool.QueryRow(ctx, `SELECT action FROM admin_log WHERE target = $1 ORDER BY id DESC LIMIT 1`,
		"catalog_item:"+id.String()).Scan(&action); err != nil {
		t.Fatal(err)
	}
	if action != catalogusage.ActionUnpublish {
		t.Fatalf("action = %q", action)
	}
	// ACT-11, ACT-12: publishing opens a new interval and keeps the pause
	if _, err := svc.Publish(ctx, actor, id, "back"); err != nil {
		t.Fatal(err)
	}
	var intervals, paused int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM catalog_item_publications WHERE item_id = $1`, id).Scan(&intervals); err != nil {
		t.Fatal(err)
	}
	if intervals != 2 {
		t.Fatalf("intervals = %d, want 2", intervals)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM scheduled_tasks WHERE id = $1 AND status = 'paused'`, task).Scan(&paused); err != nil {
		t.Fatal(err)
	}
	if paused != 1 {
		t.Fatal("publishing must not resume the tasks")
	}
	// ACC-03: the grants are unchanged by both actions
	var grants int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM access_grants WHERE kind = 'catalog_item' AND item_id = $1`, id).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if grants != 1 {
		t.Fatalf("grants = %d, want 1", grants)
	}
}

// ACC-07, ACC-08: the one-time reconciliation of access of the rollout window
// removes the connections of unpublished items, pauses the tasks, marks the
// completion and changes nothing on a second run.
func TestCatalogUsageReconcilePublished(t *testing.T) {
	ctx := context.Background()
	name := "rec-" + uuid.NewString()[:6]
	id := usageItem(t, name, false) // unchecked by the filter — created unpublished
	user := newUser(t, name+"@x.org")
	if _, err := pool.Exec(ctx, `INSERT INTO user_connections (user_id, item_id) VALUES ($1,$2)`, user, id); err != nil {
		t.Fatal(err)
	}
	acc := &access.Service{Pool: pool}
	n, ran, err := acc.ReconcilePublished(ctx, access.Actor{Email: "system"})
	if err != nil || !ran || n < 1 {
		t.Fatalf("reconcile: n=%d ran=%v err=%v", n, ran, err)
	}
	var connections int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_connections WHERE item_id = $1`, id).Scan(&connections); err != nil {
		t.Fatal(err)
	}
	if connections != 0 {
		t.Fatal("the connection of an unpublished item is not removed")
	}
	var done bool
	if err := pool.QueryRow(ctx, `SELECT (value->>'done')::boolean FROM settings WHERE key = 'catalog_access_reconcile'`).Scan(&done); err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("the completion is not marked")
	}
	if _, ran, err := acc.ReconcilePublished(ctx, access.Actor{Email: "system"}); err != nil || ran {
		t.Fatalf("the second run: ran=%v err=%v", ran, err)
	}
}

// GOL-01, GOL-10: the goal row is precomputed by the worker — 6 dead of 40
// MCP items is 15.0 %; a second run of the same day replaces the row.
func TestCatalogUsageGoal(t *testing.T) {
	ctx := context.Background()
	tag := uuid.NewString()[:6]
	day := catalogusage.Today(time.Now()).AddDate(0, 0, -1)
	var ids []uuid.UUID
	for i := range 40 {
		id := usageItem(t, "goal-"+tag+"-"+string(rune('a'+i%26))+string(rune('a'+i/26)), true)
		ids = append(ids, id)
	}
	// the first 34 items have a human call in the window; the last 6 are dead
	for i, id := range ids {
		if i >= 34 {
			continue
		}
		for d := range 90 {
			if _, err := pool.Exec(ctx, `INSERT INTO catalog_usage_daily (day, item_id, item_name, item_type, group_id, caller_kind,
					available, calls, ok, users, sessions, admin_calls, service_calls)
				VALUES ($1,$2,$3,'mcp','`+catalogusage.ZeroUUID+`','personal',true,1,1,1,1,0,0)`,
				day.AddDate(0, 0, -d), id, "goal-"+tag); err != nil {
				t.Fatal(err)
			}
		}
	}
	svc := usageService()
	if err := svc.ComputeGoal(ctx, day); err != nil {
		t.Fatal(err)
	}
	var dead, total int
	var share float64
	if err := pool.QueryRow(ctx, `SELECT dead, total, share::float8 FROM catalog_usage_goal_daily WHERE day = $1`, day).
		Scan(&dead, &total, &share); err != nil {
		t.Fatal(err)
	}
	if total < 40 || dead < 6 {
		t.Fatalf("goal: dead=%d total=%d share=%v", dead, total, share)
	}
	// GOL-01: the share is the percentage of the dead items of the denominator
	if want := float64(int64(1000*float64(dead)/float64(total)+0.5)) / 10; share != want {
		t.Fatalf("share = %v, want %v (dead=%d total=%d)", share, want, dead, total)
	}
	if err := svc.ComputeGoal(ctx, day); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM catalog_usage_goal_daily WHERE day = $1`, day).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows of the day = %d, want 1", rows)
	}
}
