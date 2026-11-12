package access

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// PauseSourceAccess is the reason of a pause of a task whose source is no
// longer accessible (FTR.NAB.CHAT-0005 R27, R28, R32).
const PauseSourceAccess = "source_access"

// PauseSourceTasks pauses the collection tasks of users who lost the access to
// an item: the task has no source to read (FTR.NAB.CHAT-0005 R27, tech §«API»).
// It reports how many tasks were paused now.
func PauseSourceTasks(ctx context.Context, q Querier, users []uuid.UUID, reason string) (int, error) {
	if len(users) == 0 {
		return 0, nil
	}
	if reason == "" {
		reason = "no access to the source"
	}
	tag, err := q.Exec(ctx, `UPDATE scheduled_tasks SET status = 'paused', pause_kind = $2, pause_reason = $3, next_run_at = NULL
		WHERE user_id = ANY($1) AND status = 'active' AND kind = 'memory_collect'`, users, PauseSourceAccess, reason)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// Revoke removes the connections of users to the items out of their access
// and pauses their collection tasks in the current transaction: the cascade
// of unpublishing an item and of revoking a grant of a group
// (FTR.NAB.CHAT-0005 R27, R28, R32; arch A12). It reports the removed
// connections and the paused tasks.
func Revoke(ctx context.Context, q Querier, users []uuid.UUID, reason string) (connections, paused int, err error) {
	var lost []uuid.UUID
	connections, lost, err = reconcile(ctx, q, users)
	if err != nil {
		return 0, 0, err
	}
	paused, err = PauseSourceTasks(ctx, q, lost, reason)
	return connections, paused, err
}

// UsersOfGroup returns the members of a group; the built-in groups are
// implicit and are resolved by SQL as well (FTR.NAB.CMN-0007 R1).
func UsersOfGroup(ctx context.Context, q Querier, group uuid.UUID) ([]uuid.UUID, error) {
	rows, err := q.Query(ctx, `SELECT u.id FROM users u
		WHERE EXISTS (SELECT 1 FROM user_groups g WHERE g.id = $1 AND (
			g.builtin = 'admins' AND u.is_admin OR g.builtin = 'all'
			OR EXISTS (SELECT 1 FROM user_group_members m WHERE m.group_id = g.id AND m.user_id = u.id)))
		AND u.status <> 'blocked'`, group)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// UsersWithAccess returns the users who have the item: members of the groups
// it is granted to and every administrator (the built-in group «admins» has
// every enabled resource).
func UsersWithAccess(ctx context.Context, q Querier, item uuid.UUID) ([]uuid.UUID, error) {
	rows, err := q.Query(ctx, `SELECT DISTINCT u.id FROM users u
		WHERE u.status <> 'blocked' AND (
			u.is_admin OR EXISTS (
				SELECT 1 FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
				WHERE ag.kind = 'catalog_item' AND ag.item_id = $1
				  AND (g.builtin = 'all' OR EXISTS (SELECT 1 FROM user_group_members m WHERE m.group_id = g.id AND m.user_id = u.id))))`, item)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ReconcilePublished runs the one-time reconciliation of the rollout window
// (FTR.NAB.CHAT-0005 R32, arch A22): pairs «user × item» that were in the
// access and fell out after the «published» filter lost their connections and
// got their collection tasks paused. It marks the completion in
// settings('catalog_access_reconcile') and does nothing on a second run.
// It reports the number of removed connections and whether it ran now.
func (s *Service) ReconcilePublished(ctx context.Context, actor Actor) (int, bool, error) {
	var done bool
	if err := s.Pool.QueryRow(ctx, `SELECT coalesce((value->>'done')::boolean, false) FROM settings WHERE key = 'catalog_access_reconcile'`).Scan(&done); err != nil {
		if postgres.IsNoRows(err) {
			return 0, false, nil // migration 0020 did not run yet
		}
		return 0, false, err
	}
	if done {
		return 0, false, nil
	}
	type pair struct{ user, item uuid.UUID }
	var pairs []pair
	rows, err := s.Pool.Query(ctx, `SELECT uc.user_id, uc.item_id
		FROM user_connections uc JOIN catalog_items i ON i.id = uc.item_id
		WHERE NOT i.published OR i.in_development`)
	if err != nil {
		return 0, false, err
	}
	for rows.Next() {
		var p pair
		if err := rows.Scan(&p.user, &p.item); err != nil {
			rows.Close()
			return 0, false, err
		}
		pairs = append(pairs, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	byItem := map[uuid.UUID][]uuid.UUID{}
	var users []uuid.UUID
	for _, p := range pairs {
		byItem[p.item] = append(byItem[p.item], p.user)
		users = append(users, p.user)
	}
	revoked := 0
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		n, paused, err := Revoke(ctx, tx, users, "no access to the source: the item was unpublished before the rollout")
		if err != nil {
			return err
		}
		revoked = n
		for item, us := range byItem {
			var name string
			_ = tx.QueryRow(ctx, `SELECT name FROM catalog_items WHERE id = $1`, item).Scan(&name)
			s.log(ctx, tx, actor, "catalog.unpublish", "catalog_item:"+item.String(),
				map[string]any{"reconcile": true, "users": len(us)},
				map[string]any{"reconcile": true, "removedConnections": len(us), "pausedTasks": paused})
		}
		v, _ := json.Marshal(map[string]any{"done": true, "at": time.Now().UTC(), "revoked": revoked})
		_, err = tx.Exec(ctx, `INSERT INTO settings (key, value) VALUES ('catalog_access_reconcile', $1)
			ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = now()`, v)
		return err
	})
	if err != nil {
		return 0, false, err
	}
	if len(pairs) > 0 {
		s.changed(ctx)
	}
	return revoked, true, nil
}

// PublishChanged publishes access.changed after a committed change of access
// (FTR.NAB.CHAT-0005 R31: unpublishing and revoking a grant).
func (s *Service) PublishChanged(ctx context.Context) { s.changed(ctx) }
