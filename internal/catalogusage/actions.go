package catalogusage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Actor is who performs an action of the tab: the author of the admin_log
// record and of the event (R31).
type Actor struct {
	ID    uuid.UUID
	Email string
}

// catalogChanged is the event of the administration (R31); there is no
// delivery to users.
const catalogChanged = "catalog.changed"

func (s *Service) publishChanged(ctx context.Context, action string, id uuid.UUID, name, reason string, actor Actor) {
	if s.Events == nil {
		return
	}
	s.Events.Publish(ctx, events.Event{Type: catalogChanged, Data: map[string]any{
		"action": action, "itemId": id.String(), "itemName": name, "reason": reason, "actorEmail": actor.Email}})
}

func (s *Service) publishAccess(ctx context.Context) {
	if s.Access != nil {
		s.Access.PublishChanged(ctx)
	}
}

// item is the reference of an action.
type item struct {
	ID        uuid.UUID
	Name      string
	Title     string
	Type      string
	Mode      *string
	Published bool
	InDev     bool
}

func (s *Service) item(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, id uuid.UUID) (*item, error) {
	var it item
	err := q.QueryRow(ctx, `SELECT id, name, coalesce(title,''), type, mode, published, in_development FROM catalog_items WHERE id = $1`, id).
		Scan(&it.ID, &it.Name, &it.Title, &it.Type, &it.Mode, &it.Published, &it.InDev)
	if postgres.IsNoRows(err) {
		return nil, ErrItemNotFound
	}
	if err != nil {
		return nil, err
	}
	return &it, nil
}

func adminLog(ctx context.Context, tx pgx.Tx, actor Actor, action, target string, before, after any) error {
	var b, a []byte
	if before != nil {
		b, _ = json.Marshal(before)
	}
	if after != nil {
		a, _ = json.Marshal(after)
	}
	var id *uuid.UUID
	if actor.ID != uuid.Nil {
		id = &actor.ID
	}
	_, err := tx.Exec(ctx, `INSERT INTO admin_log (actor_id, actor_email, action, target, before, after) VALUES ($1,$2,$3,$4,$5,$6)`,
		id, actor.Email, action, target, b, a)
	return err
}

// errDryRun rolls back a transaction that only counted.
var errDryRun = apperr.Conflict("dry_run", "dry run")

// ImpactUnpublish is POST /catalog/{id}/unpublish:impact (R27): a rolled-back
// transaction that changes nothing.
func (s *Service) ImpactUnpublish(ctx context.Context, id uuid.UUID) (Impact, error) {
	var out Impact
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		if out, err = s.impactOf(ctx, tx, id, nil); err != nil {
			return err
		}
		return errDryRun // nothing is written: the transaction goes back
	})
	if err != nil && err != errDryRun {
		return Impact{}, err
	}
	return out, nil
}

// ImpactGrant is POST /catalog/{id}/grants:impact (R28): only the affected group.
func (s *Service) ImpactGrant(ctx context.Context, id, group uuid.UUID) (Impact, error) {
	var out Impact
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var has bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM access_grants WHERE kind = 'catalog_item' AND item_id = $1 AND group_id = $2)`,
			id, group).Scan(&has); err != nil {
			return err
		}
		if !has {
			return apperr.NotFound("grant_not_found", "the grant is not found")
		}
		var err error
		if out, err = s.impactOf(ctx, tx, id, &group); err != nil {
			return err
		}
		return errDryRun
	})
	if err != nil {
		return Impact{}, err
	}
	return out, nil
}

// impact collects the consequences of taking the item away, within the group
// when it is given.
func (s *Service) impactOf(ctx context.Context, tx pgx.Tx, id uuid.UUID, only *uuid.UUID) (Impact, error) {
	it, err := s.item(ctx, tx, id)
	if err != nil {
		return Impact{}, err
	}
	out := Impact{Item: ImpactItem{ID: it.ID, Name: it.Name, Type: it.Type, Mode: it.Mode, Published: it.Published && !it.InDev},
		Grants: []ImpactGrant{}, CollectionTasks: []ImpactTask{}, ServiceAgents: []ImpactAgent{}, ServiceClients: []ImpactClient{}}
	rows, err := tx.Query(ctx, `SELECT g.id, g.name, g.builtin,
			(SELECT count(*)::int FROM users u WHERE u.status <> 'blocked' AND (
				g.builtin = 'all' OR (g.builtin = 'admins' AND u.is_admin)
				OR EXISTS (SELECT 1 FROM user_group_members m WHERE m.group_id = g.id AND m.user_id = u.id)))
		FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
		WHERE ag.kind = 'catalog_item' AND ag.item_id = $1 AND ($2::uuid IS NULL OR g.id = $2)
		ORDER BY g.position`, id, only)
	if err != nil {
		return Impact{}, err
	}
	for rows.Next() {
		var g ImpactGrant
		if err := rows.Scan(&g.GroupID, &g.GroupName, &g.Builtin, &g.Users); err != nil {
			rows.Close()
			return Impact{}, err
		}
		out.Grants = append(out.Grants, g)
	}
	rows.Close()
	users, err := s.affectedUsers(ctx, tx, id, only)
	if err != nil {
		return Impact{}, err
	}
	out.Users = len(users)
	if err := tx.QueryRow(ctx, `SELECT count(*)::int FROM user_connections WHERE item_id = $1
		AND ($2::uuid[] IS NULL OR user_id = ANY($2))`, id, uuidArray(users)).Scan(&out.Connections); err != nil {
		return Impact{}, err
	}
	tasks, err := tx.Query(ctx, `SELECT t.id, u.email, t.title FROM scheduled_tasks t JOIN users u ON u.id = t.user_id
		WHERE t.status = 'active' AND t.kind <> 'agent' AND t.user_id = ANY($1::uuid[]) ORDER BY t.created_at`, uuidArray(users))
	if err != nil {
		return Impact{}, err
	}
	for tasks.Next() {
		var t ImpactTask
		if err := tasks.Scan(&t.TaskID, &t.UserEmail, &t.Title); err != nil {
			tasks.Close()
			return Impact{}, err
		}
		out.CollectionTasks = append(out.CollectionTasks, t)
	}
	tasks.Close()
	// service agents whose configuration names the item, and the clients that run them
	agents, err := tx.Query(ctx, `SELECT name FROM service_agents WHERE config->'mcp' ? $1 ORDER BY name`, it.Name)
	if err != nil {
		return Impact{}, err
	}
	var names []string
	for agents.Next() {
		var n string
		if err := agents.Scan(&n); err != nil {
			agents.Close()
			return Impact{}, err
		}
		out.ServiceAgents = append(out.ServiceAgents, ImpactAgent{Name: n, Tasks: []ImpactTaskRef{}})
		names = append(names, n)
	}
	agents.Close()
	if len(names) > 0 {
		clients, err := tx.Query(ctx, `SELECT client_id, name, agents FROM service_clients WHERE enabled AND agents && $1::text[] ORDER BY name`, names)
		if err != nil {
			return Impact{}, err
		}
		for clients.Next() {
			var c ImpactClient
			if err := clients.Scan(&c.ClientID, &c.Name, &c.Agents); err != nil {
				clients.Close()
				return Impact{}, err
			}
			out.ServiceClients = append(out.ServiceClients, c)
		}
		clients.Close()
	}
	// the reference slice: calls of 90 days and the last call (R27)
	from := Today(s.clock()).AddDate(0, 0, -s.Cfg.ReferenceDays+1)
	if err := tx.QueryRow(ctx, `SELECT coalesce(sum(calls), 0)::bigint,
			(max(day) FILTER (WHERE calls > 0))::timestamp AT TIME ZONE 'UTC'
		FROM catalog_usage_daily WHERE item_id = $1 AND group_id = '`+ZeroUUID+`'::uuid AND day >= $2`,
		id, from).Scan(&out.Calls90, &out.LastCallAt); err != nil {
		return Impact{}, err
	}
	pub, err := s.publication(ctx, tx, id, from)
	if err != nil {
		return Impact{}, err
	}
	out.Publication = pub
	return out, nil
}

func uuidArray(ids []uuid.UUID) any {
	if len(ids) == 0 {
		return nil
	}
	return ids
}

// affectedUsers is the users that lose the item: the members of the granted
// groups (or of one group) and the administrators, who have everything.
func (s *Service) affectedUsers(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, id uuid.UUID, only *uuid.UUID) ([]uuid.UUID, error) {
	rows, err := q.Query(ctx, `SELECT DISTINCT u.id FROM users u
		WHERE u.status <> 'blocked' AND (
			u.is_admin OR EXISTS (
				SELECT 1 FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
				WHERE ag.kind = 'catalog_item' AND ag.item_id = $1 AND ($2::uuid IS NULL OR g.id = $2)
				  AND (g.builtin = 'all' OR EXISTS (SELECT 1 FROM user_group_members m WHERE m.group_id = g.id AND m.user_id = u.id))))`,
		id, only)
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

// publication is the last interval with the days of the window (R13).
func (s *Service) publication(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, id uuid.UUID, from time.Time) (*Interval, error) {
	var v Interval
	err := q.QueryRow(ctx, `SELECT p.id, p.published_at, p.unpublished_at, p.estimated,
			(SELECT count(*)::int FROM generate_series($2::date, current_date, interval '1 day') d(day)
				WHERE d.day::date >= p.published_at::date AND (p.unpublished_at IS NULL OR d.day::date < p.unpublished_at::date)),
			(SELECT count(*)::int FROM generate_series($2::date, current_date, interval '1 day') d(day)
				WHERE NOT (d.day::date >= p.published_at::date AND (p.unpublished_at IS NULL OR d.day::date < p.unpublished_at::date)))
		FROM catalog_item_publications p WHERE p.item_id = $1 ORDER BY p.published_at DESC LIMIT 1`, id, from).
		Scan(&v.ID, &v.PublishedAt, &v.UnpublishedAt, &v.Estimated, &v.AvailableDays, &v.RemovedDays)
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// Result of an action.
type Result struct {
	RemovedConnections    int       `json:"removedConnections"`
	PausedCollectionTasks int       `json:"pausedCollectionTasks"`
	PausedAgentTasks      int       `json:"pausedAgentTasks"`
	Publication           *Interval `json:"publication"`
}

// Unpublish is POST /catalog/{id}/unpublish (R27, R32): it closes the
// interval, removes the connections and pauses the tasks in one transaction.
func (s *Service) Unpublish(ctx context.Context, actor Actor, id uuid.UUID, reason, confirmName string) (Result, error) {
	if trim(reason) == "" {
		return Result{}, apperr.Unprocessable("reason_required", "a reason is required").With("field", "reason")
	}
	var res Result
	var itemName string
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		it, err := s.item(ctx, tx, id)
		if err != nil {
			return err
		}
		itemName = it.Name
		if !it.Published || it.InDev {
			return apperr.Conflict("already_unpublished", "the item is not published")
		}
		imp, err := s.impactOf(ctx, tx, id, nil)
		if err != nil {
			return err
		}
		if imp.Connections > 0 && confirmName != it.Name {
			return apperr.Unprocessable("name_mismatch", "confirm the action with the name of the item").With("field", "confirmName")
		}
		pub, err := s.publication(ctx, tx, id, Today(s.clock()).AddDate(0, 0, -s.Cfg.ReferenceDays+1))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE catalog_item_publications SET unpublished_at = now()
			WHERE item_id = $1 AND unpublished_at IS NULL`, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE catalog_items SET published = false, unpublished_at = now(), updated_at = now() WHERE id = $1`, id); err != nil {
			return err
		}
		users, err := s.affectedUsers(ctx, tx, id, nil)
		if err != nil {
			return err
		}
		connections, paused, err := access.Revoke(ctx, tx, users, "no access to the source: the item was unpublished")
		if err != nil {
			return err
		}
		res = Result{RemovedConnections: connections, PausedCollectionTasks: paused, Publication: pub}
		if res.Publication != nil {
			now := s.clock()
			res.Publication.UnpublishedAt = &now
		}
		return adminLog(ctx, tx, actor, ActionUnpublish, "catalog_item:"+id.String(),
			map[string]any{"published": true, "connections": imp.Connections},
			map[string]any{"reason": trim(reason), "removedConnections": connections, "pausedTasks": paused})
	})
	if err != nil {
		return Result{}, err
	}
	s.publishAccess(ctx)
	s.publishChanged(ctx, ActionUnpublish, id, itemName, trim(reason), actor)
	return res, nil
}

// Publish is POST /catalog/{id}/publish (R29): a new interval; the grants
// stay, the connections and the pause do not come back by themselves.
func (s *Service) Publish(ctx context.Context, actor Actor, id uuid.UUID, reason string) (Result, error) {
	if trim(reason) == "" {
		return Result{}, apperr.Unprocessable("reason_required", "a reason is required").With("field", "reason")
	}
	var res Result
	var itemName string
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		it, err := s.item(ctx, tx, id)
		if err != nil {
			return err
		}
		itemName = it.Name
		if it.Published && !it.InDev {
			return apperr.Conflict("already_published", "the item is published")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO catalog_item_publications (item_id, published_at, estimated, actor_id)
			VALUES ($1, now(), false, $2)`, id, nilUUID(actor.ID)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE catalog_items SET published = true, published_at = now(), unpublished_at = NULL,
			updated_at = now() WHERE id = $1`, id); err != nil {
			return err
		}
		pub, err := s.publication(ctx, tx, id, Today(s.clock()).AddDate(0, 0, -s.Cfg.ReferenceDays+1))
		if err != nil {
			return err
		}
		res.Publication = pub
		return adminLog(ctx, tx, actor, ActionPublish, "catalog_item:"+id.String(),
			map[string]any{"published": false}, map[string]any{"reason": trim(reason)})
	})
	if err != nil {
		return Result{}, err
	}
	s.publishAccess(ctx)
	s.publishChanged(ctx, ActionPublish, id, itemName, trim(reason), actor)
	return res, nil
}

func nilUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

// RevokeGrant is DELETE /catalog/{id}/grants/{groupId} (R28): the grant of one
// group goes, the other groups and the item itself stay.
func (s *Service) RevokeGrant(ctx context.Context, actor Actor, id, group uuid.UUID, reason string) error {
	if trim(reason) == "" {
		return apperr.Unprocessable("reason_required", "a reason is required").With("field", "reason")
	}
	var itemName string
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		it, err := s.item(ctx, tx, id)
		if err != nil {
			return err
		}
		itemName = it.Name
		tag, err := tx.Exec(ctx, `DELETE FROM access_grants WHERE kind = 'catalog_item' AND item_id = $1 AND group_id = $2`, id, group)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return apperr.NotFound("grant_not_found", "the grant is not found")
		}
		users, err := s.affectedUsers(ctx, tx, id, &group)
		if err != nil {
			return err
		}
		connections, paused, err := access.Revoke(ctx, tx, users, "no access to the source: the grant of the group was revoked")
		if err != nil {
			return err
		}
		return adminLog(ctx, tx, actor, ActionGrantRevoke, "catalog_item:"+id.String(),
			map[string]any{"groupId": group.String()},
			map[string]any{"reason": trim(reason), "removedConnections": connections, "pausedTasks": paused})
	})
	if err != nil {
		return err
	}
	s.publishAccess(ctx)
	s.publishChanged(ctx, ActionGrantRevoke, id, itemName, trim(reason), actor)
	return nil
}

// PublishPin is POST /catalog/{id}/pins (R12).
func (s *Service) PublishPin(ctx context.Context, actor Actor, id uuid.UUID, reason string, reviewAt *time.Time) (Pin, error) {
	if trim(reason) == "" {
		return Pin{}, apperr.Unprocessable("reason_required", "a reason is required").With("field", "reason")
	}
	if _, err := s.ItemExists(ctx, id); err != nil {
		return Pin{}, err
	}
	it, err := s.item(ctx, s.Pool, id)
	if err != nil {
		return Pin{}, err
	}
	now := s.clock()
	review := PinReviewAt(now, s.Cfg)
	if reviewAt != nil {
		if !reviewAt.After(now) {
			return Pin{}, apperr.Unprocessable("review_at_past", "the date of the review is in the past").With("field", "reviewAt")
		}
		review = *reviewAt
	}
	var p Pin
	err = s.Pool.QueryRow(ctx, `INSERT INTO catalog_item_pins (item_id, reason, pinned_by, review_at) VALUES ($1,$2,$3,$4)
		RETURNING id, item_id, reason, pinned_by, pinned_at, review_at, released_at`,
		id, trim(reason), nilUUID(actor.ID), review).Scan(&p.ID, &p.ItemID, &p.Reason, &p.PinnedBy, &p.PinnedAt, &p.ReviewAt, &p.ReleasedAt)
	if err != nil {
		return Pin{}, err
	}
	p.ItemName = firstNonEmpty(it.Title, it.Name)
	p.Effective = p.ReleasedAt == nil && p.ReviewAt.After(now)
	return p, nil
}

// ReleasePin is DELETE /catalog/{id}/pins/{pinId} (R12).
func (s *Service) ReleasePin(ctx context.Context, id, pin uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE catalog_item_pins SET released_at = now()
		WHERE id = $1 AND item_id = $2 AND released_at IS NULL`, pin, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return apperr.NotFound("pin_not_found", "the pin is not found")
	}
	return nil
}
