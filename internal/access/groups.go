package access

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Choice is a model of a connection.
type Choice struct {
	ConnectionID uuid.UUID `json:"connectionId"`
	Model        string    `json:"model"`
}

// Grants are what a group grants (R1).
type Grants struct {
	Models       []Choice    `json:"models"`
	DefaultModel *Choice     `json:"defaultModel"`
	Channels     []string    `json:"channels"`
	CatalogItems []uuid.UUID `json:"catalogItems"`
}

// Group is a group of users as the administration shows it.
type Group struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	Builtin     *string   `json:"builtin"`
	Position    int       `json:"position"`
	MemberCount int       `json:"memberCount"`
	// Grants is nil for «admins»: it has everything enabled (AllEnabled).
	Grants     *Grants `json:"grants"`
	AllEnabled bool    `json:"allEnabled"`
}

// UserRef is a member of a group.
type UserRef struct {
	ID    uuid.UUID `json:"id"`
	Email string    `json:"email"`
	Name  string    `json:"name"`
}

const groupCols = `g.id, g.name, g.description, g.builtin, g.position,
	CASE g.builtin
	  WHEN 'all' THEN (SELECT count(*) FROM users WHERE created_via <> 'group' AND status <> 'archived')
	  WHEN 'admins' THEN (SELECT count(*) FROM users WHERE is_admin AND status <> 'archived')
	  ELSE (SELECT count(*) FROM user_group_members m JOIN users u ON u.id = m.user_id WHERE m.group_id = g.id AND u.status <> 'archived') END`

func scanGroup(row pgx.Row) (*Group, error) {
	var g Group
	if err := row.Scan(&g.ID, &g.Name, &g.Description, &g.Builtin, &g.Position, &g.MemberCount); err != nil {
		return nil, err
	}
	g.AllEnabled = g.Builtin != nil && *g.Builtin == BuiltinAdmins
	return &g, nil
}

func (s *Service) grants(ctx context.Context, q Querier, gid uuid.UUID) (*Grants, error) {
	gr := &Grants{Models: []Choice{}, Channels: []string{}, CatalogItems: []uuid.UUID{}}
	rows, err := q.Query(ctx, `SELECT kind, connection_id, model, channel, item_id, is_default FROM access_grants WHERE group_id = $1 ORDER BY id`, gid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var conn, item *uuid.UUID
		var model, channel *string
		var def bool
		if err := rows.Scan(&kind, &conn, &model, &channel, &item, &def); err != nil {
			return nil, err
		}
		switch kind {
		case KindModel:
			c := Choice{ConnectionID: *conn, Model: *model}
			gr.Models = append(gr.Models, c)
			if def {
				gr.DefaultModel = &c
			}
		case KindChannel:
			gr.Channels = append(gr.Channels, *channel)
		case KindItem:
			gr.CatalogItems = append(gr.CatalogItems, *item)
		}
	}
	return gr, rows.Err()
}

// Groups lists the groups: «admins» first, the ordinary ones by position,
// «all» last (R4).
func (s *Service) Groups(ctx context.Context) ([]Group, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+groupCols+` FROM user_groups g ORDER BY g.position, lower(g.name)`)
	if err != nil {
		return nil, err
	}
	var out []Group
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, *g)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if !out[i].AllEnabled {
			if out[i].Grants, err = s.grants(ctx, s.Pool, out[i].ID); err != nil {
				return nil, err
			}
		}
	}
	if out == nil {
		out = []Group{}
	}
	return out, nil
}

// Group loads a group.
func (s *Service) Group(ctx context.Context, id uuid.UUID) (*Group, error) {
	g, err := scanGroup(s.Pool.QueryRow(ctx, `SELECT `+groupCols+` FROM user_groups g WHERE g.id = $1`, id))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "group not found")
	}
	if err != nil {
		return nil, err
	}
	if !g.AllEnabled {
		if g.Grants, err = s.grants(ctx, s.Pool, g.ID); err != nil {
			return nil, err
		}
	}
	return g, nil
}

func builtinErr() error {
	return apperr.Conflict("group_builtin", "a built-in group cannot be deleted, renamed or have its members changed")
}

func validName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || utf8.RuneCountInString(name) > 80 {
		return "", apperr.Unprocessable("invalid_name", "the name is 1–80 characters").With("field", "name")
	}
	return name, nil
}

// Actor is who changes access (the journal of the administration, R14).
type Actor struct {
	ID    uuid.UUID
	Email string
}

func (s *Service) log(ctx context.Context, q Querier, actor Actor, action, target string, before, after any) {
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
	_, _ = q.Exec(ctx, `INSERT INTO admin_log (actor_id, actor_email, action, target, before, after) VALUES ($1,$2,$3,$4,$5,$6)`,
		id, actor.Email, action, target, b, a)
}

// Create creates an ordinary group at the top of the order.
func (s *Service) Create(ctx context.Context, actor Actor, name string, descr *string) (*Group, error) {
	name, err := validName(name)
	if err != nil {
		return nil, err
	}
	var id uuid.UUID
	err = s.Pool.QueryRow(ctx, `INSERT INTO user_groups (name, description, position)
		VALUES ($1, NULLIF($2,''), COALESCE((SELECT max(position) FROM user_groups WHERE builtin IS NULL), 0) + 1) RETURNING id`, name, deref(descr)).Scan(&id)
	if postgres.IsUniqueViolation(err) {
		return nil, apperr.Conflict("group_name_taken", "a group with this name exists").With("field", "name")
	}
	if err != nil {
		return nil, err
	}
	s.log(ctx, s.Pool, actor, "group.create", "group:"+id.String(), nil, map[string]any{"name": name})
	return s.Group(ctx, id)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return strings.TrimSpace(*s)
}

// Update renames a group or changes its description.
func (s *Service) Update(ctx context.Context, actor Actor, id uuid.UUID, name, descr *string) (*Group, error) {
	g, err := s.Group(ctx, id)
	if err != nil {
		return nil, err
	}
	if g.Builtin != nil && name != nil {
		return nil, builtinErr()
	}
	if name != nil {
		n, err := validName(*name)
		if err != nil {
			return nil, err
		}
		name = &n
	}
	_, err = s.Pool.Exec(ctx, `UPDATE user_groups SET name = COALESCE($2, name),
		description = CASE WHEN $3::text IS NULL THEN description ELSE NULLIF($3,'') END, updated_at = now() WHERE id = $1`, id, name, descr)
	if postgres.IsUniqueViolation(err) {
		return nil, apperr.Conflict("group_name_taken", "a group with this name exists").With("field", "name")
	}
	if err != nil {
		return nil, err
	}
	s.log(ctx, s.Pool, actor, "group.update", "group:"+id.String(), map[string]any{"name": g.Name, "description": g.Description},
		map[string]any{"name": name, "description": descr})
	return s.Group(ctx, id)
}

// Delete deletes an ordinary group; confirm is its id (GRP-06).
func (s *Service) Delete(ctx context.Context, actor Actor, id uuid.UUID, confirm string) error {
	g, err := s.Group(ctx, id)
	if err != nil {
		return err
	}
	if g.Builtin != nil {
		return builtinErr()
	}
	if confirm != id.String() {
		return apperr.Unprocessable("confirm_required", "confirm with the id of the group")
	}
	members, err := s.memberIDs(ctx, s.Pool, id)
	if err != nil {
		return err
	}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM user_groups WHERE id = $1`, id); err != nil {
			return err
		}
		n, err := reconcile(ctx, tx, members)
		s.log(ctx, tx, actor, "group.delete", "group:"+id.String(), g, map[string]any{"revokedConnections": n})
		return err
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

// Order sets the order of the ordinary groups (R4); every one must be named.
func (s *Service) Order(ctx context.Context, actor Actor, ids []uuid.UUID) error {
	var all []uuid.UUID
	rows, err := s.Pool.Query(ctx, `SELECT id FROM user_groups WHERE builtin IS NULL`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id uuid.UUID
		if rows.Scan(&id) == nil {
			all = append(all, id)
		}
	}
	rows.Close()
	if len(ids) != len(all) {
		return apperr.Unprocessable("invalid_order", "name every ordinary group once")
	}
	for _, id := range all {
		if !slices.Contains(ids, id) {
			return apperr.Unprocessable("invalid_order", "name every ordinary group once")
		}
	}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		for i, id := range ids {
			if _, err := tx.Exec(ctx, `UPDATE user_groups SET position = $2, updated_at = now() WHERE id = $1`, id, i+1); err != nil {
				return err
			}
		}
		s.log(ctx, tx, actor, "group.order", "groups", nil, ids)
		return nil
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

// Members lists the members of a group; q searches the name and the email.
func (s *Service) Members(ctx context.Context, id uuid.UUID, q string, limit int) ([]UserRef, error) {
	g, err := s.Group(ctx, id)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	cond := `u.id IN (SELECT user_id FROM user_group_members WHERE group_id = $1)`
	switch {
	case g.Builtin != nil && *g.Builtin == BuiltinAll:
		cond = `$1::uuid IS NOT NULL`
	case g.Builtin != nil && *g.Builtin == BuiltinAdmins:
		cond = `u.is_admin AND $1::uuid IS NOT NULL`
	}
	rows, err := s.Pool.Query(ctx, `SELECT u.id, u.email, COALESCE(u.name,'') FROM users u WHERE `+cond+`
		AND u.created_via <> 'group' AND u.status <> 'archived' AND ($2 = '' OR u.email ILIKE '%' || $2 || '%' OR u.name ILIKE '%' || $2 || '%')
		ORDER BY lower(COALESCE(NULLIF(u.name,''), u.email)) LIMIT $3`, id, strings.TrimSpace(q), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UserRef{}
	for rows.Next() {
		var u UserRef
		if err := rows.Scan(&u.ID, &u.Email, &u.Name); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *Service) memberIDs(ctx context.Context, q Querier, id uuid.UUID) ([]uuid.UUID, error) {
	var b *string
	if err := q.QueryRow(ctx, `SELECT builtin FROM user_groups WHERE id = $1`, id).Scan(&b); err != nil {
		return nil, err
	}
	sql := `SELECT user_id FROM user_group_members WHERE group_id = $1`
	switch {
	case b != nil && *b == BuiltinAll:
		sql = `SELECT id FROM users WHERE $1::uuid IS NOT NULL AND created_via <> 'group'`
	case b != nil && *b == BuiltinAdmins:
		sql = `SELECT id FROM users WHERE $1::uuid IS NOT NULL AND is_admin`
	}
	rows, err := q.Query(ctx, sql, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var u uuid.UUID
		if rows.Scan(&u) == nil {
			out = append(out, u)
		}
	}
	return out, rows.Err()
}

// AddMembers adds users to an ordinary group (≤ 500 at a time).
func (s *Service) AddMembers(ctx context.Context, actor Actor, id uuid.UUID, users []uuid.UUID) error {
	g, err := s.Group(ctx, id)
	if err != nil {
		return err
	}
	if g.Builtin != nil {
		return builtinErr()
	}
	if len(users) == 0 || len(users) > 500 {
		return apperr.Unprocessable("invalid_users", "from 1 to 500 users")
	}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		for _, u := range users {
			if _, err := tx.Exec(ctx, `INSERT INTO user_group_members (group_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, id, u); err != nil {
				if postgres.IsForeignKeyViolation(err) {
					return apperr.Unprocessable("not_found", "unknown user").With("userId", u)
				}
				return err
			}
		}
		s.log(ctx, tx, actor, "group.members.add", "group:"+id.String(), nil, users)
		return nil
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

// RemoveMember removes a user from an ordinary group; what only the group
// gave is taken (R10).
func (s *Service) RemoveMember(ctx context.Context, actor Actor, id, user uuid.UUID) error {
	g, err := s.Group(ctx, id)
	if err != nil {
		return err
	}
	if g.Builtin != nil {
		return builtinErr()
	}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM user_group_members WHERE group_id = $1 AND user_id = $2`, id, user); err != nil {
			return err
		}
		n, err := reconcile(ctx, tx, []uuid.UUID{user})
		s.log(ctx, tx, actor, "group.members.remove", "group:"+id.String(), user, map[string]any{"revokedConnections": n})
		return err
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

// ModelInfo tells whether a model of a connection exists and calls tools; the
// models package answers it (wired in app).
type ModelInfo func(ctx context.Context, conn uuid.UUID, model string) (exists, usable bool)

// writeGrants replaces the grants of a group in a transaction.
func writeGrants(ctx context.Context, tx pgx.Tx, id uuid.UUID, g Grants, info ModelInfo) error {
	if g.DefaultModel != nil && !slices.ContainsFunc(g.Models, func(c Choice) bool { return c == *g.DefaultModel }) {
		return apperr.Unprocessable("invalid_grant", "the default model must be one of the models of the group").With("field", "defaultModel")
	}
	if _, err := tx.Exec(ctx, `DELETE FROM access_grants WHERE group_id = $1`, id); err != nil {
		return err
	}
	for _, m := range g.Models {
		if info != nil {
			exists, usable := info(ctx, m.ConnectionID, m.Model)
			if !exists {
				return apperr.Unprocessable("invalid_grant", "unknown model").With("model", m.Model)
			}
			if !usable { // FTR.NAB.CHAT-0001 R8, GRP-03
				return apperr.Unprocessable("model_no_tools", "the model cannot call tools").With("model", m.Model)
			}
		}
		def := g.DefaultModel != nil && *g.DefaultModel == m
		if _, err := tx.Exec(ctx, `INSERT INTO access_grants (group_id, kind, connection_id, model, is_default) VALUES ($1,'model',$2,$3,$4)
			ON CONFLICT DO NOTHING`, id, m.ConnectionID, m.Model, def); err != nil {
			if postgres.IsForeignKeyViolation(err) {
				return apperr.Unprocessable("invalid_grant", "unknown connection").With("connectionId", m.ConnectionID)
			}
			return err
		}
	}
	for _, c := range g.Channels {
		if c == "web" { // R6, GRP-04
			return apperr.Unprocessable("invalid_grant", "the site is always available and is not granted").With("channel", c)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO access_grants (group_id, kind, channel) VALUES ($1,'channel',$2) ON CONFLICT DO NOTHING`, id, c); err != nil {
			if postgres.IsForeignKeyViolation(err) {
				return apperr.Unprocessable("invalid_grant", "unknown channel").With("channel", c)
			}
			return err
		}
	}
	for _, it := range g.CatalogItems {
		if _, err := tx.Exec(ctx, `INSERT INTO access_grants (group_id, kind, item_id) VALUES ($1,'catalog_item',$2) ON CONFLICT DO NOTHING`, id, it); err != nil {
			if postgres.IsForeignKeyViolation(err) {
				return apperr.Unprocessable("invalid_grant", "unknown catalog item").With("itemId", it)
			}
			return err
		}
	}
	return nil
}

// Revoked is a connection of a user to an item that a change takes away (R10).
type Revoked struct {
	ItemID uuid.UUID `json:"itemId"`
	Users  int       `json:"users"`
}

// PutGrants replaces the grants of a group; the connections to the items its
// members lose are deleted in the same transaction (R10).
func (s *Service) PutGrants(ctx context.Context, actor Actor, id uuid.UUID, g Grants, info ModelInfo) (*Group, error) {
	cur, err := s.Group(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.AllEnabled {
		return nil, builtinErr()
	}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if err := writeGrants(ctx, tx, id, g, info); err != nil {
			return err
		}
		members, err := s.memberIDs(ctx, tx, id)
		if err != nil {
			return err
		}
		n, err := reconcile(ctx, tx, members)
		s.log(ctx, tx, actor, "group.grants", "group:"+id.String(), cur.Grants, map[string]any{"grants": g, "revokedConnections": n})
		return err
	})
	if err != nil {
		return nil, err
	}
	s.changed(ctx)
	return s.Group(ctx, id)
}

// errDryRun rolls back a transaction that only counted.
var errDryRun = errors.New("dry run")

// Impact counts the connections the grants would take away, changing nothing
// (the confirmation of design §3.2).
func (s *Service) Impact(ctx context.Context, id uuid.UUID, g Grants, info ModelInfo) ([]Revoked, error) {
	cur, err := s.Group(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.AllEnabled {
		return nil, builtinErr()
	}
	out := []Revoked{}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if err := writeGrants(ctx, tx, id, g, info); err != nil {
			return err
		}
		members, err := s.memberIDs(ctx, tx, id)
		if err != nil {
			return err
		}
		lost, err := losing(ctx, tx, members)
		if err != nil {
			return err
		}
		by := map[uuid.UUID]int{}
		for _, l := range lost {
			by[l.item]++
		}
		for item, n := range by {
			out = append(out, Revoked{ItemID: item, Users: n})
		}
		return errDryRun
	})
	if err != nil && !errors.Is(err, errDryRun) {
		return nil, err
	}
	return out, nil
}

type loss struct{ user, item uuid.UUID }

// losing lists the connections of users to items out of their access.
func losing(ctx context.Context, q Querier, users []uuid.UUID) ([]loss, error) {
	var out []loss
	for _, u := range users {
		rows, err := q.Query(ctx, `SELECT item_id FROM user_connections WHERE user_id = $1`, u)
		if err != nil {
			return nil, err
		}
		var items []uuid.UUID
		for rows.Next() {
			var it uuid.UUID
			if rows.Scan(&it) == nil {
				items = append(items, it)
			}
		}
		rows.Close()
		if len(items) == 0 {
			continue
		}
		a, err := effective(ctx, q, u)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			if !a.HasItem(it) {
				out = append(out, loss{u, it})
			}
		}
	}
	return out, nil
}

// reconcile deletes the connections of users to items out of their access:
// personal credentials do not stay without a right to use them (R10).
// The collection of communications pauses by itself without its source.
func reconcile(ctx context.Context, q Querier, users []uuid.UUID) (int, error) {
	lost, err := losing(ctx, q, users)
	if err != nil {
		return 0, err
	}
	for _, l := range lost {
		if _, err := q.Exec(ctx, `DELETE FROM user_connections WHERE user_id = $1 AND item_id = $2`, l.user, l.item); err != nil {
			return 0, err
		}
	}
	return len(lost), nil
}

// Reconcile is reconcile for the given users outside a change of a group:
// a lost role of an administrator, an unpublished item.
func (s *Service) Reconcile(ctx context.Context, users []uuid.UUID) (int, error) {
	n, err := reconcile(ctx, s.Pool, users)
	if err == nil {
		s.changed(ctx)
	}
	return n, err
}

// UserGroups sets the ordinary groups of a user (R7).
func (s *Service) UserGroups(ctx context.Context, actor Actor, uid uuid.UUID, ids []uuid.UUID) (*Access, error) {
	var before []uuid.UUID
	rows, err := s.Pool.Query(ctx, `SELECT group_id FROM user_group_members WHERE user_id = $1`, uid)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var g uuid.UUID
		if rows.Scan(&g) == nil {
			before = append(before, g)
		}
	}
	rows.Close()
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		for _, id := range ids {
			var b *string
			if err := tx.QueryRow(ctx, `SELECT builtin FROM user_groups WHERE id = $1`, id).Scan(&b); err != nil {
				if postgres.IsNoRows(err) {
					return apperr.Unprocessable("invalid_group", "unknown group").With("groupId", id)
				}
				return err
			}
			if b != nil {
				return apperr.Unprocessable("invalid_group", "a built-in group has no members to set").With("groupId", id)
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM user_group_members WHERE user_id = $1 AND NOT (group_id = ANY($2))`, uid, ids); err != nil {
			return err
		}
		for _, id := range ids {
			if _, err := tx.Exec(ctx, `INSERT INTO user_group_members (group_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, id, uid); err != nil {
				if postgres.IsForeignKeyViolation(err) {
					return apperr.NotFound("not_found", "user not found")
				}
				return err
			}
		}
		n, err := reconcile(ctx, tx, []uuid.UUID{uid})
		s.log(ctx, tx, actor, "user.groups", "user:"+uid.String(), before, map[string]any{"groups": ids, "revokedConnections": n})
		return err
	})
	if err != nil {
		return nil, err
	}
	s.changed(ctx)
	return s.Effective(ctx, uid)
}

// LogEntry is a line of the journal of the administration (R14).
type LogEntry struct {
	ID         int64           `json:"id"`
	At         time.Time       `json:"at"`
	ActorEmail string          `json:"actorEmail"`
	Action     string          `json:"action"`
	Target     string          `json:"target"`
	Before     json.RawMessage `json:"before"`
	After      json.RawMessage `json:"after"`
}

// Log lists the journal, newest first; target filters by group:<id> or user:<id>.
func (s *Service) Log(ctx context.Context, target string, before int64, limit int) ([]LogEntry, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := s.Pool.Query(ctx, `SELECT id, at, actor_email, action, target, before, after FROM admin_log
		WHERE ($1 = '' OR target = $1) AND ($2 = 0 OR id < $2) ORDER BY id DESC LIMIT $3`, target, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []LogEntry{}
	for rows.Next() {
		var e LogEntry
		var b, a []byte
		if err := rows.Scan(&e.ID, &e.At, &e.ActorEmail, &e.Action, &e.Target, &b, &a); err != nil {
			return nil, err
		}
		if len(b) > 0 {
			e.Before = b
		}
		if len(a) > 0 {
			e.After = a
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AdminChanged is called after a change of the role of administrator
// (ADM-02): what only «admins» gave is taken.
func (s *Service) AdminChanged(ctx context.Context, actor Actor, uid uuid.UUID, admin bool) {
	n := 0
	if !admin {
		n, _ = reconcile(ctx, s.Pool, []uuid.UUID{uid})
	}
	s.log(ctx, s.Pool, actor, "user.admin", "user:"+uid.String(), nil, map[string]any{"admin": admin, "revokedConnections": n})
	s.changed(ctx)
}

// ─── HTTP ───────────────────────────────────────────────────────────

// Routes mounts /groups, /users/{id}/access, /users/{id}/groups and
// /admin-log of the administration (tech §2). actor names who acts.
func (s *Service) Routes(r chi.Router, actor func(r *http.Request) Actor, info ModelInfo) {
	h := httpx.Handler
	r.Get("/groups", h(func(w http.ResponseWriter, r *http.Request) error {
		l, err := s.Groups(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Post("/groups", h(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Name        string  `json:"name"`
			Description *string `json:"description"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		g, err := s.Create(r.Context(), actor(r), in.Name, in.Description)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, g)
		return nil
	}))
	r.Put("/groups/order", h(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			IDs []uuid.UUID `json:"ids"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := s.Order(r.Context(), actor(r), in.IDs); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Get("/groups/{id}", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		g, err := s.Group(r.Context(), id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, g)
		return nil
	}))
	r.Patch("/groups/{id}", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			Name        *string `json:"name"`
			Description *string `json:"description"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		g, err := s.Update(r.Context(), actor(r), id, in.Name, in.Description)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, g)
		return nil
	}))
	r.Delete("/groups/{id}", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		if err := s.Delete(r.Context(), actor(r), id, r.URL.Query().Get("confirm")); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Get("/groups/{id}/members", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		l, err := s.Members(r.Context(), id, r.URL.Query().Get("q"), 0)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Post("/groups/{id}/members", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			UserIDs []uuid.UUID `json:"userIds"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := s.AddMembers(r.Context(), actor(r), id, in.UserIDs); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Delete("/groups/{id}/members/{user}", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		user, err := httpx.ParamUUID(r, "user")
		if err != nil {
			return err
		}
		if err := s.RemoveMember(r.Context(), actor(r), id, user); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Put("/groups/{id}/grants", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in Grants
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		g, err := s.PutGrants(r.Context(), actor(r), id, in, info)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, g)
		return nil
	}))
	r.Post("/groups/{id}/grants:impact", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in Grants
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		out, err := s.Impact(r.Context(), id, in, info)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"revokedConnections": out})
		return nil
	}))
	r.Get("/users/{id}/access", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		a, err := s.Effective(r.Context(), id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, a)
		return nil
	}))
	r.Put("/users/{id}/groups", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			GroupIDs []uuid.UUID `json:"groupIds"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		a, err := s.UserGroups(r.Context(), actor(r), id, in.GroupIDs)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, a)
		return nil
	}))
	r.Get("/admin-log", h(func(w http.ResponseWriter, r *http.Request) error {
		var before int64
		_, _ = fmt.Sscan(r.URL.Query().Get("before"), &before)
		l, err := s.Log(r.Context(), r.URL.Query().Get("target"), before, 0)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
}
