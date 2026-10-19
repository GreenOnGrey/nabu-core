// Package access is the access of users to the resources of Nabu by groups
// (FTR.NAB.CMN-0007): models, channels, MCP items and skills are granted to
// groups only; a user has everything granted to one of the groups. The
// built-in group «all» has every user, «admins» — every administrator and
// every enabled resource. The package computes the effective access in SQL
// and Go; the consumers (models, channels, catalog) get it through functions
// wired in app.
package access

import (
	"context"
	"encoding/json"
	"slices"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Built-in groups.
const (
	BuiltinAll    = "all"
	BuiltinAdmins = "admins"
)

// Kinds of grants.
const (
	KindModel   = "model"
	KindChannel = "channel"
	KindItem    = "catalog_item"
)

// Changed is the event after a change of access: the caches of channels drop it.
const Changed = "access.changed"

// Querier is a pool or a transaction.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Service computes and changes access.
type Service struct {
	Pool   *pgxpool.Pool
	Events events.Publisher
}

// GroupRef names a group of a user.
type GroupRef struct {
	ID      uuid.UUID `json:"id"`
	Name    string    `json:"name"`
	Builtin *string   `json:"builtin"`
}

// ModelGrant is a model in the access of a user.
type ModelGrant struct {
	ConnectionID uuid.UUID   `json:"connectionId"`
	Model        string      `json:"model"`
	Name         string      `json:"name"`
	Connection   string      `json:"connection"`
	Via          []uuid.UUID `json:"via"`
}

// DefaultModel is the default model of a user and where it comes from (R8).
type DefaultModel struct {
	ConnectionID uuid.UUID  `json:"connectionId"`
	Model        string     `json:"model"`
	Source       string     `json:"source"` // user | group | global | first
	GroupID      *uuid.UUID `json:"groupId,omitempty"`
}

// ChannelGrant is a channel in the access of a user.
type ChannelGrant struct {
	Kind string      `json:"kind"`
	Via  []uuid.UUID `json:"via"`
}

// ItemGrant is an item of the catalog in the access of a user.
type ItemGrant struct {
	ID    uuid.UUID   `json:"id"`
	Type  string      `json:"type"`
	Title string      `json:"title"`
	Mode  *string     `json:"mode"`
	Via   []uuid.UUID `json:"via"`
}

// Access is the effective access of a user (R5, R7).
type Access struct {
	Admin        bool           `json:"admin"`
	Groups       []GroupRef     `json:"groups"`
	Models       []ModelGrant   `json:"models"`
	DefaultModel *DefaultModel  `json:"defaultModel"`
	Channels     []ChannelGrant `json:"channels"`
	CatalogItems []ItemGrant    `json:"catalogItems"`
}

// HasModel reports whether the model is granted.
func (a *Access) HasModel(conn uuid.UUID, model string) bool {
	return slices.ContainsFunc(a.Models, func(m ModelGrant) bool { return m.ConnectionID == conn && m.Model == model })
}

// HasChannel reports whether the channel is granted (the site always is: R6).
func (a *Access) HasChannel(kind string) bool {
	return kind == "web" || slices.ContainsFunc(a.Channels, func(c ChannelGrant) bool { return c.Kind == kind })
}

// HasItem reports whether the item is granted.
func (a *Access) HasItem(id uuid.UUID) bool {
	return slices.ContainsFunc(a.CatalogItems, func(i ItemGrant) bool { return i.ID == id })
}

// usableModel is a model of an enabled connection a personal agent can run on.
type usableModel struct {
	conn       uuid.UUID
	connName   string
	id, name   string
	connCreate int // the order of connections
}

// resources are the enabled resources of the platform.
type resources struct {
	models   []usableModel
	channels []string
	items    map[uuid.UUID]ItemGrant
}

func loadResources(ctx context.Context, q Querier) (*resources, error) {
	r := &resources{items: map[uuid.UUID]ItemGrant{}}
	rows, err := q.Query(ctx, `SELECT id, name, models FROM model_connections WHERE enabled ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	n := 0
	for rows.Next() {
		var id uuid.UUID
		var name string
		var raw []byte
		if err := rows.Scan(&id, &name, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		var ms []struct {
			ID         string   `json:"id"`
			Name       string   `json:"name"`
			Tools      *bool    `json:"tools"`
			Embeddings bool     `json:"embeddings"`
			Gone       bool     `json:"gone"`
			Overrides  []string `json:"overrides"`
		}
		_ = json.Unmarshal(raw, &ms)
		for _, m := range ms {
			byName := agent.EmbeddingsName(m.ID) && !slices.Contains(m.Overrides, "tools") // stored before its kind was known
			if (m.Tools != nil && !*m.Tools) || m.Embeddings || byName || m.Gone {         // FTR.NAB.CHAT-0001 R8
				continue
			}
			nm := m.Name
			if nm == "" {
				nm = m.ID
			}
			r.models = append(r.models, usableModel{conn: id, connName: name, id: m.ID, name: nm, connCreate: n})
		}
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = q.Query(ctx, `SELECT kind FROM channels WHERE enabled AND kind <> 'web' ORDER BY kind`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return nil, err
		}
		r.channels = append(r.channels, k)
	}
	rows.Close()
	rows, err = q.Query(ctx, `SELECT id, type, COALESCE(NULLIF(title,''), name), mode FROM catalog_items WHERE published AND NOT in_development`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var it ItemGrant
		if err := rows.Scan(&it.ID, &it.Type, &it.Title, &it.Mode); err != nil {
			rows.Close()
			return nil, err
		}
		r.items[it.ID] = it
	}
	rows.Close()
	return r, rows.Err()
}

// Effective computes the access of a user (arch §3.2). uuid.Nil is nobody in
// particular: the access of «Все пользователи» alone — a group agent without
// an owner gets it (R12). A blocked user has no access, an administrator
// included.
func (s *Service) Effective(ctx context.Context, uid uuid.UUID) (*Access, error) {
	return effective(ctx, s.Pool, uid)
}

func effective(ctx context.Context, q Querier, uid uuid.UUID) (*Access, error) {
	a := &Access{Groups: []GroupRef{}, Models: []ModelGrant{}, Channels: []ChannelGrant{}, CatalogItems: []ItemGrant{}}
	if uid != uuid.Nil {
		var blocked bool
		if err := q.QueryRow(ctx, `SELECT is_admin, status = 'blocked' FROM users WHERE id = $1`, uid).Scan(&a.Admin, &blocked); err != nil {
			if postgres.IsNoRows(err) {
				return a, nil
			}
			return nil, err
		}
		if blocked {
			a.Admin = false
			return a, nil
		}
	}
	res, err := loadResources(ctx, q)
	if err != nil {
		return nil, err
	}
	// the groups of the user: «all», «admins» for an administrator, the memberships
	rows, err := q.Query(ctx, `SELECT g.id, g.name, g.builtin, g.position FROM user_groups g
		WHERE g.builtin = 'all' OR (g.builtin = 'admins' AND $2) OR g.id IN (SELECT group_id FROM user_group_members WHERE user_id = $1)
		ORDER BY g.position, lower(g.name)`, uid, a.Admin)
	if err != nil {
		return nil, err
	}
	var admins uuid.UUID
	pos := map[uuid.UUID]int{}
	var ids []uuid.UUID
	for rows.Next() {
		var g GroupRef
		var p int
		if err := rows.Scan(&g.ID, &g.Name, &g.Builtin, &p); err != nil {
			rows.Close()
			return nil, err
		}
		a.Groups = append(a.Groups, g)
		pos[g.ID] = p
		ids = append(ids, g.ID)
		if g.Builtin != nil && *g.Builtin == BuiltinAdmins {
			admins = g.ID
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	models := map[string]*ModelGrant{}
	channels := map[string]*ChannelGrant{}
	items := map[uuid.UUID]*ItemGrant{}
	var groupDefault *DefaultModel
	defPos := 0
	addModel := func(m usableModel, via uuid.UUID) {
		k := m.conn.String() + "/" + m.id
		if g, ok := models[k]; ok {
			if !slices.Contains(g.Via, via) {
				g.Via = append(g.Via, via)
			}
			return
		}
		models[k] = &ModelGrant{ConnectionID: m.conn, Model: m.id, Name: m.name, Connection: m.connName, Via: []uuid.UUID{via}}
	}
	addChannel := func(k string, via uuid.UUID) {
		if g, ok := channels[k]; ok {
			if !slices.Contains(g.Via, via) {
				g.Via = append(g.Via, via)
			}
			return
		}
		channels[k] = &ChannelGrant{Kind: k, Via: []uuid.UUID{via}}
	}
	addItem := func(it ItemGrant, via uuid.UUID) {
		if g, ok := items[it.ID]; ok {
			if !slices.Contains(g.Via, via) {
				g.Via = append(g.Via, via)
			}
			return
		}
		it.Via = []uuid.UUID{via}
		items[it.ID] = &it
	}
	// R3: «admins» has every enabled resource
	if a.Admin && admins != uuid.Nil {
		for _, m := range res.models {
			addModel(m, admins)
		}
		for _, c := range res.channels {
			addChannel(c, admins)
		}
		for _, it := range res.items {
			addItem(it, admins)
		}
	}
	if len(ids) > 0 {
		rows, err = q.Query(ctx, `SELECT group_id, kind, connection_id, model, channel, item_id, is_default FROM access_grants WHERE group_id = ANY($1)`, ids)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var gid uuid.UUID
			var kind string
			var conn, item *uuid.UUID
			var model, channel *string
			var isDefault bool
			if err := rows.Scan(&gid, &kind, &conn, &model, &channel, &item, &isDefault); err != nil {
				rows.Close()
				return nil, err
			}
			switch kind {
			case KindModel:
				for _, m := range res.models {
					if m.conn == *conn && m.id == *model {
						addModel(m, gid)
						if isDefault && (groupDefault == nil || pos[gid] < defPos) {
							g := gid
							groupDefault, defPos = &DefaultModel{ConnectionID: m.conn, Model: m.id, Source: "group", GroupID: &g}, pos[gid]
						}
					}
				}
			case KindChannel:
				if slices.Contains(res.channels, *channel) {
					addChannel(*channel, gid)
				}
			case KindItem:
				if it, ok := res.items[*item]; ok {
					addItem(it, gid)
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	for _, m := range res.models { // the order of connections and of their models
		if g, ok := models[m.conn.String()+"/"+m.id]; ok {
			a.Models = append(a.Models, *g)
		}
	}
	for _, c := range res.channels {
		if g, ok := channels[c]; ok {
			a.Channels = append(a.Channels, *g)
		}
	}
	for _, g := range items {
		a.CatalogItems = append(a.CatalogItems, *g)
	}
	sort.Slice(a.CatalogItems, func(i, j int) bool { return a.CatalogItems[i].Title < a.CatalogItems[j].Title })
	a.DefaultModel = defaultModel(ctx, q, uid, a, groupDefault)
	return a, nil
}

// defaultModel is the rule of R8: the choice of the user, the default of the
// group higher in the order, the global default, the first model by name.
func defaultModel(ctx context.Context, q Querier, uid uuid.UUID, a *Access, group *DefaultModel) *DefaultModel {
	if len(a.Models) == 0 {
		return nil
	}
	var conn *uuid.UUID
	var model *string
	_ = q.QueryRow(ctx, `SELECT model_connection_id, model FROM agent_profiles WHERE user_id = $1`, uid).Scan(&conn, &model)
	if conn != nil && model != nil && a.HasModel(*conn, *model) {
		return &DefaultModel{ConnectionID: *conn, Model: *model, Source: "user"}
	}
	if group != nil {
		return group
	}
	var raw []byte
	if q.QueryRow(ctx, `SELECT value FROM settings WHERE key = 'personal_models'`).Scan(&raw) == nil {
		var pm struct {
			Default *struct {
				ConnectionID uuid.UUID `json:"connectionId"`
				Model        string    `json:"model"`
			} `json:"default"`
		}
		if json.Unmarshal(raw, &pm) == nil && pm.Default != nil && a.HasModel(pm.Default.ConnectionID, pm.Default.Model) {
			return &DefaultModel{ConnectionID: pm.Default.ConnectionID, Model: pm.Default.Model, Source: "global"}
		}
	}
	first := a.Models[0]
	for _, m := range a.Models[1:] {
		if m.Name < first.Name {
			first = m
		}
	}
	return &DefaultModel{ConnectionID: first.ConnectionID, Model: first.Model, Source: "first"}
}

// ItemGranted reports whether the item is in the access of the user, without
// computing the rest (the proxy of MCP asks on every call: R10). As in
// Effective, uuid.Nil asks for «Все пользователи» alone and a blocked or
// unknown user has nothing.
func (s *Service) ItemGranted(ctx context.Context, uid, item uuid.UUID) bool {
	var ok bool
	_ = s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM catalog_items c WHERE c.id = $2 AND c.published AND NOT c.in_development
		AND ($1 = '00000000-0000-0000-0000-000000000000'::uuid OR EXISTS (SELECT 1 FROM users WHERE id = $1 AND status <> 'blocked'))
		AND (
		COALESCE((SELECT is_admin FROM users WHERE id = $1), false) OR EXISTS (
			SELECT 1 FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
			WHERE ag.kind = 'catalog_item' AND ag.item_id = $2
			  AND (g.builtin = 'all' OR g.id IN (SELECT group_id FROM user_group_members WHERE user_id = $1)))))`, uid, item).Scan(&ok)
	return ok
}

// Items returns the granted items of the user as a set.
func (s *Service) Items(ctx context.Context, uid uuid.UUID) (map[uuid.UUID]bool, error) {
	a, err := s.Effective(ctx, uid)
	if err != nil {
		return nil, err
	}
	out := map[uuid.UUID]bool{}
	for _, it := range a.CatalogItems {
		out[it.ID] = true
	}
	return out, nil
}

// Channels returns the granted channels of the user as a set (the site included).
func (s *Service) Channels(ctx context.Context, uid uuid.UUID) (map[string]bool, error) {
	a, err := s.Effective(ctx, uid)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{"web": true}
	for _, c := range a.Channels {
		out[c.Kind] = true
	}
	return out, nil
}

func (s *Service) changed(ctx context.Context) {
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: Changed, Data: map[string]any{}})
	}
}
