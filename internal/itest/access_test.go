//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
)

func builtinGroup(t *testing.T, kind string) uuid.UUID {
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(), `SELECT id FROM user_groups WHERE builtin = $1`, kind).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// FTR.NAB.CMN-0007: ADM-01…ADM-04, GRP-01…GRP-08, ACC-01…ACC-05, RES-01, RES-03, RES-05, RES-07, RES-10, RES-11, OLD-01…OLD-03
func TestAccessGroups(t *testing.T) {
	ctx := context.Background()
	acc := &access.Service{Pool: pool}
	actor := access.Actor{Email: "root@x.org"}
	box, _ := crypto.NewBox(key)
	ms := &models.Service{Pool: pool, Box: box, Access: acc.Effective}
	no := false
	conn, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Access " + uuid.NewString()[:6], BaseURL: "http://llm.invalid",
		APIKey: "sk-access-1", Models: []models.ModelRef{{ID: "x-1"}, {ID: "x-2"}, {ID: "no-tools", Overrides: &models.CapsPatch{Tools: &no}}}})
	if err != nil {
		t.Fatal(err)
	}
	x1 := access.Choice{ConnectionID: conn.ID, Model: "x-1"}
	x2 := access.Choice{ConnectionID: conn.ID, Model: "x-2"}
	_, _ = pool.Exec(ctx, `UPDATE channels SET enabled = true WHERE kind IN ('telegram','vkteams')`)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `UPDATE channels SET enabled = false WHERE kind IN ('telegram','vkteams')`)
	})
	signer := jwt.NewSigner("https://nabu.test", key)
	cat := &catalog.Service{Pool: pool, Box: box, Signer: signer, Access: acc}
	mode, yes := "personal", true
	item, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "mail-" + uuid.NewString()[:6], Source: &catalog.Source{Kind: "url", URL: "http://mail.invalid/mcp"},
		Mode: &mode, Published: &yes, PersonalAuth: &catalog.PersonalAuth{Kind: "token"}, ReadOnly: ptr(true)})
	if err != nil {
		t.Fatal(err)
	}
	u := newUser(t, "acc-"+uuid.NewString()[:6]+"@x.org")

	// GRP-01
	name := "Бухгалтерия " + uuid.NewString()[:4]
	a, err := acc.Create(ctx, actor, name, ptr("Учёт"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acc.Create(ctx, actor, strings.ToLower(name), nil); !isCodeErr(err, "group_name_taken") {
		t.Fatalf("GRP-01: %v", err)
	}
	// GRP-03, GRP-04, GRP-05
	if _, err := acc.PutGrants(ctx, actor, a.ID, access.Grants{Models: []access.Choice{{ConnectionID: conn.ID, Model: "no-tools"}}}, ms.ModelInfo); !isCodeErr(err, "model_no_tools") {
		t.Fatalf("GRP-03: %v", err)
	}
	if _, err := acc.PutGrants(ctx, actor, a.ID, access.Grants{Channels: []string{"web"}}, ms.ModelInfo); !isCodeErr(err, "invalid_grant") {
		t.Fatalf("GRP-04: %v", err)
	}
	if _, err := acc.PutGrants(ctx, actor, a.ID, access.Grants{Models: []access.Choice{x1}, DefaultModel: &x2}, ms.ModelInfo); !isCodeErr(err, "invalid_grant") {
		t.Fatalf("GRP-05: %v", err)
	}

	// RES-03: no model granted
	if _, _, err := ms.ResolveUser(ctx, u, nil, nil); !isCodeErr(err, "no_models_granted") {
		t.Fatalf("RES-03: %v", err)
	}
	// RES-07 (part): an item out of the access cannot be connected
	if _, err := cat.Connect(ctx, u, item.ID, "token-of-u"); !isCodeErr(err, "access_denied_item") {
		t.Fatalf("connect without access: %v", err)
	}

	// ACC-01: the access of two groups
	if _, err := acc.PutGrants(ctx, actor, a.ID, access.Grants{Models: []access.Choice{x1, x2}, DefaultModel: &x1, Channels: []string{"telegram"},
		CatalogItems: []uuid.UUID{item.ID}}, ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	b, _ := acc.Create(ctx, actor, "Юристы "+uuid.NewString()[:4], nil)
	if _, err := acc.PutGrants(ctx, actor, b.ID, access.Grants{Models: []access.Choice{x2}, DefaultModel: &x2, CatalogItems: []uuid.UUID{item.ID}}, ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	if err := acc.AddMembers(ctx, actor, a.ID, []uuid.UUID{u}); err != nil {
		t.Fatal(err)
	}
	if err := acc.AddMembers(ctx, actor, b.ID, []uuid.UUID{u}); err != nil {
		t.Fatal(err)
	}
	ea, err := acc.Effective(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	via := func(c access.Choice) []uuid.UUID {
		for _, m := range ea.Models {
			if m.ConnectionID == c.ConnectionID && m.Model == c.Model {
				return m.Via
			}
		}
		return nil
	}
	if len(via(x1)) != 1 || len(via(x2)) != 2 || !ea.HasChannel("telegram") || ea.HasChannel("vkteams") || !ea.HasItem(item.ID) || ea.Admin {
		t.Fatalf("ACC-01: %+v", ea)
	}
	// RES-01: the default of the group higher in the order
	if err := acc.Order(ctx, actor, orderWith(t, a.ID, b.ID)); err != nil {
		t.Fatal(err)
	}
	if spec, _, err := ms.ResolveUser(ctx, u, nil, nil); err != nil || spec.ModelID != "x-1" {
		t.Fatalf("RES-01 (A first): %+v %v", spec, err)
	}
	if err := acc.Order(ctx, actor, orderWith(t, b.ID, a.ID)); err != nil {
		t.Fatal(err)
	}
	if spec, _, err := ms.ResolveUser(ctx, u, nil, nil); err != nil || spec.ModelID != "x-2" {
		t.Fatalf("RES-01 (B first): %+v %v", spec, err)
	}
	// GRP-07
	if err := acc.Order(ctx, actor, []uuid.UUID{a.ID}); !isCodeErr(err, "invalid_order") {
		t.Fatalf("GRP-07: %v", err)
	}
	// RES-02: the user's choice, then taken away
	_ = repo().SetAgent(ctx, u, nil, nil, &conn.ID, ptr("x-1"))
	if spec, _, _ := ms.ResolveUser(ctx, u, nil, nil); spec.ModelID != "x-1" {
		t.Fatalf("the choice of the user: %s", spec.ModelID)
	}
	avail, _ := ms.AvailableFor(ctx, u)
	if len(avail) != 2 {
		t.Fatalf("AvailableFor: %+v", avail)
	}

	// RES-05: the registry grants channels by groups
	reg := &channels.Registry{Pool: pool, Granted: acc.Channels}
	if ok, why, _ := reg.Available(ctx, u, "telegram"); !ok || why != channels.ReasonGroup {
		t.Fatalf("RES-05 telegram: %v %s", ok, why)
	}
	if ok, _, _ := reg.Available(ctx, u, "vkteams"); ok {
		t.Fatal("RES-05: vkteams is not granted")
	}
	if ok, _, _ := reg.Available(ctx, u, "web"); !ok {
		t.Fatal("ACC-03: the site is always open")
	}
	// OLD-01
	if err := reg.SetUserChannels(ctx, u, map[string]bool{"vkteams": true}); !isCodeErr(err, "use_groups") {
		t.Fatalf("OLD-01: %v", err)
	}

	// RES-07, RES-10, RES-11: connections and the taking of access
	if _, err := cat.Connect(ctx, u, item.ID, "token-of-u"); err != nil {
		t.Fatal(err)
	}
	imp, err := acc.Impact(ctx, a.ID, access.Grants{Models: []access.Choice{x1, x2}, DefaultModel: &x1}, ms.ModelInfo)
	if err != nil || len(imp) != 0 {
		t.Fatalf("RES-10: the item stays through B: %+v %v", imp, err)
	}
	if _, err := acc.PutGrants(ctx, actor, a.ID, access.Grants{Models: []access.Choice{x1, x2}, DefaultModel: &x1, Channels: []string{"telegram"}}, ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	if !connected(t, u, item.ID) {
		t.Fatal("RES-10: the connection is deleted though B grants the item")
	}
	imp, err = acc.Impact(ctx, b.ID, access.Grants{Models: []access.Choice{x2}}, ms.ModelInfo)
	if err != nil || len(imp) != 1 || imp[0].Users != 1 || !connected(t, u, item.ID) {
		t.Fatalf("RES-11: %+v %v", imp, err)
	}
	if _, err := acc.PutGrants(ctx, actor, b.ID, access.Grants{Models: []access.Choice{x2}, DefaultModel: &x2}, ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	if connected(t, u, item.ID) || acc.ItemGranted(ctx, u, item.ID) {
		t.Fatal("RES-07, RES-08: the connection and the access stay after the item was taken")
	}
	if l, _ := cat.ForUser(ctx, u); containsItem(l, item.ID) {
		t.Fatal("RES-07: the item is shown in Connections")
	}

	// ACC-05, GRP-06, GRP-08
	if _, err := acc.UserGroups(ctx, actor, u, []uuid.UUID{builtinGroup(t, access.BuiltinAll)}); !isCodeErr(err, "invalid_group") {
		t.Fatalf("ACC-05: %v", err)
	}
	if err := acc.Delete(ctx, actor, b.ID, "wrong"); !isCodeErr(err, "confirm_required") {
		t.Fatalf("GRP-06: %v", err)
	}
	if err := acc.Delete(ctx, actor, b.ID, b.ID.String()); err != nil {
		t.Fatal(err)
	}
	if l, _ := acc.Log(ctx, "group:"+b.ID.String(), httpx.Page{Limit: 100}); len(l.Items) < 3 {
		t.Fatalf("GRP-08: %d entries", len(l.Items))
	}
	if l, _ := acc.Log(ctx, "group:"+b.ID.String(), httpx.Page{Limit: 2}); len(l.Items) != 2 || l.NextCursor == nil {
		t.Fatalf("GRP-08 pages: %+v", l)
	} else if c, err := httpx.ParsePage(httptest.NewRequest("GET", "/?cursor="+*l.NextCursor, nil)); err != nil {
		t.Fatal(err)
	} else if next, _ := acc.Log(ctx, "group:"+b.ID.String(), c); len(next.Items) == 0 || next.Items[0].ID >= l.Items[1].ID {
		t.Fatalf("GRP-08 next page: %+v", next)
	}

	// ADM-04: built-in groups
	all, admins := builtinGroup(t, access.BuiltinAll), builtinGroup(t, access.BuiltinAdmins)
	if err := acc.AddMembers(ctx, actor, all, []uuid.UUID{u}); !isCodeErr(err, "group_builtin") {
		t.Fatalf("ADM-04 members: %v", err)
	}
	if _, err := acc.PutGrants(ctx, actor, admins, access.Grants{}, ms.ModelInfo); !isCodeErr(err, "group_builtin") {
		t.Fatalf("ADM-04 grants of admins: %v", err)
	}
	if _, err := acc.Update(ctx, actor, all, ptr("Everybody"), nil); !isCodeErr(err, "group_builtin") {
		t.Fatalf("ADM-04 rename: %v", err)
	}
	if err := acc.Delete(ctx, actor, all, all.String()); !isCodeErr(err, "group_builtin") {
		t.Fatalf("ADM-04 delete: %v", err)
	}

	// ADM-01, ADM-02: an administrator has everything enabled; the role goes, the access goes
	_, _ = pool.Exec(ctx, `UPDATE users SET is_admin = true WHERE id = $1`, u)
	if !acc.ItemGranted(ctx, u, item.ID) {
		t.Fatal("ADM-01: an administrator has every published item")
	}
	if _, err := cat.Connect(ctx, u, item.ID, "token-of-u"); err != nil {
		t.Fatal(err)
	}
	ea, _ = acc.Effective(ctx, u)
	if !ea.Admin || !ea.HasChannel("vkteams") || !ea.HasModel(conn.ID, "x-2") {
		t.Fatalf("ADM-01: %+v", ea)
	}
	// ADM-03: the last administrator keeps the role
	var others []uuid.UUID
	rows, _ := pool.Query(ctx, `SELECT id FROM users WHERE is_admin AND status = 'active' AND id <> $1`, u)
	for rows.Next() {
		var id uuid.UUID
		_ = rows.Scan(&id)
		others = append(others, id)
	}
	rows.Close()
	_, _ = pool.Exec(ctx, `UPDATE users SET is_admin = false WHERE id = ANY($1)`, others)
	_, _ = pool.Exec(ctx, `UPDATE users SET status = 'active' WHERE id = $1`, u)
	boss := newUser(t, "boss-"+uuid.NewString()[:6]+"@x.org")
	if _, err := repo().AdminPatch(ctx, boss, u, ptr(false), nil); !isCodeErr(err, "last_admin") {
		t.Fatalf("ADM-03: %v", err)
	}
	_, _ = pool.Exec(ctx, `UPDATE users SET is_admin = true WHERE id = $1`, boss)
	// tech §4: the access goes in the transaction of the role — a failure keeps both
	failing := repo()
	failing.RoleChanged = func(context.Context, pgx.Tx, uuid.UUID, uuid.UUID, bool) error { return errors.New("reconcile failed") }
	if _, err := failing.AdminPatch(ctx, boss, u, ptr(false), nil); err == nil {
		t.Fatal("ADM-02: the role changed although the access did not")
	}
	if ea, _ := acc.Effective(ctx, u); !ea.Admin || !connected(t, u, item.ID) {
		t.Fatalf("ADM-02: a failed change of the role left %+v", ea)
	}
	roles := repo()
	roles.RoleChanged = func(ctx context.Context, tx pgx.Tx, by, uid uuid.UUID, admin bool) error {
		return acc.AdminChanged(ctx, tx, access.Actor{ID: by}, uid, admin)
	}
	if _, err := roles.AdminPatch(ctx, boss, u, ptr(false), nil); err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `UPDATE users SET is_admin = true WHERE id = ANY($1)`, others)
	_, _ = pool.Exec(ctx, `UPDATE users SET is_admin = false WHERE id = $1`, boss)
	if connected(t, u, item.ID) {
		t.Fatal("ADM-02: the connection of an item only «admins» gave stays")
	}

	// OLD-02, OLD-03: the personal models of the administration
	allID := builtinGroup(t, access.BuiltinAll)
	_, _ = pool.Exec(ctx, `INSERT INTO access_grants (group_id, kind, connection_id, model) VALUES ($1,'model',$2,'x-2') ON CONFLICT DO NOTHING`, allID, conn.ID)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM access_grants WHERE group_id = $1 AND connection_id = $2`, allID, conn.ID)
	})
	r := chi.NewRouter()
	ms.AdminRoutes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/personal-models")
	if err != nil {
		t.Fatal(err)
	}
	var view models.PersonalModels
	_ = jsonDecode(resp, &view)
	if !containsChoice(view.Available, conn.ID, "x-2") {
		t.Fatalf("OLD-03: %+v", view)
	}
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/personal-models", strings.NewReader(`{"default":null,"available":[]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("OLD-02: %v %v", resp.StatusCode, err)
	}
	resp.Body.Close()
}

// MIG-01…MIG-05: the migration gives «all» what everybody had (run in a
// transaction that is rolled back).
func TestAccessMigration(t *testing.T) {
	ctx := context.Background()
	mig, err := os.ReadFile("../../migrations/0007_access_groups.sql")
	if err != nil {
		t.Fatal(err)
	}
	up := string(mig)
	from := strings.Index(up, "INSERT INTO access_grants")
	to := strings.Index(up, "-- +goose Down")
	if from < 0 || to < from {
		t.Fatal("no inserts of «all» in the migration")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	box, _ := crypto.NewBox(key)
	var connID uuid.UUID
	enc, _ := box.Seal("sk-mig-0001")
	if err := tx.QueryRow(ctx, `INSERT INTO model_connections (name, type, base_url, api_key_enc, models) VALUES ($1,'deepseek','https://api.deepseek.com',$2,
		'[{"id":"deepseek-v4-pro","tools":true},{"id":"deepseek-v4.1-flash","tools":true}]') RETURNING id`, "Mig "+uuid.NewString()[:6], enc).Scan(&connID); err != nil {
		t.Fatal(err)
	}
	all := builtinGroup(t, access.BuiltinAll)
	for _, q := range []string{
		`DELETE FROM access_grants WHERE group_id = '` + all.String() + `'`,
		`INSERT INTO settings (key, value) VALUES ('personal_models', '{"default":{"connectionId":"` + connID.String() + `","model":"deepseek-v4.1-flash"},
			"available":[{"connectionId":"` + connID.String() + `","model":"deepseek-v4.1-flash"},{"connectionId":"` + connID.String() + `","model":"deepseek-v4-pro"},
			{"connectionId":"` + uuid.NewString() + `","model":"gone"}]}') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		`UPDATE channels SET all_users = (kind IN ('web','telegram'))`,
		`INSERT INTO channel_user_access (channel, user_id) SELECT 'vkteams', id FROM users LIMIT 1 ON CONFLICT DO NOTHING`,
	} {
		if _, err := tx.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, up[from:to]); err != nil {
		t.Fatalf("the inserts of the migration: %v", err)
	}
	var models, defaults, chans, webs int
	_ = tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE kind = 'model'), count(*) FILTER (WHERE is_default), count(*) FILTER (WHERE kind = 'channel'),
		count(*) FILTER (WHERE channel = 'web') FROM access_grants WHERE group_id = $1`, all).Scan(&models, &defaults, &chans, &webs)
	// MIG-02: the model of a deleted connection is skipped, the migration passes
	if models != 2 || defaults != 1 || chans != 1 || webs != 0 {
		t.Fatalf("MIG-01, MIG-02: models %d defaults %d channels %d web %d", models, defaults, chans, webs)
	}
	// MIG-03: the personal grant of VK Teams is not carried over
	var kinds []string
	rows, _ := tx.Query(ctx, `SELECT channel FROM access_grants WHERE group_id = $1 AND kind = 'channel'`, all)
	for rows.Next() {
		var k string
		_ = rows.Scan(&k)
		kinds = append(kinds, k)
	}
	rows.Close()
	if strings.Join(kinds, ",") != "telegram" {
		t.Fatalf("MIG-03: %v", kinds)
	}
	// MIG-04: a user without personal grants has the same access before and after
	var before, after []string
	for _, q := range []struct {
		sql string
		out *[]string
	}{
		{`SELECT a->>'model' FROM settings s, jsonb_array_elements(s.value->'available') a WHERE s.key = 'personal_models'
			AND EXISTS (SELECT 1 FROM model_connections c WHERE c.id = (a->>'connectionId')::uuid)
			UNION ALL SELECT 'channel:' || kind FROM channels WHERE all_users AND kind <> 'web' ORDER BY 1`, &before},
		{`SELECT COALESCE(model, 'channel:' || channel) FROM access_grants WHERE group_id = '` + all.String() + `' AND kind IN ('model','channel') ORDER BY 1`, &after},
	} {
		rows, err := tx.Query(ctx, q.sql)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var v string
			_ = rows.Scan(&v)
			*q.out = append(*q.out, v)
		}
		rows.Close()
	}
	if strings.Join(before, ",") != strings.Join(after, ",") {
		t.Fatalf("MIG-04: %v → %v", before, after)
	}
	// MIG-05: Down, then Up — the schema and the built-in groups are back
	down := up[to+len("-- +goose Down"):]
	upAll := up[strings.Index(up, "-- +goose Up")+len("-- +goose Up") : to]
	if _, err := tx.Exec(ctx, down); err != nil {
		t.Fatalf("MIG-05 down: %v", err)
	}
	if _, err := tx.Exec(ctx, upAll); err != nil {
		t.Fatalf("MIG-05 up: %v", err)
	}
	var builtins int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM user_groups WHERE builtin IN ('all','admins')`).Scan(&builtins); err != nil || builtins != 2 {
		t.Fatalf("MIG-05: %d %v", builtins, err)
	}
}

func orderWith(t *testing.T, first ...uuid.UUID) []uuid.UUID {
	rows, err := pool.Query(context.Background(), `SELECT id FROM user_groups WHERE builtin IS NULL ORDER BY position`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := append([]uuid.UUID{}, first...)
	for rows.Next() {
		var id uuid.UUID
		_ = rows.Scan(&id)
		if !containsID(first, id) {
			out = append(out, id)
		}
	}
	return out
}

func containsID(l []uuid.UUID, id uuid.UUID) bool {
	for _, x := range l {
		if x == id {
			return true
		}
	}
	return false
}

func connected(t *testing.T, uid, item uuid.UUID) bool {
	var ok bool
	_ = pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM user_connections WHERE user_id = $1 AND item_id = $2)`, uid, item).Scan(&ok)
	return ok
}

func containsItem(l []catalog.UserItem, id uuid.UUID) bool {
	for _, x := range l {
		if x.ID == id {
			return true
		}
	}
	return false
}

func containsChoice(l []models.Choice, conn uuid.UUID, model string) bool {
	for _, c := range l {
		if c.ConnectionID == conn && c.Model == model {
			return true
		}
	}
	return false
}

func jsonDecode(resp *http.Response, v any) error {
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}
