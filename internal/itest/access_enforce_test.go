//go:build integration

package itest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/auth"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/groups"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
)

// accessRig is a connection of models, a platform MCP server and the access
// service of a test.
type accessRig struct {
	t      *testing.T
	acc    *access.Service
	ms     *models.Service
	cat    *catalog.Service
	conn   *models.Connection
	item   *catalog.Item
	proxy  *httptest.Server
	actor  access.Actor
	cancel func()
}

func newAccessRig(t *testing.T) *accessRig {
	ctx := context.Background()
	r := &accessRig{t: t, acc: &access.Service{Pool: pool}, actor: access.Actor{Email: "root@x.org"}}
	box, _ := crypto.NewBox(key)
	r.ms = &models.Service{Pool: pool, Box: box, Access: r.acc.Effective}
	var err error
	if r.conn, err = r.ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Enforce " + uuid.NewString()[:6], BaseURL: "http://llm.invalid",
		APIKey: "sk-enforce-1", Models: []models.ModelRef{{ID: "m-1"}, {ID: "m-2"}}}); err != nil {
		t.Fatal(err)
	}
	up := httptest.NewServer(&fakeMCP{tools: []map[string]any{{"name": "echo"}}})
	r.cat = &catalog.Service{Pool: pool, Box: box, Signer: jwt.NewSigner("https://nabu.test", key), InternalURL: "http://api:8081", Access: r.acc, S3: &mem{m: map[string][]byte{}}}
	r.cat.CheckMCP = func(context.Context, agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
		return &agent.MCPCheckResponse{OK: true, Tools: []agent.MCPTool{{Name: "echo"}}}, nil
	}
	mode := "platform"
	if r.item, err = r.cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "enforce-" + uuid.NewString()[:6], Source: &catalog.Source{Kind: "url", URL: up.URL},
		Mode: &mode, Published: ptr(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.cat.Check(ctx, r.item.ID, "", uuid.Nil); err != nil {
		t.Fatal(err)
	}
	if r.item, err = r.cat.Save(ctx, &r.item.ID, catalog.Input{AllowedTools: &[]string{"echo"}}); err != nil {
		t.Fatal(err)
	}
	router := chi.NewRouter()
	r.cat.ProxyRoutes(router, func(ctx context.Context, uid uuid.UUID) (string, bool) { return "u@x.org", repo().Allowed(ctx, uid) }, nil)
	r.proxy = httptest.NewServer(router)
	t.Cleanup(func() {
		r.proxy.Close()
		up.Close()
		// the item must not stay granted to «all» for other tests
		_, _ = pool.Exec(ctx, `DELETE FROM access_grants WHERE item_id = $1`, r.item.ID)
	})
	return r
}

// call calls the tool echo through the proxy with a token: the HTTP status.
func (r *accessRig) call(tok string) int {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": "echo", "arguments": map[string]any{}}})
	req, _ := http.NewRequest(http.MethodPost, r.proxy.URL+"/internal/v1/mcp-proxy/"+r.item.ID.String(), bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func (r *accessRig) group(grants access.Grants, members ...uuid.UUID) uuid.UUID {
	ctx := context.Background()
	g, err := r.acc.Create(ctx, r.actor, "Enforce "+uuid.NewString()[:6], nil)
	if err != nil {
		r.t.Fatal(err)
	}
	if _, err := r.acc.PutGrants(ctx, r.actor, g.ID, grants, r.ms.ModelInfo); err != nil {
		r.t.Fatal(err)
	}
	if len(members) > 0 {
		if err := r.acc.AddMembers(ctx, r.actor, g.ID, members); err != nil {
			r.t.Fatal(err)
		}
	}
	r.t.Cleanup(func() { _ = r.acc.Delete(context.Background(), r.actor, g.ID, g.ID.String()) })
	return g.ID
}

// groupAgent inserts a group agent of a Telegram chat with an owner (nil — none).
func groupAgent(t *testing.T, owner *uuid.UUID, skills ...string) uuid.UUID {
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(), `INSERT INTO group_agents (channel, external_chat_id, owner_id, skills) VALUES ('telegram', $1, $2, $3) RETURNING id`,
		"-100"+uuid.NewString()[:8], owner, append([]string{}, skills...)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// FTR.NAB.CMN-0007: RES-08, RES-09, RES-13, RES-14 and the blocked users
// (R10, R12): every call of a tool is checked against the access as it is now.
func TestAccessToolCalls(t *testing.T) {
	ctx := context.Background()
	r := newAccessRig(t)
	owner, member := newUser(t, "enf-owner-"+uuid.NewString()[:6]+"@x.org"), newUser(t, "enf-member-"+uuid.NewString()[:6]+"@x.org")
	m1 := access.Choice{ConnectionID: r.conn.ID, Model: "m-1"}
	g := r.group(access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1, CatalogItems: []uuid.UUID{r.item.ID}}, owner, member)
	if _, err := r.cat.Connect(ctx, member, r.item.ID, ""); err != nil {
		t.Fatal(err)
	}
	ga := groupAgent(t, &owner)
	orphan := groupAgent(t, nil)
	userTok := r.cat.ProxyToken(member, r.item.ID, "web", "", time.Hour)
	groupTok := r.cat.ProxyToken(uuid.Nil, r.item.ID, "group:"+ga.String(), "", time.Hour)
	orphanTok := r.cat.ProxyToken(uuid.Nil, r.item.ID, "group:"+orphan.String(), "", time.Hour)
	runTok := r.cat.Signer.Issue(jwt.Claims{Audience: jwt.AudMCP, Subject: "proxy:" + r.item.ID.String(), Run: "run-1", Channel: "run"}, time.Hour)
	if s := r.call(userTok); s != 200 {
		t.Fatalf("the member calls: %d", s)
	}
	if s := r.call(groupTok); s != 200 {
		t.Fatalf("RES-13: the group agent of the owner calls: %d", s)
	}
	if s := r.call(orphanTok); s != 403 {
		t.Fatalf("R12: a group agent without an owner has only «all»: %d", s)
	}
	// RES-13: the session of the group agent is built from the owner's access
	if sm, err := r.cat.ForGroup(ctx, ga, nil, "", time.Hour, owner); err != nil || !slices.ContainsFunc(sm.Servers, func(s agent.MCPServer) bool { return s.Name == r.item.Name }) {
		t.Fatalf("RES-13: %+v %v", sm, err)
	}
	if sm, _ := r.cat.ForGroup(ctx, orphan, nil, "", time.Hour, uuid.Nil); slices.ContainsFunc(sm.Servers, func(s agent.MCPServer) bool { return s.Name == r.item.Name }) {
		t.Fatal("R12: a group agent without an owner got an item «all» does not have")
	}

	// RES-08, RES-13: the item is taken from the group — the calls of a running turn are refused at once
	if _, err := r.acc.PutGrants(ctx, r.actor, g, access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1}, r.ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	if s := r.call(userTok); s != 403 {
		t.Fatalf("RES-08: %d", s)
	}
	if s := r.call(groupTok); s != 403 {
		t.Fatalf("RES-13: the group agent kept the item of its owner: %d", s)
	}
	// RES-14: a service agent has the items of its configuration, groups aside
	if s := r.call(runTok); s != 200 {
		t.Fatalf("RES-14: %d", s)
	}
	// RES-09: the collection from the item stops with source_access (decision 7: the collector pauses)
	if _, err := r.cat.CallSource(ctx, member, r.item.ID, "echo", json.RawMessage(`{}`)); !errors.Is(err, catalog.ErrSourceAccess) {
		t.Fatalf("RES-09: %v", err)
	}

	// R12: «all» is the access of a group agent without an owner
	all := builtinGroup(t, access.BuiltinAll)
	if _, err := pool.Exec(ctx, `INSERT INTO access_grants (group_id, kind, item_id) VALUES ($1, 'catalog_item', $2)`, all, r.item.ID); err != nil {
		t.Fatal(err)
	}
	if s := r.call(orphanTok); s != 200 {
		t.Fatalf("R12: «all» grants the item: %d", s)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM access_grants WHERE group_id = $1 AND item_id = $2`, all, r.item.ID); err != nil {
		t.Fatal(err)
	}

	// a blocked administrator has nothing, and its tokens stop working
	boss := newUser(t, "enf-boss-"+uuid.NewString()[:6]+"@x.org")
	_, _ = pool.Exec(ctx, `UPDATE users SET is_admin = true WHERE id = $1`, boss)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE users SET is_admin = false, status = 'active' WHERE id = $1`, boss)
	})
	if !r.acc.ItemGranted(ctx, boss, r.item.ID) {
		t.Fatal("ADM-01: an administrator has the item")
	}
	_, _ = pool.Exec(ctx, `UPDATE users SET status = 'blocked' WHERE id = $1`, boss)
	if ea, _ := r.acc.Effective(ctx, boss); ea.Admin || len(ea.Models) != 0 || len(ea.CatalogItems) != 0 || r.acc.ItemGranted(ctx, boss, r.item.ID) {
		t.Fatalf("a blocked administrator: %+v", ea)
	}
	if r.acc.ItemGranted(ctx, uuid.New(), r.item.ID) {
		t.Fatal("an unknown user has an item")
	}
}

// FTR.NAB.CMN-0007: RES-02, RES-04, RES-12, RES-13, SEC-02 (R8, R11, R12);
// FTR.NAB.CHAT-0001 SEC-02:
// models and skills of users and group agents.
func TestAccessModelsAndSkills(t *testing.T) {
	ctx := context.Background()
	r := newAccessRig(t)
	u := newUser(t, "enf-models-"+uuid.NewString()[:6]+"@x.org")
	m1, m2 := access.Choice{ConnectionID: r.conn.ID, Model: "m-1"}, access.Choice{ConnectionID: r.conn.ID, Model: "m-2"}
	g := r.group(access.Grants{Models: []access.Choice{m1, m2}, DefaultModel: &m1}, u)
	api := &chat.API{Store: &chat.Store{Pool: pool}, Pool: pool, Users: repo(), Models: r.ms, Events: &capture{}}
	if _, err := api.PatchAgent(ctx, u, nil, nil, &models.Choice{ConnectionID: r.conn.ID, Model: "m-2"}); err != nil {
		t.Fatal(err)
	}
	// RES-02: the choice is taken away — the turn goes to the default, the menu shows it
	if _, err := r.acc.PutGrants(ctx, r.actor, g, access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1}, r.ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	if spec, _, err := r.ms.ResolveUser(ctx, u, nil, nil); err != nil || spec.ModelID != "m-1" {
		t.Fatalf("RES-02: %+v %v", spec, err)
	}
	ag, _ := repo().GetAgent(ctx, u)
	if spec, _, _ := r.ms.ResolveUser(ctx, u, ag.ConnectionID, ag.Model); spec.ModelID != "m-1" {
		t.Fatalf("RES-02: the taken choice still answers: %s", spec.ModelID)
	}
	if d, err := r.ms.DefaultFor(ctx, u); err != nil || d == nil || d.Model != "m-1" {
		t.Fatalf("RES-02: the default shown: %+v %v", d, err)
	}
	// RES-04, SEC-02: a model out of the access is refused by the server
	if _, err := api.PatchAgent(ctx, u, nil, nil, &models.Choice{ConnectionID: r.conn.ID, Model: "m-2"}); !isCodeErr(err, "model_not_granted") {
		t.Fatalf("RES-04: %v", err)
	}

	// RES-12, SEC-02: a skill of another group is neither listed nor accepted
	snap := "enf-snap-" + uuid.NewString()[:6]
	var skillItem uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO catalog_items (type, name, source, published, skills, skills_snapshot) VALUES ('skill', $1, '{}', true, '["contract-review"]', $2) RETURNING id`,
		"enf-skill-"+uuid.NewString()[:6], snap).Scan(&skillItem); err != nil {
		t.Fatal(err)
	}
	lawyer := newUser(t, "enf-lawyer-"+uuid.NewString()[:6]+"@x.org")
	r.group(access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1, CatalogItems: []uuid.UUID{skillItem}}, lawyer)
	for _, uid := range []uuid.UUID{u, lawyer} {
		if _, err := pool.Exec(ctx, `INSERT INTO user_connections (user_id, item_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, uid, skillItem); err != nil {
			t.Fatal(err)
		}
	}
	names := func(uid uuid.UUID) []string {
		l, err := r.cat.SkillsFor(ctx, uid)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, s := range l {
			out = append(out, s.Name)
		}
		return out
	}
	if slices.Contains(names(u), "contract-review") || !slices.Contains(names(lawyer), "contract-review") {
		t.Fatalf("RES-12: %v / %v", names(u), names(lawyer))
	}
	api.Skills = func(ctx context.Context, uid uuid.UUID) ([]chat.Skill, error) {
		l, err := r.cat.SkillsFor(ctx, uid)
		out := []chat.Skill{}
		for _, s := range l {
			out = append(out, chat.Skill(s))
		}
		return out, err
	}
	if _, err := api.PostMessage(ctx, u, nil, chat.PostInput{Text: "проверь договор", Channel: "web", Skill: "contract-review"}); !isCodeErr(err, "skill_unavailable") {
		t.Fatalf("SEC-02 skill: %v", err)
	}
	// RES-13: the skills of a group agent are those its owner chose within the owner's access
	if l, _ := r.cat.GroupSkills(ctx, lawyer, []string{"contract-review"}); len(l) != 1 {
		t.Fatalf("RES-13: the skills of the agent of the lawyer: %+v", l)
	}
	if l, _ := r.cat.GroupSkills(ctx, u, []string{"contract-review"}); len(l) != 0 {
		t.Fatalf("RES-13: an owner without the skill gave it to the agent: %+v", l)
	}
	// RES-13: the model of a group agent is checked against the owner's access
	allowed := func(owner uuid.UUID, m string) bool {
		avail, _ := r.ms.AvailableFor(ctx, owner)
		return slices.ContainsFunc(avail, func(a models.AvailableModel) bool { return a.ConnectionID == r.conn.ID && a.Model == m })
	}
	gs := &groups.Service{Pool: pool, Users: repo()}
	ga := groupAgent(t, &u)
	admin := newUser(t, "enf-ga-admin-"+uuid.NewString()[:6]+"@x.org")
	choose := func(m string) error {
		_, err := gs.Update(ctx, adminPrincipal(admin), ga, groups.Patch{Model: &groups.ModelChoice{ConnectionID: r.conn.ID, Model: m}},
			func(_ context.Context, owner uuid.UUID, c groups.ModelChoice) bool { return allowed(owner, c.Model) })
		return err
	}
	if err := choose("m-1"); err != nil {
		t.Fatalf("RES-13: a model of the owner: %v", err)
	}
	if err := choose("m-2"); !isCodeErr(err, "model_not_available") {
		t.Fatalf("RES-13: a model out of the owner's access: %v", err)
	}
}

// FTR.NAB.CMN-0007: ACC-02, ADM-05, GRP-02 (R1, R3, R5).
func TestAccessScope(t *testing.T) {
	ctx := context.Background()
	r := newAccessRig(t)
	u := newUser(t, "enf-scope-"+uuid.NewString()[:6]+"@x.org")
	m1 := access.Choice{ConnectionID: r.conn.ID, Model: "m-1"}
	g := r.group(access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1, Channels: []string{"vkteams"}, CatalogItems: []uuid.UUID{r.item.ID}}, u)
	_, _ = pool.Exec(ctx, `UPDATE channels SET enabled = true WHERE kind = 'vkteams'`)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE channels SET enabled = false WHERE kind = 'vkteams'`)
	})
	if ea, _ := r.acc.Effective(ctx, u); !ea.HasModel(r.conn.ID, "m-1") || !ea.HasChannel("vkteams") || !ea.HasItem(r.item.ID) {
		t.Fatalf("before: %+v", ea)
	}
	// ACC-02: a switched-off connection, channel and an unpublished item are not in the access
	_, _ = pool.Exec(ctx, `UPDATE model_connections SET enabled = false WHERE id = $1`, r.conn.ID)
	_, _ = pool.Exec(ctx, `UPDATE channels SET enabled = false WHERE kind = 'vkteams'`)
	if _, err := r.cat.Save(ctx, &r.item.ID, catalog.Input{Published: ptr(false)}); err != nil {
		t.Fatal(err)
	}
	if ea, _ := r.acc.Effective(ctx, u); ea.HasModel(r.conn.ID, "m-1") || ea.HasChannel("vkteams") || ea.HasItem(r.item.ID) || r.acc.ItemGranted(ctx, u, r.item.ID) {
		t.Fatalf("ACC-02: %+v", ea)
	}
	// ADM-05: nor for an administrator
	boss := newUser(t, "enf-adm5-"+uuid.NewString()[:6]+"@x.org")
	_, _ = pool.Exec(ctx, `UPDATE users SET is_admin = true WHERE id = $1`, boss)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE users SET is_admin = false WHERE id = $1`, boss)
	})
	if ea, _ := r.acc.Effective(ctx, boss); ea.HasModel(r.conn.ID, "m-1") || ea.HasItem(r.item.ID) {
		t.Fatalf("ADM-05: %+v", ea)
	}
	_, _ = pool.Exec(ctx, `UPDATE model_connections SET enabled = true WHERE id = $1`, r.conn.ID)

	// GRP-02: 500 members at once; 501 are refused
	var many []uuid.UUID
	rows, err := pool.Query(ctx, `INSERT INTO users (email) SELECT 'bulk-' || $1 || '-' || i || '@x.org' FROM generate_series(1, 501) i RETURNING id`, uuid.NewString()[:6])
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id uuid.UUID
		_ = rows.Scan(&id)
		many = append(many, id)
	}
	rows.Close()
	if err := r.acc.AddMembers(ctx, r.actor, g, many); !isCodeErr(err, "invalid_users") {
		t.Fatalf("GRP-02 501: %v", err)
	}
	if err := r.acc.AddMembers(ctx, r.actor, g, many[:500]); err != nil {
		t.Fatalf("GRP-02 500: %v", err)
	}
}

// FTR.NAB.CMN-0007: ACC-06, ACC-07, ACC-08 (R13): a change of the access
// takes effect at once through the event, and within 30 s without it.
func TestAccessCaches(t *testing.T) {
	ctx := context.Background()
	r := newAccessRig(t)
	u := newUser(t, "enf-cache-"+uuid.NewString()[:6]+"@x.org")
	_, _ = pool.Exec(ctx, `UPDATE channels SET enabled = true WHERE kind = 'vkteams'`)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE channels SET enabled = false WHERE kind = 'vkteams'`)
	})
	m1 := access.Choice{ConnectionID: r.conn.ID, Model: "m-1"}
	g := r.group(access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1}, u)
	now := time.Now()
	hub := events.NewHub()
	reg := &channels.Registry{Pool: pool, Granted: r.acc.Channels, Clock: func() time.Time { return now }}
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	go reg.Listen(lctx, hub)
	r.acc.Events = hub
	if reg.Open(ctx, u, "vkteams") {
		t.Fatal("vkteams is not granted yet")
	}
	// ACC-06: the event drops the cache — the next message sees the new access
	if _, err := r.acc.PutGrants(ctx, r.actor, g, access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1, Channels: []string{"vkteams"}}, r.ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for !reg.Open(ctx, u, "vkteams") {
		if time.Now().After(deadline) {
			t.Fatal("ACC-06: the new access is not seen within 1 s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// ACC-07: the event is lost — the cache expires within 30 s
	r.acc.Events = nil
	if _, err := r.acc.PutGrants(ctx, r.actor, g, access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1}, r.ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	if !reg.Open(ctx, u, "vkteams") {
		t.Fatal("ACC-07: the cache is expected to hold without the event")
	}
	now = now.Add(30 * time.Second)
	if reg.Open(ctx, u, "vkteams") {
		t.Fatal("ACC-07: the old access lives longer than 30 s")
	}

	// ACC-08: 20 groups, 500 grants, a cold computation within 20 ms (p95)
	refs := make([]models.ModelRef, 25)
	for i := range refs {
		refs[i] = models.ModelRef{ID: fmt.Sprintf("perf-%02d", i)}
	}
	conn, err := r.ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Perf " + uuid.NewString()[:6], BaseURL: "http://llm.invalid", APIKey: "sk-perf-00001", Models: refs})
	if err != nil {
		t.Fatal(err)
	}
	var grants []access.Choice
	for _, m := range refs {
		grants = append(grants, access.Choice{ConnectionID: conn.ID, Model: m.ID})
	}
	for range 20 {
		r.group(access.Grants{Models: grants, DefaultModel: &grants[0]}, u)
	}
	var took []time.Duration
	for range 40 {
		start := time.Now()
		ea, err := r.acc.Effective(ctx, u)
		took = append(took, time.Since(start))
		if err != nil || len(ea.Groups) < 21 {
			t.Fatalf("ACC-08: %v %d groups", err, len(ea.Groups))
		}
	}
	slices.Sort(took)
	if p95 := took[len(took)*95/100]; p95 > 20*time.Millisecond {
		t.Fatalf("ACC-08: p95 %s", p95)
	}
}

func adminPrincipal(uid uuid.UUID) *domain.Principal {
	return &domain.Principal{UserID: uid, IsAdmin: true}
}

// FTR.NAB.CMN-0007 SEC-01, FTR.NAB.CHAT-0001 SEC-01: every route of groups,
// access and model connections (discover, refresh) answers 403 to a user
// who is not an administrator — the guard of /admin/api/v1 in app.
func TestAccessAdminRoutes(t *testing.T) {
	r := newAccessRig(t)
	u := newUser(t, "enf-sec-"+uuid.NewString()[:6]+"@x.org")
	router := chi.NewRouter()
	as := func(p *domain.Principal) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				next.ServeHTTP(w, req.WithContext(httpx.WithPrincipal(req.Context(), p)))
			})
		}
	}
	router.Route("/admin/api/v1", func(ar chi.Router) {
		ar.Use(as(&domain.Principal{UserID: u}), auth.RequireAdmin)
		r.ms.AdminRoutes(ar)
		r.acc.Routes(ar, func(*http.Request) access.Actor { return access.Actor{} }, r.ms.ModelInfo)
	})
	srv := httptest.NewServer(router)
	defer srv.Close()
	n := 0
	_ = chi.Walk(router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		path := strings.NewReplacer("{id}", uuid.NewString(), "{user}", uuid.NewString()).Replace(route)
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("SEC-01: %s %s → %d", method, route, resp.StatusCode)
		}
		n++
		return nil
	})
	if n < 20 {
		t.Fatalf("SEC-01: only %d routes", n)
	}
}

// sendings counts what an adapter sent.
type sendings struct {
	kind string
	sent []string
}

func (s *sendings) Channel() string { return s.kind }
func (s *sendings) Send(_ context.Context, _, text string) error {
	s.sent = append(s.sent, text)
	return nil
}
func (s *sendings) Typing(context.Context, string) {}
func (s *sendings) FetchFile(context.Context, string) (io.ReadCloser, string, error) {
	return nil, "", errors.New("no files")
}

// FTR.NAB.CMN-0007 RES-06 (R9): an answer is not delivered to a channel the
// groups of the user no longer grant.
func TestAccessDelivery(t *testing.T) {
	ctx := context.Background()
	r := newAccessRig(t)
	u := newUser(t, "enf-deliver-"+uuid.NewString()[:6]+"@x.org")
	_, _ = pool.Exec(ctx, `UPDATE channels SET enabled = true WHERE kind = 'vkteams'`)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE channels SET enabled = false WHERE kind = 'vkteams'`)
	})
	if _, err := pool.Exec(ctx, `INSERT INTO channel_contacts (channel, user_id, chat_id) VALUES ('vkteams', $1, 'chat-1')`, u); err != nil {
		t.Fatal(err)
	}
	m1 := access.Choice{ConnectionID: r.conn.ID, Model: "m-1"}
	g := r.group(access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1, Channels: []string{"vkteams"}}, u)
	reg := &channels.Registry{Pool: pool, Granted: r.acc.Channels}
	ad := &sendings{kind: "vkteams"}
	eng := &engine.Engine{Pool: pool, Users: repo(), Registry: reg, Adapters: map[string]channels.Adapter{"vkteams": ad}}
	out := func(text string) []byte {
		b, _ := json.Marshal(engine.Outbound{UserID: u, Channel: "vkteams", Text: text})
		return b
	}
	if err := eng.HandleOutbound(ctx, nil, out("первый")); err != nil || len(ad.sent) != 1 {
		t.Fatalf("granted: %v %v", err, ad.sent)
	}
	if _, err := r.acc.PutGrants(ctx, r.actor, g, access.Grants{Models: []access.Choice{m1}, DefaultModel: &m1}, r.ms.ModelInfo); err != nil {
		t.Fatal(err)
	}
	reg.Invalidate() // the event of access.Service in a pod
	if err := eng.HandleOutbound(ctx, nil, out("второй")); err != nil || len(ad.sent) != 1 {
		t.Fatalf("RES-06: delivered after the channel was taken: %v %v", err, ad.sent)
	}
}
