//go:build integration

package itest

// FTR.NAB.ADM-0001 qa

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/collect"
	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
)

// fakeVK is an MCP server with the tools of VK WorkSpace and the shapes of
// its answers (arch §5): chats by the time of the last message, messages of
// a chat from the end back.
type fakeVK struct {
	mu       sync.Mutex
	token    string
	self     string
	chats    []map[string]any
	messages map[string][]map[string]any
	calls    map[string]int
	read     []string // chat_sn read with messenger-list-messages
	limited  bool     // HTTP 429 on every call
}

func (f *fakeVK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.limited {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		http.Error(w, "missing or invalid Authorization header", http.StatusUnauthorized)
		return
	}
	body, _ := io.ReadAll(r.Body)
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		} `json:"params"`
	}
	_ = json.Unmarshal(body, &req)
	if len(req.ID) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	answer := func(result any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}
	text := func(v any) {
		b, _ := json.Marshal(v)
		answer(map[string]any{"content": []map[string]string{{"type": "text", "text": string(b)}}, "isError": false})
	}
	switch req.Method {
	case "initialize":
		answer(map[string]any{"protocolVersion": mcp.ProtocolVersion, "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "vk", "version": "1"}})
	case "tools/list":
		ro := map[string]any{"readOnlyHint": true}
		var tools []map[string]any
		for _, n := range []string{"messenger-get-current-user", "messenger-list-chats", "messenger-list-messages", "mail-list-folders", "mail-search", "mail-read-thread"} {
			tools = append(tools, map[string]any{"name": n, "inputSchema": map[string]any{"type": "object"}, "annotations": ro})
		}
		for _, n := range []string{"messenger-mark-as-read", "messenger-send-message"} {
			tools = append(tools, map[string]any{"name": n, "inputSchema": map[string]any{"type": "object"}})
		}
		answer(map[string]any{"tools": tools})
	case "tools/call":
		a := req.Params.Arguments
		f.calls[req.Params.Name]++
		switch req.Params.Name {
		case "messenger-get-current-user":
			text(map[string]any{"email": f.self, "name": "Me"})
		case "messenger-list-chats":
			out := []map[string]any{}
			for _, c := range f.chats {
				if q, _ := a["name_query"].(string); q != "" && !strings.Contains(strings.ToLower(c["name"].(string)), strings.ToLower(q)) {
					continue
				}
				if types, ok := a["include_types"].([]any); ok {
					keep := false
					for _, t := range types {
						keep = keep || t == c["type"]
					}
					if !keep {
						continue
					}
				}
				out = append(out, c)
			}
			text(map[string]any{"chats": out, "next_cursor": nil})
		case "messenger-list-messages":
			sn, _ := a["chat_sn"].(string)
			f.read = append(f.read, sn)
			all := f.messages[sn]
			end := len(all)
			if c, ok := a["cursor"].(string); ok && c != "" {
				var cur struct {
					From json.Number `json:"from_msg_id"`
				}
				raw, err := decodeCursor(c)
				if err != nil || json.Unmarshal(raw, &cur) != nil {
					answer(map[string]any{"content": []map[string]string{{"type": "text", "text": "invalid cursor"}}, "isError": true})
					return
				}
				end = 0
				for i, m := range all {
					if lessNum(m["teams_id"].(string), cur.From.String()) {
						end = i + 1
					}
				}
			}
			lim := 200
			if l, ok := a["limit"].(float64); ok {
				lim = int(l)
			}
			start := max(0, end-lim)
			text(map[string]any{"messages": all[start:end], "next_cursor": nil})
		default:
			text(map[string]string{"done": req.Params.Name})
		}
	default:
		answer(map[string]any{})
	}
}

func lessNum(a, b string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

func decodeCursor(c string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(c) }

var vkID = 7707800000000000000

func vkMsg(chatSN, sender string, at time.Time, mentions ...string) map[string]any {
	vkID++
	id := fmt.Sprint(vkID)
	if mentions == nil {
		mentions = []string{}
	}
	return map[string]any{"chat_sn": chatSN, "sender_id": sender, "teams_id": id, "mentions": mentions, "text": "текст сообщения",
		"time_sent": at.UTC().Format(time.RFC3339), "url": "https://teams.example/message/v1/" + id}
}

func vkChat(sn, name, typ string, last time.Time, notify bool) map[string]any {
	return map[string]any{"chat_sn": sn, "name": name, "type": typ, "is_member": true, "last_message_at": last.Format(time.RFC3339),
		"notifications_enabled": notify, "unread_count": 0, "unread_mentions_count": 0}
}

// VK-01, VK-03, VK-04, TEAMS-11, TEAMS-12, TEAMS-14, TEAMS-15, TEAMS-16, TEAMS-18, RATE-01
func TestVKWorkSpaceCollect(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	on := true
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://nabu.test", key)
	ms := &models.Service{Pool: pool, Box: box}
	conn, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "VK " + uuid.NewString()[:6], BaseURL: "http://llm.invalid", APIKey: "sk-fake-0003", Models: models.Refs("fake-1")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &on, Model: []byte(fmt.Sprintf(`{"connectionId":%q,"model":"fake-1"}`, conn.ID))}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		off := false
		_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off})
	})
	collect.SetVKSettings(time.Hour, 3) // a small limit of a chat for TEAMS-15
	t.Cleanup(func() { collect.SetVKSettings(time.Hour, 1000) })

	self := "vk-" + uuid.NewString()[:6] + "@corp.example"
	now := time.Now().UTC()
	at := now.Add(-2 * time.Hour)
	team, disc, noisy, muted := "1001@chat.agent", "1002@chat.agent", "1003@chat.agent", "1004@chat.agent"
	parent := vkMsg(team, "a@corp.example", at)
	parent["thread_id"] = disc
	vk := &fakeVK{token: "vk-token", self: self, calls: map[string]int{}, chats: []map[string]any{
		vkChat(team, "team", "group", now.Add(-time.Hour), true),
		vkChat(noisy, "tech-devs", "group", now.Add(-time.Hour), true),
		vkChat(muted, "alerts-muted", "group", now.Add(-time.Hour), false),
	}, messages: map[string][]map[string]any{
		team:  {parent},
		disc:  {vkMsg(disc, self, at.Add(time.Minute))},
		noisy: {vkMsg(noisy, self, at), vkMsg(noisy, "b@corp.example", at.Add(time.Second)), vkMsg(noisy, "b@corp.example", at.Add(2*time.Second)), vkMsg(noisy, self, at.Add(3*time.Second)), vkMsg(noisy, self, at.Add(4*time.Second))},
		muted: {vkMsg(muted, self, at)},
	}}
	vsrv := httptest.NewServer(vk)
	defer vsrv.Close()

	cat := &catalog.Service{Pool: pool, Box: box, Signer: signer}
	cat.MemoryRun = s.ProxyGuard
	userEmail := func(ctx context.Context, uid uuid.UUID) (string, bool) {
		u, err := repo().Get(ctx, uid)
		if err != nil || u == nil {
			return "", false
		}
		return u.Email, true
	}
	cat.SetUserEmail(userEmail)
	builtin := mcp.NewServer(signer)
	builtin.Register(s.CollectTools(func(ctx context.Context, uid uuid.UUID) comm.UserInfo {
		e, _ := userEmail(ctx, uid)
		return comm.UserInfo{Email: e, Timezone: "UTC"}
	})...)
	ir := chi.NewRouter()
	ir.Handle("/internal/v1/mcp", builtin)
	cat.ProxyRoutes(ir, userEmail, nil)
	isrv := httptest.NewServer(ir)
	defer isrv.Close()
	cat.InternalURL = isrv.URL

	mode, yes := "personal", true
	item, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "vk-" + uuid.NewString()[:6], Source: &catalog.Source{Kind: "url", URL: vsrv.URL},
		Mode: &mode, Published: &yes, PersonalAuth: &catalog.PersonalAuth{Kind: "token"}, ReadOnly: ptr(false)})
	if err != nil {
		t.Fatal(err)
	}
	adapter := "vkworkspace"
	if err := s.SetSource(ctx, item.ID, &adapter, &yes); err != nil {
		t.Fatal(err)
	}
	a := newUser(t, self)
	if _, err := cat.Connect(ctx, a, item.ID, "vk-token"); err != nil {
		t.Fatal(err)
	}
	_ = s.Consent(ctx, a)

	// VK-01: one item gives two sources.
	srcs, err := s.Sources(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, x := range srcs {
		if x.ItemID == item.ID {
			keys = append(keys, x.Source+"="+x.Title)
		}
	}
	if strings.Join(keys, ",") != "vkteams=VK Teams,vkmail=Почта VK WorkMail" {
		t.Fatalf("VK-01: sources %v", keys)
	}

	bq := &queue{}
	ev := &capture{}
	ts := &tasks.Service{Pool: pool, Events: ev, Bus: bq, Rules: tasks.Rules{MinInterval: time.Minute, MaxActive: 5}, Catchup: time.Hour, Tick: time.Second,
		MaxFailures: 3, DefaultTimezone: "UTC", MemoryGate: func(ctx context.Context, uid uuid.UUID, kind string, set []byte) error {
			return s.CheckCollect(ctx, uid, set)
		}}
	// TEAMS-11: a muted group is a chat of VK Teams
	if _, err := ts.SetMemoryTask(ctx, a, "collect", tasks.MemoryPatch{Enabled: &yes,
		Settings: []byte(fmt.Sprintf(`{"sources":[{"itemId":%q,"source":"vkteams"}],"includeMuted":["abc"]}`, item.ID))}); err == nil || !strings.Contains(err.Error(), "invalid_exclusion") {
		t.Fatalf("TEAMS-11: %v", err)
	}
	op := &fakeCollector{}
	eng := &engine.Engine{Cfg: engine.Config{InternalURL: isrv.URL}, Pool: appPool(t), Op: op, Users: repo(), Chat: &chat.Store{Pool: pool}, Models: ms, Catalog: cat, Tasks: ts, Comm: s,
		Services: &services.Service{Pool: pool, Events: ev}, Ledger: &ledger.Ledger{Pool: pool}, Signer: signer, Events: ev, Bus: bq,
		CollectCfg: engine.CollectConfig{BatchThreads: 20, RunTimeout: time.Minute, BackfillDays: 30}}
	eng.Collector = eng
	mt, err := ts.SetMemoryTask(ctx, a, "collect", tasks.MemoryPatch{Enabled: &yes, Settings: []byte(fmt.Sprintf(`{"sources":[{"itemId":%q,"source":"vkteams"}]}`, item.ID))})
	if err != nil {
		t.Fatal(err)
	}
	tick := func() (status, summary string, failures int) {
		t.Helper()
		var id uuid.UUID
		if err := pool.QueryRow(ctx, `INSERT INTO task_runs (task_id, scheduled_for, status) VALUES ($1, now(), 'running') RETURNING id`, *mt.TaskID).Scan(&id); err != nil {
			t.Fatal(err)
		}
		msg, _ := json.Marshal(tasks.RunMessage{RunID: id, TaskID: *mt.TaskID, UserID: a})
		if err := eng.HandleTaskRun(ctx, []byte(a.String()), msg); err != nil {
			t.Fatal(err)
		}
		for _, m := range bq.take() {
			if err := eng.HandleCollectBatch(ctx, nil, m); err != nil {
				t.Fatal(err)
			}
		}
		var sum *string
		if err := pool.QueryRow(ctx, `SELECT r.status, r.summary, t.failures FROM task_runs r JOIN scheduled_tasks t ON t.id = r.task_id WHERE r.id = $1`, id).Scan(&status, &sum, &failures); err != nil {
			t.Fatal(err)
		}
		if sum != nil {
			summary = *sum
		}
		return
	}

	var refused []string
	op.script = func(call func(server, tool string, args any) (string, bool)) {
		out, isErr := call("nabu", "comm_batch", map[string]any{})
		if isErr {
			refused = append(refused, "comm_batch: "+out)
			return
		}
		var batch struct {
			Threads []struct {
				ThreadKey    string             `json:"threadKey"`
				DayKey       string             `json:"dayKey"`
				ReadWith     collect.ToolCall   `json:"readWith"`
				AlsoReadWith []collect.ToolCall `json:"alsoReadWith"`
			} `json:"threads"`
		}
		_ = json.Unmarshal([]byte(out), &batch)
		for _, th := range batch.Threads {
			call(item.Name, th.ReadWith.Tool, th.ReadWith.Args)
			for _, c := range th.AlsoReadWith {
				if out, isErr := call(item.Name, c.Tool, c.Args); isErr {
					refused = append(refused, "part: "+out) // TEAMS-14: a discussion of the batch passes
				}
			}
			// VK-03, VK-04: what the collector may not do
			if out, isErr := call(item.Name, "messenger-mark-as-read", map[string]any{"chat_sn": th.ThreadKey, "last_read_message_id": "1"}); isErr || strings.Contains(out, "not available") {
				refused = append(refused, "mark: "+out)
			}
			if out, isErr := call(item.Name, "messenger-list-messages", map[string]any{"chat_sn": muted}); isErr || strings.Contains(out, "not in the batch") {
				refused = append(refused, "foreign: "+out)
			}
			call("nabu", "comm_note_save", map[string]any{"threadKey": th.ThreadKey, "dayKey": th.DayKey, "title": "Чат " + th.ThreadKey, "summary": "Обсудили", "topics": []string{}})
		}
	}

	status, summary, _ := tick()
	if status != "succeeded" {
		t.Fatalf("tick: %s %s, refused %v", status, summary, refused)
	}
	// TEAMS-15: the noisy chat stopped at the limit and the result names it
	first, rest, _ := strings.Cut(summary, "\n")
	if !strings.HasPrefix(first, "messages:") || rest != `truncated:{"tech-devs":3}` {
		t.Fatalf("TEAMS-15: summary %q", summary)
	}
	joined := strings.Join(refused, " | ")
	if strings.Contains(joined, "part:") || !strings.Contains(joined, "mark:") || !strings.Contains(joined, "foreign:") {
		t.Fatalf("VK-03, VK-04, TEAMS-14: refused %v", refused)
	}
	vk.mu.Lock()
	marked, readMuted := vk.calls["messenger-mark-as-read"], 0
	for _, sn := range vk.read {
		if sn == muted {
			readMuted++
		}
	}
	vk.mu.Unlock()
	if marked != 0 || readMuted != 0 {
		t.Fatalf("VK-03: the source got mark-as-read %d times, the muted group was read %d times", marked, readMuted)
	}
	notes, _ := s.List(ctx, a, comm.Filter{}, httpx.Page{Limit: 10})
	titles := map[string]bool{}
	for _, n := range notes.Items {
		titles[n.Title] = true
	}
	if !titles["Чат "+team] || !titles["Чат "+noisy] || titles["Чат "+muted] {
		t.Fatalf("notes: %v", titles)
	}
	// TEAMS-16: the overlap brings the same messages again: nothing new
	vk.mu.Lock()
	vk.messages[team] = append(vk.messages[team], vkMsg(team, self, time.Now().UTC().Add(-time.Minute)))
	vk.mu.Unlock()
	if status, summary, _ := tick(); status != "succeeded" || !strings.HasPrefix(summary, "messages:1 ") {
		t.Fatalf("TEAMS-16: %s %q", status, summary)
	}
	if status, summary, _ := tick(); status != "succeeded" || summary != "new:0" {
		t.Fatalf("TEAMS-16: %s %q", status, summary)
	}

	// RATE-01: HTTP 429 — the tick is repeated later, not a failure
	vk.mu.Lock()
	vk.limited = true
	vk.mu.Unlock()
	if status, summary, failures := tick(); status != "succeeded" || summary != "rate_limited" || failures != 0 {
		t.Fatalf("RATE-01: %s %q failures %d", status, summary, failures)
	}
	vk.mu.Lock()
	vk.limited = false
	vk.mu.Unlock()

	// TEAMS-12: the muted groups of the user for the settings
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(httpx.WithPrincipal(req.Context(), &domain.Principal{UserID: a})))
		})
	})
	api := &comm.API{Store: s, CallSource: func(ctx context.Context, uid, it uuid.UUID, tool string, args json.RawMessage) (string, error) {
		return cat.CallSource(ctx, uid, it, tool, args)
	}}
	r.Route("/api/v1", api.Routes)
	hsrv := httptest.NewServer(r)
	defer hsrv.Close()
	resp, err := http.Get(hsrv.URL + "/api/v1/comm/sources/" + item.ID.String() + "/muted-groups?q=" + url.QueryEscape("alert"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != `{"items":[{"chatSn":"`+muted+`","name":"alerts-muted"}]}`+"\n" {
		t.Fatalf("TEAMS-12: %d %s", resp.StatusCode, b)
	}
}

// fakeOAuthMCP is an MCP server with the OAuth of MCP, like VK WorkSpace.
type fakeOAuthMCP struct {
	srv   *httptest.Server
	mu    sync.Mutex
	forms []url.Values
	regs  int
}

func newFakeOAuthMCP(t *testing.T) *fakeOAuthMCP {
	f := &fakeOAuthMCP{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /mcp", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Bearer resource_metadata="`+f.srv.URL+`/.well-known/oauth-protected-resource/mcp"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
	mux.HandleFunc("GET /.well-known/oauth-protected-resource/mcp", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"resource": f.srv.URL + "/mcp", "authorization_servers": []string{f.srv.URL}})
	})
	mux.HandleFunc("GET /.well-known/oauth-authorization-server", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"issuer": f.srv.URL, "authorization_endpoint": f.srv.URL + "/oauth/authorize",
			"token_endpoint": f.srv.URL + "/oauth/token", "registration_endpoint": f.srv.URL + "/oauth/register",
			"code_challenge_methods_supported": []string{"S256"}, "token_endpoint_auth_methods_supported": []string{"none"}})
	})
	mux.HandleFunc("POST /oauth/register", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.regs++
		n := f.regs
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"client_id": fmt.Sprintf("client-%d", n)})
	})
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		f.mu.Lock()
		f.forms = append(f.forms, r.PostForm)
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "at-" + r.PostForm.Get("grant_type"), "refresh_token": "rt", "expires_in": 60})
	})
	f.srv = httptest.NewTLSServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// OAUTH-01 (stored), OAUTH-06, OAUTH-07, OAUTH-08
func TestMCPOAuthCatalog(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	f := newFakeOAuthMCP(t)
	cat := &catalog.Service{Pool: pool, Box: box, Signer: jwt.NewSigner("https://nabu.test", key), HTTP: f.srv.Client(),
		PublicAPIURL: "https://nabu.test", WebURL: "https://nabu.test"}
	mode, yes := "personal", true
	item, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "oauth-" + uuid.NewString()[:6], Source: &catalog.Source{Kind: "url", URL: f.srv.URL + "/mcp"},
		Mode: &mode, Published: &yes, PersonalAuth: &catalog.PersonalAuth{Kind: catalog.KindMCPOAuth, ClientID: "ignored"}})
	if err != nil {
		t.Fatal(err)
	}
	pa := item.PersonalAuth
	if pa == nil || pa.ClientID != "client-1" || pa.Issuer != f.srv.URL || pa.Resource != f.srv.URL+"/mcp" || pa.RegisteredAt == nil || pa.Error != nil || !item.Published {
		t.Fatalf("OAUTH-01: %+v", pa)
	}
	// a save without a new address keeps the registration
	title := "VK WorkSpace"
	if again, err := cat.Save(ctx, &item.ID, catalog.Input{Title: &title, PersonalAuth: &catalog.PersonalAuth{Kind: catalog.KindMCPOAuth}}); err != nil || again.PersonalAuth.ClientID != "client-1" {
		t.Fatalf("a second save: %+v %v", again, err)
	}

	// OAUTH-06: the connection names the resource and the code challenge
	uid := newUser(t, "oauth-"+uuid.NewString()[:6]+"@x.org")
	res, err := cat.Connect(ctx, uid, item.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	au, _ := url.Parse(res.AuthorizeURL)
	q := au.Query()
	if q.Get("client_id") != "client-1" || q.Get("code_challenge_method") != "S256" || q.Get("resource") != f.srv.URL+"/mcp" {
		t.Fatalf("OAUTH-06: %s", res.AuthorizeURL)
	}
	back := cat.Callback(ctx, item.ID, q.Get("state"), "code-1", "")
	if !strings.Contains(back, "oauth=connected") {
		t.Fatalf("OAUTH-06: callback %s", back)
	}
	f.mu.Lock()
	form := f.forms[0]
	f.mu.Unlock()
	if form.Get("resource") != f.srv.URL+"/mcp" || form.Get("code_verifier") == "" || form.Get("client_id") != "client-1" {
		t.Fatalf("OAUTH-06: token request %v", form)
	}

	// OAUTH-07: an expired token is refreshed by the call
	if _, err := pool.Exec(ctx, `UPDATE user_connections SET expires_at = now() - interval '1 minute' WHERE user_id = $1 AND item_id = $2`, uid, item.ID); err != nil {
		t.Fatal(err)
	}
	cat.CheckMCP = func(context.Context, agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
		return &agent.MCPCheckResponse{OK: true}, nil
	}
	if _, err := cat.Check(ctx, item.ID, "", uid); err != nil { // the check of a personal item uses the access of the administrator
		t.Fatal(err)
	}
	f.mu.Lock()
	refreshed := len(f.forms) == 2 && f.forms[1].Get("grant_type") == "refresh_token" && f.forms[1].Get("resource") == f.srv.URL+"/mcp"
	f.mu.Unlock()
	if !refreshed {
		t.Fatalf("OAUTH-07: forms %v", f.forms)
	}

	// OAUTH-08: the registration again drops the connections
	rr, err := cat.Reregister(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	var left int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_connections WHERE item_id = $1`, item.ID).Scan(&left)
	if rr.ClientID != "client-2" || rr.DisconnectedUsers != 1 || left != 0 {
		t.Fatalf("OAUTH-08: %+v, connections left %d", rr, left)
	}
	if _, err := cat.Reregister(ctx, uuid.New()); err == nil {
		t.Fatal("re-registration of a missing item")
	}
}
