//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
)

// newServiceClient adds a service client with delegation and returns the
// principal it acts as for the user.
func newServiceClient(t *testing.T, name string, uid uuid.UUID) *domain.Principal {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(), `INSERT INTO service_clients (name, client_id, secret_hash, can_delegate)
		VALUES ($1, $1, '\x00', true) RETURNING id`, name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return &domain.Principal{UserID: uid, ClientID: &id, ClientName: name}
}

func countPrefix(msgs []string, prefix string) int {
	n := 0
	for _, m := range msgs {
		if strings.HasPrefix(m, prefix) {
			n++
		}
	}
	return n
}

// FTR.NAB.CHAT-0002 NC-01…NC-14
func TestNotices(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	uid := newUser(t, "notice-"+uuid.NewString()[:6]+"@x.org")
	other := newUser(t, "notice-other-"+uuid.NewString()[:6]+"@x.org")
	name := "hmr-" + uuid.NewString()[:6]
	p := newServiceClient(t, name, uid)
	topic, err := r.api.Store.CreateTopic(ctx, uid, "ISS.FMS-0007 — Скидка")
	if err != nil {
		t.Fatal(err)
	}
	n := chat.Notice{Text: "**Discovery обновлён · ревизия 4**", Payload: json.RawMessage(`{"kind":"discovery.revision","revision":4}`),
		IdempotencyKey: "discovery-notice:run-1:" + uid.String()}

	// NC-01
	m, created, err := r.api.PostNotice(ctx, p, topic.ID.String(), n)
	if err != nil || !created || m.Role != "notice" || m.Channel != "client:"+name || m.ClientName == nil || *m.ClientName != name ||
		!strings.Contains(string(m.Payload), `"revision": 4`) && !strings.Contains(string(m.Payload), `"revision":4`) {
		t.Fatalf("NC-01: %+v %v %v", m, created, err)
	}
	// NC-08: the events of a message and of the unread counter
	if ts := strings.Join(r.ev.types(), ","); !strings.Contains(ts, "message.created") || !strings.Contains(ts, "conversation.unread") {
		t.Fatalf("NC-08: %s", ts)
	}
	// NC-06: a repeat returns the same message and counts once
	again, created, err := r.api.PostNotice(ctx, p, topic.ID.String(), n)
	if err != nil || created || again.ID != m.ID {
		t.Fatalf("NC-06: %+v %v %v", again, created, err)
	}
	if c := r.api.Store.UnreadCount(ctx, topic.ID); c != 1 {
		t.Fatalf("NC-06: unread %d", c)
	}
	// NC-07: the same key in another topic is another notice
	topic2, _ := r.api.Store.CreateTopic(ctx, uid, "Другая тема")
	if m2, created, err := r.api.PostNotice(ctx, p, topic2.ID.String(), n); err != nil || !created || m2.ID == m.ID {
		t.Fatalf("NC-07: %v %v", created, err)
	}
	// NC-02: a topic of another user
	foreign, _ := r.api.Store.CreateTopic(ctx, other, "Чужая")
	if _, _, err := r.api.PostNotice(ctx, p, foreign.ID.String(), n); !isCodeErr(err, "not_found") {
		t.Fatalf("NC-02: %v", err)
	}
	// NC-03: an archived topic
	if _, err := r.api.Store.Patch(ctx, uid, topic2.ID, nil, ptr(true)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.api.PostNotice(ctx, p, topic2.ID.String(), chat.Notice{Text: "x", IdempotencyKey: "other"}); !isCodeErr(err, "not_found") {
		t.Fatalf("NC-03: %v", err)
	}
	// NC-04: the site itself does not write notices
	if _, _, err := r.api.PostNotice(ctx, &domain.Principal{UserID: uid}, topic.ID.String(), n); !isCodeErr(err, "delegation_required") {
		t.Fatalf("NC-04: %v", err)
	}
	// NC-05 through the API: the field is named
	if _, _, err := r.api.PostNotice(ctx, p, topic.ID.String(), chat.Notice{Text: "", IdempotencyKey: "k"}); !isCodeErr(err, "invalid_notice") {
		t.Fatalf("NC-05: %v", err)
	}
	// NC-09, NC-10: no turn, nothing for messengers
	r.bus.mu.Lock()
	inbound, outbound := countPrefix(r.bus.msgs, kafka.TopicInbound+" "), countPrefix(r.bus.msgs, kafka.TopicOutbound+" ")
	r.bus.mu.Unlock()
	if inbound != 0 || outbound != 0 || len(r.op.prompts) != 0 {
		t.Fatalf("NC-09, NC-10: inbound %d outbound %d prompts %d", inbound, outbound, len(r.op.prompts))
	}

	// NC-11: the next turn begins with the notice
	if _, err := r.api.Post(ctx, uid, &topic.ID, "что изменилось?", "client:"+name, p.ClientID, nil, nil); err != nil {
		t.Fatal(err)
	}
	r.drain()
	last := r.op.prompts[len(r.op.prompts)-1].Text
	if !strings.HasPrefix(last, "[Notice of "+name+", ") || !strings.Contains(last, "Discovery обновлён") || !strings.HasSuffix(last, "что изменилось?") {
		t.Fatalf("NC-11: %q", last)
	}
	// the notice is in the context once: the following turn does not repeat it
	r.api.Post(ctx, uid, &topic.ID, "спасибо", "client:"+name, p.ClientID, nil, nil) //nolint:errcheck
	r.drain()
	if last := r.op.prompts[len(r.op.prompts)-1].Text; strings.Contains(last, "[Notice of") {
		t.Fatalf("NC-11: repeated %q", last)
	}
	// the persona tells the agent what the mark is (NC-15)
	if sys := r.op.opened[len(r.op.opened)-1].SystemAppend; !strings.Contains(sys, "[Notice of <product>, <time>]") {
		t.Fatalf("NC-15: %q", sys)
	}

	// NC-12: at most ten notices in a turn, the latest
	for i := range 15 {
		if _, _, err := r.api.PostNotice(ctx, p, topic.ID.String(), chat.Notice{Text: "n" + string(rune('a'+i)), IdempotencyKey: "many-" + string(rune('a'+i))}); err != nil {
			t.Fatal(err)
		}
	}
	r.api.Post(ctx, uid, &topic.ID, "итог?", "client:"+name, p.ClientID, nil, nil) //nolint:errcheck
	r.drain()
	last = r.op.prompts[len(r.op.prompts)-1].Text
	if c := strings.Count(last, "[Notice of"); c != chat.MaxTurnNotices || strings.Contains(last, "\nna\n") || !strings.Contains(last, "\nno\n") {
		t.Fatalf("NC-12: %d %q", c, last)
	}

	// NC-13: a session without a snapshot is seeded with the notices it knew
	if _, _, err := r.api.PostNotice(ctx, p, topic.ID.String(), chat.Notice{Text: "seen before", IdempotencyKey: "seen"}); err != nil {
		t.Fatal(err)
	}
	r.api.Post(ctx, uid, &topic.ID, "ок", "client:"+name, p.ClientID, nil, nil) //nolint:errcheck
	r.drain()
	_, _ = pool.Exec(ctx, `UPDATE harness_sessions SET closed_at = now() WHERE conversation_id = $1`, topic.ID)
	delete(r.s3.m, "sessions/"+topic.ID.String()+".jsonl")
	r.api.Post(ctx, uid, &topic.ID, "ещё", "client:"+name, p.ClientID, nil, nil) //nolint:errcheck
	r.drain()
	seeded := false
	for _, h := range r.op.opened[len(r.op.opened)-1].History {
		if h.Role == "user" && strings.HasPrefix(h.Text, "[Notice of "+name) && strings.HasSuffix(h.Text, "seen before") {
			seeded = true
		}
	}
	if !seeded {
		t.Fatalf("NC-13: %+v", r.op.opened[len(r.op.opened)-1].History)
	}

	// NC-14: a reset hides the notices before it
	if _, _, err := r.api.PostNotice(ctx, p, topic.ID.String(), chat.Notice{Text: "before reset", IdempotencyKey: "reset"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.api.Reset(ctx, uid, &topic.ID, "client:"+name); err != nil {
		t.Fatal(err)
	}
	r.drain()
	r.api.Post(ctx, uid, &topic.ID, "после сброса", "client:"+name, p.ClientID, nil, nil) //nolint:errcheck
	r.drain()
	if last := r.op.prompts[len(r.op.prompts)-1].Text; strings.Contains(last, "before reset") {
		t.Fatalf("NC-14: %q", last)
	}
}

// FTR.NAB.CHAT-0002 NX-01…NX-03, NX-06: the delegated call of a product names
// the topic and the channel of the turn under the signature of Nabu.
func TestProductTurnContext(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://nabu.test", key)
	got := make(chan string, 8)
	product := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		got <- r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"ok"}]}}`))
	}))
	defer product.Close()
	st := &chat.Store{Pool: pool}
	cat := &catalog.Service{Pool: pool, Box: box, Signer: signer, InternalURL: "http://api.test", ChannelOf: st.LastUserChannel}
	mode := "personal"
	it, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "product-" + uuid.NewString()[:6], Source: &catalog.Source{Kind: "url", URL: product.URL},
		Mode: &mode, Published: ptr(true), PersonalAuth: &catalog.PersonalAuth{Kind: "delegation", Audience: "hammurapi"}})
	if err != nil {
		t.Fatal(err)
	}
	email := "turn-" + uuid.NewString()[:6] + "@x.org"
	uid := newUser(t, email)
	if _, err := cat.Connect(ctx, uid, it.ID, ""); err != nil {
		t.Fatal(err)
	}
	router := chi.NewRouter()
	cat.ProxyRoutes(router, func(context.Context, uuid.UUID) (string, bool) { return email, true }, nil)
	api := httptest.NewServer(router)
	defer api.Close()
	topic, _ := st.CreateTopic(ctx, uid, "ISS.FMS-0007")

	call := func(conv string) string {
		tok := cat.ProxyToken(uid, it.ID, "", conv, time.Minute)
		req, _ := http.NewRequest(http.MethodPost, api.URL+"/internal/v1/mcp-proxy/"+it.ID.String(),
			strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"request_discovery_revision","arguments":{"conv":"forged","ch":"client:hammurapi"}}}`))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		select {
		case a := <-got:
			return strings.TrimPrefix(a, "Bearer ")
		case <-time.After(5 * time.Second):
			t.Fatalf("the product got no call: %d", resp.StatusCode)
			return ""
		}
	}
	for _, c := range []struct{ id, channel string }{{"NX-01", "client:hammurapi"}, {"NX-02", domain.ChannelTelegram}, {"NX-03", domain.ChannelWeb}} {
		if _, err := st.AddUser(ctx, topic.ID, "сообщение", c.channel, nil, nil); err != nil {
			t.Fatal(err)
		}
		claims, err := signer.Verify(call(topic.ID.String()), "hammurapi")
		if err != nil || claims.Conversation != topic.ID.String() || claims.Channel != c.channel || claims.Email != email {
			t.Fatalf("%s: %+v %v", c.id, claims, err)
		}
	}
	// a call outside a conversation (a task) names no turn
	if claims, err := signer.Verify(call(""), "hammurapi"); err != nil || claims.Conversation != "" || claims.Channel != "" {
		t.Fatalf("NX-05: %+v %v", claims, err)
	}
}

// FTR.NAB.CHAT-0002 NX-07: the tools of a catalog item reach an open session.
// An administrator allows a new tool of a product: the next turn opens the
// session again from its snapshot, so the agent lists the new tool and keeps
// the context; without a change the session is kept.
func TestSessionCatalogChange(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	uid := newUser(t, "catalog-"+uuid.NewString()[:6]+"@x.org")
	mode := "personal"
	it, err := r.cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "product-" + uuid.NewString()[:6], Source: &catalog.Source{Kind: "url", URL: "https://p/mcp"},
		Mode: &mode, Published: ptr(true), PersonalAuth: &catalog.PersonalAuth{Kind: "delegation", Audience: "hammurapi"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.cat.Connect(ctx, uid, it.ID, ""); err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `UPDATE catalog_items SET tools = '[{"name":"read_issue"}]', allowed_tools = '["read_issue"]' WHERE id = $1`, it.ID)
	main, _ := r.api.Store.Main(ctx, uid)
	turn := func(text string) {
		t.Helper()
		if _, err := r.api.Post(ctx, uid, nil, text, domain.ChannelWeb, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		r.drain()
	}
	turn("раз")
	opened := len(r.op.opened)
	turn("два")
	if len(r.op.opened) != opened {
		t.Fatal("NX-07: the session is opened again without a change of the catalog")
	}
	// the administrator allows a new tool of the item
	if _, err := pool.Exec(ctx, `UPDATE catalog_items SET tools = '[{"name":"read_issue"},{"name":"request_discovery_revision"}]',
		allowed_tools = '["read_issue","request_discovery_revision"]' WHERE id = $1`, it.ID); err != nil {
		t.Fatal(err)
	}
	turn("три")
	if len(r.op.opened) != opened+1 {
		t.Fatalf("NX-07: the session is not opened again after the change: %d → %d", opened, len(r.op.opened))
	}
	if last := r.op.opened[len(r.op.opened)-1]; len(last.Snapshot) == 0 {
		t.Fatal("NX-07: the session is not restored from its snapshot — the context is lost")
	}
	var open int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM harness_sessions WHERE conversation_id = $1 AND closed_at IS NULL`, main.ID).Scan(&open)
	if open != 1 {
		t.Fatalf("NX-07: %d open sessions", open)
	}
}
