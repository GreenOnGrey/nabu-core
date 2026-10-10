//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/accounts"
	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/auth"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/channels/email"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/confirm"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/groups"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

func registry(t *testing.T) *channels.Registry {
	t.Helper()
	box, err := crypto.NewBox(key)
	if err != nil {
		t.Fatal(err)
	}
	return &channels.Registry{Pool: pool, Box: box}
}

func code(err error) string {
	if e, ok := apperr.As(err); ok {
		return e.Code
	}
	return ""
}

func t2(v bool) *bool { return &v }

// inbox records what the channels pass to the agent.
type inbox struct {
	mu  sync.Mutex
	got []channels.Inbound
}

func (i *inbox) Receive(_ context.Context, in channels.Inbound) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.got = append(i.got, in)
	return nil
}

// listS3 is an in-memory storage with List.
type listS3 struct{ mem }

func (s *listS3) List(_ context.Context, prefix string) ([]storage.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []storage.Object
	for k := range s.m {
		if strings.HasPrefix(k, prefix) {
			out = append(out, storage.Object{Key: k})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// CH-01, CH-02, CH-03, CH-04: the registry and the availability rule.
func TestChannels(t *testing.T) {
	ctx := context.Background()
	r := registry(t)
	uid := newUser(t, "ch1@x.org")
	// messengers and mail are off until an administrator switches them on
	var defaults int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM channels WHERE kind IN ('email','vkteams') AND NOT enabled AND NOT all_users`).Scan(&defaults)
	if defaults != 2 {
		t.Fatalf("mail and VK Teams are off by default: %d", defaults)
	}
	if _, err := r.Update(ctx, domain.ChannelWeb, channels.Patch{Enabled: t2(false)}, nil); code(err) != "channel_locked" {
		t.Fatalf("CH-01: %v", err)
	}
	if ok, why, _ := r.Available(ctx, uid, domain.ChannelWeb); !ok || why != channels.ReasonAlways {
		t.Fatal("web is always available")
	}
	// CH-03: enabled, not for everybody, the user is not in the list
	if _, err := r.Update(ctx, domain.ChannelVKTeams, channels.Patch{Enabled: t2(true), AllUsers: t2(false),
		Settings: json.RawMessage(`{"apiUrl":"https://vk.example/bot/v1/"}`), Secrets: map[string]string{"token": "s3cr3t"}}, channels.ValidateVKTeams); err != nil {
		t.Fatal(err)
	}
	if ok, why, _ := r.Available(ctx, uid, domain.ChannelVKTeams); ok || why != channels.ReasonDisabled {
		t.Fatalf("CH-03: %v %s", ok, why)
	}
	// CH-04: adding the user opens the channel at once on this pod
	if err := r.SetUsers(ctx, domain.ChannelVKTeams, []uuid.UUID{uid}, nil); err != nil {
		t.Fatal(err)
	}
	if ok, why, _ := r.Available(ctx, uid, domain.ChannelVKTeams); !ok || why != channels.ReasonUser {
		t.Fatalf("CH-04: %v %s", ok, why)
	}
	// CH-02: for everybody
	if _, err := r.Update(ctx, domain.ChannelVKTeams, channels.Patch{AllUsers: t2(true)}, nil); err != nil {
		t.Fatal(err)
	}
	other := newUser(t, "ch2@x.org")
	if ok, why, _ := r.Available(ctx, other, domain.ChannelVKTeams); !ok || why != channels.ReasonAllUsers {
		t.Fatalf("CH-02: %v %s", ok, why)
	}
	// secrets are stored encrypted and never listed with values
	c, err := r.Get(ctx, domain.ChannelVKTeams)
	if err != nil || len(c.Secrets) != 1 || c.Secrets[0] != "token" || strings.Contains(string(c.Settings), "s3cr3t") || c.UsersCount != 1 {
		t.Fatalf("%+v %v", c, err)
	}
	var raw []byte
	_ = pool.QueryRow(ctx, `SELECT secrets_enc FROM channels WHERE kind = 'vkteams'`).Scan(&raw)
	if strings.Contains(string(raw), "s3cr3t") {
		t.Fatal("the secret is stored in the clear")
	}
	var st channels.VKTeamsSettings
	sec, _, err := r.Config(ctx, domain.ChannelVKTeams, &st)
	if err != nil || sec["token"] != "s3cr3t" || st.APIURL != "https://vk.example/bot/v1" {
		t.Fatalf("%v %+v %v", sec, st, err)
	}
	// the card of a user: reasons; hammurapi only with its client
	list, err := r.OfUser(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	for _, uc := range list {
		if uc.Kind == domain.ChannelWeb && uc.Reason != channels.ReasonAlways {
			t.Fatalf("%+v", uc)
		}
	}
	if err := r.SetUserChannels(ctx, uid, map[string]bool{domain.ChannelWeb: false}); code(err) != "channel_locked" {
		t.Fatalf("web for a user: %v", err)
	}
	// the cache keeps the answer until the event or the TTL
	_, _ = pool.Exec(ctx, `UPDATE channels SET enabled = false WHERE kind = 'vkteams'`)
	if ok, _, _ := r.Available(ctx, other, domain.ChannelVKTeams); !ok {
		t.Fatal("the cache was expected to answer")
	}
	r.Invalidate()
	if ok, _, _ := r.Available(ctx, other, domain.ChannelVKTeams); ok {
		t.Fatal("the cache was not reset")
	}
}

// TG-01…TG-07: the personal key of Telegram and the binding.
func TestTelegramKeys(t *testing.T) {
	ctx := context.Background()
	k := &channels.Keys{Pool: pool, Pepper: channels.DerivePepper(key)}
	uid := newUser(t, "tg1@x.org")
	key1, err := k.Issue(ctx, uid)
	if err != nil || !strings.HasPrefix(key1, "NB-") || len(key1) != 17 || !channels.LooksLikeKey(key1) {
		t.Fatalf("TG-01: %q %v", key1, err)
	}
	// TG-02: only the HMAC is stored
	var hash []byte
	_ = pool.QueryRow(ctx, `SELECT key_hash FROM telegram_keys WHERE user_id = $1`, uid).Scan(&hash)
	if len(hash) != 32 || strings.Contains(string(hash), channels.NormalizeKey(key1)) {
		t.Fatal("TG-02")
	}
	key2, _ := k.Issue(ctx, uid) // TG-01: the reissue invalidates the old key
	if res, _ := k.Bind(ctx, 100, "ivan", key1); res.OK {
		t.Fatal("the old key works after the reissue")
	}
	// TG-04: lower case, no dashes
	res, err := k.Bind(ctx, 100, "ivan", strings.ToLower(strings.ReplaceAll(key2, "-", "")))
	if err != nil || !res.OK || res.UserID != uid {
		t.Fatalf("TG-04: %+v %v", res, err)
	}
	if got, ok, _ := k.UserOf(ctx, 100); !ok || got != uid {
		t.Fatal("binding")
	}
	if chat, ok := k.ChatOf(ctx, uid); !ok || chat != "100" {
		t.Fatal(chat)
	}
	// TG-06: a second account replaces the first, which is reported
	res, err = k.Bind(ctx, 200, "", key2)
	if err != nil || !res.OK || res.Previous != 100 {
		t.Fatalf("TG-06: %+v %v", res, err)
	}
	if _, ok, _ := k.UserOf(ctx, 100); ok {
		t.Fatal("the first account stays bound")
	}
	// TG-05: five wrong keys block the account for an hour; the key is not checked
	for i := 0; i < channels.MaxBindAttempts; i++ {
		if res, _ := k.Bind(ctx, 300, "", "NB-AAAA-AAAA-AAAA"); res.OK || res.Blocked {
			t.Fatalf("attempt %d: %+v", i, res)
		}
	}
	other := newUser(t, "tg2@x.org")
	good, _ := k.Issue(ctx, other)
	if res, _ := k.Bind(ctx, 300, "", good); !res.Blocked || res.OK {
		t.Fatalf("TG-05: %+v", res)
	}
	// TG-07: switching the channel off and on keeps the binding
	r := registry(t)
	if _, err := r.Update(ctx, domain.ChannelTelegram, channels.Patch{Enabled: t2(false)}, nil); err != nil {
		t.Fatal(err)
	}
	if r.Open(ctx, uid, domain.ChannelTelegram) {
		t.Fatal("the channel is off")
	}
	if _, err := r.Update(ctx, domain.ChannelTelegram, channels.Patch{Enabled: t2(true), AllUsers: t2(true)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := k.UserOf(ctx, 200); !ok || !r.Open(ctx, uid, domain.ChannelTelegram) {
		t.Fatal("TG-07")
	}
}

// fakeTelegram is a Bot API that records calls.
type fakeTelegram struct {
	mu       sync.Mutex
	calls    []string
	bodies   []string
	richFail bool
}

func (f *fakeTelegram) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.calls = append(f.calls, method)
		f.bodies = append(f.bodies, string(b))
		fail := f.richFail && method == "sendRichMessage"
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case fail:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":false,"description":"Bad Request: method not found"}`))
		case method == "getMe":
			_, _ = w.Write([]byte(`{"ok":true,"result":{"id":777,"username":"nabu_bot"}}`))
		case method == "getChatMemberCount":
			_, _ = w.Write([]byte(`{"ok":true,"result":5}`))
		default:
			_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
		}
	}))
}

func (f *fakeTelegram) said(method, part string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, c := range f.calls {
		if c == method && strings.Contains(f.bodies[i], part) {
			return true
		}
	}
	return false
}

func (f *fakeTelegram) count(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == method {
			n++
		}
	}
	return n
}

// TG-03, TG-08, TG-09, GR-01…GR-04, GR-09, GR-10, GR-11: the Telegram bot and group agents.
func TestTelegramBotAndGroups(t *testing.T) {
	ctx := context.Background()
	f := &fakeTelegram{}
	srv := f.server()
	defer srv.Close()
	r := registry(t)
	if _, err := r.Update(ctx, domain.ChannelTelegram, channels.Patch{Enabled: t2(true), AllUsers: t2(true), GroupsEnabled: t2(false)}, nil); err != nil {
		t.Fatal(err)
	}
	keys := &channels.Keys{Pool: pool, Pepper: key}
	bot := channels.NewTelegram(srv.URL, "TOKEN", "hooksecret")
	in := &inbox{}
	s3 := &listS3{mem{m: map[string][]byte{}}}
	acc := &accounts.Service{Pool: pool, S3: s3, Ledger: &ledger.Ledger{Pool: pool}}
	gs := &groups.Service{Pool: pool, Registry: r, Users: repo(), Inbox: in, RetentionDays: acc.Retention, DeleteData: acc.DeleteUserData}
	wh := &channels.Webhook{Bot: bot, Keys: keys, Registry: r, Users: repo(), Inbox: in, Groups: gs, MaxFile: 1 << 20, WebURL: "https://nabu.x"}
	private := func(from int64, text string) channels.Update {
		return channels.Update{Message: &channels.TGMessage{From: &channels.TGUser{ID: from, First: "Ivan"}, Chat: channels.TGChat{ID: from, Type: "private"}, Text: text}}
	}
	owner := newUser(t, "gr-owner@x.org")
	// TG-03: the bot asks for the key, then binds
	wh.Handle(ctx, private(9001, "привет"))
	if !f.said("sendMessage", "Nabu") || len(in.got) != 0 {
		t.Fatalf("TG-03: the key was not asked: %v", f.calls)
	}
	k, _ := keys.Issue(ctx, owner)
	wh.Handle(ctx, private(9001, k))
	if got, ok, _ := keys.UserOf(ctx, 9001); !ok || got != owner {
		t.Fatal("TG-03: not bound")
	}
	wh.Handle(ctx, private(9001, "что нового?"))
	if len(in.got) != 1 || in.got[0].UserID != owner || in.got[0].Channel != domain.ChannelTelegram {
		t.Fatalf("%+v", in.got)
	}
	// R2: the channel switched off for everybody but not enabled for the user
	if _, err := r.Update(ctx, domain.ChannelTelegram, channels.Patch{AllUsers: t2(false)}, nil); err != nil {
		t.Fatal(err)
	}
	wh.Handle(ctx, private(9001, "ещё вопрос"))
	if len(in.got) != 1 {
		t.Fatal("a message of a closed channel reached the agent")
	}
	if _, err := r.Update(ctx, domain.ChannelTelegram, channels.Patch{AllUsers: t2(true)}, nil); err != nil {
		t.Fatal(err)
	}

	// TG-08, TG-09: a rich message, HTML when the rich method fails
	if err := bot.Send(ctx, "9001", "| a | b |\n|---|---|\n| 1 | 2 |"); err != nil || !f.said("sendRichMessage", `"type":"table"`) {
		t.Fatalf("TG-08: %v %v", err, f.calls)
	}
	f.mu.Lock()
	f.richFail = true
	f.mu.Unlock()
	if err := bot.Send(ctx, "9001", "**важно**\n\n| a | b |\n|---|---|\n| 1 | 2 |"); err != nil || !f.said("sendMessage", `важно`) || !f.said("sendMessage", `"parse_mode":"HTML"`) || !f.said("sendMessage", `pre`) {
		t.Fatalf("TG-09: %v %v", err, f.bodies)
	}
	if err := bot.Draft(ctx, "9001", 42, "черновик"); err != nil || !f.said("sendMessageDraft", `"draft_id":42`) {
		t.Fatal("TG-10: draft")
	}

	added := func(chat int64, from int64, status string) channels.Update {
		u := channels.Update{}
		_ = json.Unmarshal([]byte(`{"my_chat_member":{"chat":{"id":`+itoa(chat)+`,"type":"supergroup","title":"Платформа"},"from":{"id":`+itoa(from)+
			`},"new_chat_member":{"status":"`+status+`"}}}`), &u)
		return u
	}
	// GR-02: groups are off — the bot explains and leaves
	wh.Handle(ctx, added(-500, 9001, "member"))
	if f.count("leaveChat") != 1 {
		t.Fatalf("GR-02 (groups off): %v", f.calls)
	}
	if _, err := r.Update(ctx, domain.ChannelTelegram, channels.Patch{GroupsEnabled: t2(true)}, nil); err != nil {
		t.Fatal(err)
	}
	// GR-02: added by an account that is not linked
	wh.Handle(ctx, added(-500, 4242, "member"))
	if f.count("leaveChat") != 2 {
		t.Fatal("GR-02 (not linked)")
	}
	// GR-01: a linked user with the channel — the group agent with the owner
	wh.Handle(ctx, added(-500, 9001, "member"))
	ga, err := gs.ByChat(ctx, domain.ChannelTelegram, "-500")
	if err != nil || ga == nil || ga.Status != groups.Active || ga.Owner == nil || ga.Owner.ID != owner || ga.DataUserID == nil ||
		ga.MembersCount == nil || *ga.MembersCount != 5 || f.count("leaveChat") != 2 {
		t.Fatalf("GR-01: %+v %v", ga, err)
	}
	// the technical user of the group is not a user of the administration
	if l, _ := repo().List(ctx, "groups.nabu.invalid", "", 0, 10); len(l) != 0 {
		t.Fatal("the group user is listed")
	}
	groupMsg := func(from int64, text string, mention bool) channels.Update {
		m := &channels.TGMessage{From: &channels.TGUser{ID: from, First: "Пётр"}, Chat: channels.TGChat{ID: -500, Type: "supergroup", Title: "Платформа"}, Text: text}
		u := channels.Update{Message: m}
		if mention {
			b, _ := json.Marshal(u)
			var raw map[string]map[string]any
			_ = json.Unmarshal(b, &raw)
			raw["message"]["entities"] = []map[string]any{{"type": "mention", "offset": 0, "length": len([]rune("@nabu_bot"))}}
			b, _ = json.Marshal(raw)
			_ = json.Unmarshal(b, &u)
		}
		return u
	}
	before := len(in.got)
	// GR-03: without a mention nothing is passed or stored
	wh.Handle(ctx, groupMsg(9001, "обсуждаем релиз", false))
	if len(in.got) != before {
		t.Fatal("GR-03")
	}
	// GR-04: a mention by a participant without a Nabu account
	wh.Handle(ctx, groupMsg(5555, "@nabu_bot что с алертами?", true))
	if len(in.got) != before || !f.said("sendMessage", "Nabu") {
		t.Fatal("GR-04")
	}
	// an address of a member goes to the conversation of the group, with the author
	wh.Handle(ctx, groupMsg(9001, "@nabu_bot что с алертами?", true))
	if len(in.got) != before+1 || in.got[before].UserID != *ga.DataUserID || in.got[before].Text != "что с алертами?" ||
		!strings.Contains(string(in.got[before].Context), "gr-owner@x.org") {
		t.Fatalf("group message: %+v", in.got[len(in.got)-1])
	}
	// GR-07: the session of a group agent gets platform items only
	cat := &catalog.Service{Pool: pool, Box: r.Box, Signer: jwt.NewSigner("https://nabu-api.x", key), InternalURL: "http://api:8081"}
	sm, err := cat.ForGroup(ctx, ga.ID, nil, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range sm.Servers {
		var mode string
		_ = pool.QueryRow(ctx, `SELECT mode FROM catalog_items WHERE name = $1`, s.Name).Scan(&mode)
		if mode != "platform" {
			t.Fatalf("GR-07: %s is %s", s.Name, mode)
		}
	}
	// GR-09: the ownership goes only to a member with access to the channel
	admin := &domain.Principal{UserID: newUser(t, "gr-admin@x.org"), IsAdmin: true}
	stranger := newUser(t, "gr-stranger@x.org")
	if _, err := gs.Update(ctx, admin, ga.ID, groups.Patch{OwnerID: &stranger}, nil); code(err) != "invalid_owner" {
		t.Fatalf("GR-09: %v", err)
	}
	notOwner := &domain.Principal{UserID: stranger}
	name := "Платон"
	if _, err := gs.Update(ctx, notOwner, ga.ID, groups.Patch{Name: &name}, nil); code(err) != "group_agent_forbidden" {
		t.Fatalf("not the owner: %v", err)
	}
	if a, err := gs.Update(ctx, &domain.Principal{UserID: owner}, ga.ID, groups.Patch{Name: &name}, nil); err != nil || a.Name != "Платон" {
		t.Fatalf("owner: %v", err)
	}
	// GR-10: removed and added again by the same owner — the data return
	_, _ = pool.Exec(ctx, `INSERT INTO memories (user_id, text, source) VALUES ($1,'Релизы по четвергам','agent')`, *ga.DataUserID)
	wh.Handle(ctx, added(-500, 9001, "left"))
	ga, _ = gs.ByChat(ctx, domain.ChannelTelegram, "-500")
	if ga.Status != groups.Removed || ga.DataUntil == nil || time.Until(*ga.DataUntil) < 179*24*time.Hour {
		t.Fatalf("R17: %+v", ga)
	}
	dataUser := *ga.DataUserID
	wh.Handle(ctx, added(-500, 9001, "member"))
	ga, _ = gs.ByChat(ctx, domain.ChannelTelegram, "-500")
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM memories WHERE user_id = $1`, dataUser).Scan(&n)
	if ga.Status != groups.Active || *ga.DataUserID != dataUser || n != 1 || ga.Name != "Платон" {
		t.Fatalf("GR-10: %+v %d", ga, n)
	}
	// an agent switched off by an administrator stays off: neither a change of
	// the bot's rights nor its return to the chat switches it on
	off := groups.Disabled
	if _, err := gs.Update(ctx, admin, ga.ID, groups.Patch{Status: &off}, nil); err != nil {
		t.Fatal(err)
	}
	promoted := channels.Update{}
	_ = json.Unmarshal([]byte(`{"my_chat_member":{"chat":{"id":-500,"type":"supergroup"},"from":{"id":9001},
		"old_chat_member":{"status":"member"},"new_chat_member":{"status":"administrator"}}}`), &promoted)
	left := f.count("leaveChat")
	for _, u := range []channels.Update{promoted, added(-500, 9001, "left"), added(-500, 9001, "member")} {
		wh.Handle(ctx, u)
		if ga, _ = gs.ByChat(ctx, domain.ChannelTelegram, "-500"); ga.Status != groups.Disabled || ga.DataUntil == nil || *ga.DataUserID != dataUser {
			t.Fatalf("a switched off agent: %+v", ga)
		}
	}
	if f.count("leaveChat") != left {
		t.Fatal("the bot left the chat of a switched off agent")
	}
	on := groups.Active
	if _, err := gs.Update(ctx, admin, ga.ID, groups.Patch{Status: &on}, nil); err != nil {
		t.Fatal(err)
	}
	// GR-11: the purge deletes the data after data_until
	s3.m["spaces/"+dataUser.String()+"/notes.md"] = []byte("x")
	wh.Handle(ctx, added(-500, 9001, "kicked"))
	_, _ = pool.Exec(ctx, `UPDATE group_agents SET data_until = now() - interval '1 day' WHERE id = $1`, ga.ID)
	if _, err := acc.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	acc.Groups = gs
	if _, err := acc.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE id = $1`, dataUser).Scan(&n)
	if again, _ := gs.ByChat(ctx, domain.ChannelTelegram, "-500"); again != nil || n != 0 || len(s3.m) != 0 {
		t.Fatalf("GR-11: %v %d %v", again, n, s3.m)
	}
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// AR-01…AR-15: archiving, restoring and the purge.
func TestAccounts(t *testing.T) {
	ctx := context.Background()
	s3 := &listS3{mem{m: map[string][]byte{}}}
	r := registry(t)
	keys := &channels.Keys{Pool: pool, Pepper: key}
	in := &inbox{}
	acc := &accounts.Service{Pool: pool, S3: s3, Ledger: &ledger.Ledger{Pool: pool}}
	gs := &groups.Service{Pool: pool, Registry: r, Users: repo(), Inbox: in, RetentionDays: acc.Retention, DeleteData: acc.DeleteUserData}
	acc.Groups = gs
	stopped, closed := 0, 0
	fail := true
	acc.Sandboxes = sandboxes(func() error {
		if fail {
			return io.ErrUnexpectedEOF
		}
		stopped++
		return nil
	})
	acc.CloseSessions = func(context.Context, uuid.UUID) error { closed++; return nil }
	if err := (&ledger.Ledger{Pool: pool}).Partitions(ctx, 365*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	adminID := newUser(t, "ar-admin@x.org")
	_, _ = pool.Exec(ctx, `UPDATE users SET is_admin = true WHERE id = $1`, adminID)
	admin := accounts.Actor{UserID: &adminID, Email: "ar-admin@x.org"}
	uid := newUser(t, "ar1@x.org")
	other := newUser(t, "ar2@x.org")
	_, _ = acc.Archive(ctx, []string{"ar2@x.org"}, accounts.ArchiveOptions{}, admin)
	fail = false
	for {
		if done, err := acc.RunOne(ctx); err != nil || !done {
			break
		}
	}
	fail, stopped, closed = true, 0, 0

	// the state of an employee: a session, a task, a personal access, Telegram, a group agent, data
	if _, err := repo().CreateSession(ctx, uid, "csrf"); err != nil {
		t.Fatal(err)
	}
	ts := &tasks.Service{Pool: pool, Bus: &bus{}, Rules: tasks.Rules{MinInterval: time.Minute, MaxActive: 5}, Catchup: time.Hour, Tick: time.Second,
		MaxFailures: 3, DefaultTimezone: "Europe/Moscow"}
	ti := tasks.CreateInput{Title: "Сводка", Instruction: "Сводка"}
	ti.Schedule.Cron = "0 10 * * 1"
	task, err := ts.Create(ctx, uid, ti, "web", nil)
	if err != nil {
		t.Fatal(err)
	}
	var item uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO catalog_items (type, name, source, mode) VALUES ('mcp','ar-jira','{"kind":"url","url":"https://j"}','personal') RETURNING id`).Scan(&item); err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `INSERT INTO user_connections (user_id, item_id) VALUES ($1,$2)`, uid, item)
	k, _ := keys.Issue(ctx, uid)
	if res, _ := keys.Bind(ctx, 7001, "", k); !res.OK {
		t.Fatal("bind")
	}
	if _, err := r.Update(ctx, domain.ChannelTelegram, channels.Patch{Enabled: t2(true), AllUsers: t2(true), GroupsEnabled: t2(true)}, nil); err != nil {
		t.Fatal(err)
	}
	for _, chat := range []string{"-701", "-702"} {
		if _, ok := gs.Added(ctx, channels.GroupEvent{Channel: domain.ChannelTelegram, ChatID: chat, Title: "G" + chat, AdderID: uid}); !ok {
			t.Fatal("group")
		}
	}
	_, _ = pool.Exec(ctx, `INSERT INTO memories (user_id, text, source) VALUES ($1,'Любит отчёты по понедельникам','agent')`, uid)
	store := &chat.Store{Pool: pool}
	main, _ := store.Main(ctx, uid)
	if _, err := store.AddUser(ctx, main.ID, "привет", "web", nil, nil); err != nil {
		t.Fatal(err)
	}
	s3.m["spaces/"+uid.String()+"/report.md"] = []byte("x")
	s3.m["attachments/"+uid.String()+"/a"] = []byte("x")
	s3.m["sessions/"+main.ID.String()+".jsonl"] = []byte("x")
	s3.m["spaces/"+adminID.String()+"/keep.md"] = []byte("x")

	// AR-13: active, already archived, unknown
	res, err := acc.Archive(ctx, []string{" AR1@x.org", "ar2@x.org", "nobody@x.org"}, accounts.ArchiveOptions{GroupAgents: accounts.GroupsTransfer}, admin)
	if err != nil || len(res) != 3 || res[0].Result != accounts.Archived || res[1].Result != accounts.AlreadyArchived || res[2].Result != accounts.NotFound {
		t.Fatalf("AR-13: %+v %v", res, err)
	}
	if _, err := acc.Archive(ctx, []string{"ar-admin@x.org"}, accounts.ArchiveOptions{}, admin); code(err) != "cannot_archive_self" {
		t.Fatalf("self: %v", err)
	}
	// AR-01: closed at once — the sign-in, the sessions and the channels
	u, _ := repo().Get(ctx, uid)
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_sessions WHERE user_id = $1`, uid).Scan(&n)
	if u.Status != "archived" || u.ArchivedBy == nil || *u.ArchivedBy != "ar-admin@x.org" || u.PurgeAfter == nil || n != 0 {
		t.Fatalf("AR-01 mark: %+v %d", u, n)
	}
	r.Invalidate()
	if r.Open(ctx, uid, domain.ChannelTelegram) || repo().Allowed(ctx, uid) {
		t.Fatal("AR-04: the channels stay open")
	}
	if p, _ := repo().Principal(ctx, uid); p == nil || !p.Archived {
		t.Fatal("the principal is not archived")
	}
	// AR-02: a failure in the middle; the retry resumes at the failed step
	for {
		done, err := acc.RunOne(ctx)
		if err != nil {
			break
		}
		if !done {
			t.Fatal("the failing step was expected")
		}
	}
	var step, status string
	_ = pool.QueryRow(ctx, `SELECT step, status FROM account_jobs WHERE user_id = $1`, uid).Scan(&step, &status)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_connections WHERE user_id = $1`, uid).Scan(&n)
	if step != "sandbox" || status != "running" || n != 1 || closed != 1 {
		t.Fatalf("AR-02: %s %s %d %d", step, status, n, closed)
	}
	fail = false
	for {
		done, err := acc.RunOne(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			break
		}
	}
	_ = pool.QueryRow(ctx, `SELECT step, status FROM account_jobs WHERE user_id = $1`, uid).Scan(&step, &status)
	if status != "done" || stopped != 1 || closed != 1 {
		t.Fatalf("AR-02: %s %s %d %d", step, status, stopped, closed)
	}
	// AR-01: tasks paused, the personal access and Telegram removed, the data kept
	tk, _ := ts.Get(ctx, uid, task.ID)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_connections WHERE user_id = $1`, uid).Scan(&n)
	_, bound, _ := keys.UserOf(ctx, 7001)
	hasKey, _ := keys.HasKey(ctx, uid)
	var mems, msgs int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM memories WHERE user_id = $1`, uid).Scan(&mems)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1`, main.ID).Scan(&msgs)
	if tk.Status != "paused" || n != 0 || bound || hasKey || mems != 1 || msgs != 1 || len(s3.m) != 4 {
		t.Fatalf("AR-01: %s %d %v %v %d %d %d", tk.Status, n, bound, hasKey, mems, msgs, len(s3.m))
	}
	// AR-03: group agents go to the administrator
	for _, a := range mustList(t, gs) {
		if a.Owner == nil || a.Owner.ID != adminID || a.Status != groups.Active {
			t.Fatalf("AR-03 transfer: %+v", a)
		}
	}
	// AR-15: the audit names who archived
	var ini, tool string
	_ = pool.QueryRow(ctx, `SELECT COALESCE(initiator_email,''), tool FROM audit WHERE user_id = $1 ORDER BY at DESC LIMIT 1`, uid).Scan(&ini, &tool)
	if tool != "account.archive" || ini != "ar-admin@x.org" {
		t.Fatalf("AR-15: %s %s", tool, ini)
	}

	// AR-05: the sign-in of an archived user with a new ID of the provider
	su, created, err := repo().SignIn(ctx, users.Identity{Issuer: "t", Subject: "new-subject", Email: "ar1@x.org"}, false)
	if err != nil || created || su.ID != uid || su.Status != "archived" || !su.NewIdentity {
		t.Fatalf("AR-05: %+v %v %v", su, created, err)
	}
	if err := acc.RequestRestore(ctx, uid, &accounts.Identity{Issuer: "t", Subject: "new-subject"}); err != nil {
		t.Fatal(err)
	}
	_ = acc.RequestRestore(ctx, uid, nil) // a repeated sign-in keeps one request and the identity
	reqs, err := acc.Requests(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var req *accounts.Request
	for i := range reqs {
		if reqs[i].UserID == uid {
			req = &reqs[i]
		}
	}
	if req == nil || req.NewIdentity == nil || req.NewIdentity.Subject != "new-subject" {
		t.Fatalf("AR-05: %+v", reqs)
	}
	// AR-06: restoring with the link — the new identity signs in to the old account
	if res, err := acc.Restore(ctx, uid, &req.ID, false, admin); err != nil || res != accounts.Restored {
		t.Fatalf("AR-06: %s %v", res, err)
	}
	su, _, err = repo().SignIn(ctx, users.Identity{Issuer: "t", Subject: "new-subject", Email: "ar1@x.org"}, false)
	if err != nil || su.ID != uid || su.Status != "active" || su.PurgeAfter != nil {
		t.Fatalf("AR-06: %+v %v", su, err)
	}
	// AR-08: tasks stay paused; Telegram and personal accesses do not return
	tk, _ = ts.Get(ctx, uid, task.ID)
	_, bound, _ = keys.UserOf(ctx, 7001)
	if tk.Status != "paused" || bound || mems != 1 {
		t.Fatalf("AR-08: %s %v", tk.Status, bound)
	}
	if res, _ := acc.Restore(ctx, uid, nil, false, admin); res != accounts.AlreadyActive {
		t.Fatal(res)
	}

	// AR-03: «disable»; AR-07: restoring through the API links the identity of the next sign-in
	apiClient := uuid.New()
	_, _ = pool.Exec(ctx, `INSERT INTO service_clients (id, name, client_id, secret_hash) VALUES ($1,'hr','hr','x')`, apiClient)
	hr := accounts.Actor{ClientID: &apiClient, Client: "hr", Initiator: "hr-bot@x.org"}
	_, _ = pool.Exec(ctx, `UPDATE group_agents SET owner_id = $1 WHERE external_chat_id = '-702'`, uid)
	if res, err := acc.Archive(ctx, []string{"ar1@x.org"}, accounts.ArchiveOptions{GroupAgents: accounts.GroupsDisable}, hr); err != nil || res[0].Result != accounts.Archived {
		t.Fatalf("%+v %v", res, err)
	}
	for {
		done, err := acc.RunOne(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			break
		}
	}
	g702, _ := gs.ByChat(ctx, domain.ChannelTelegram, "-702")
	u, _ = repo().Get(ctx, uid)
	if g702.Status != groups.Disabled || g702.DataUntil == nil || *u.ArchivedBy != "api:hr" {
		t.Fatalf("AR-03 disable: %+v %v", g702, u.ArchivedBy)
	}
	_ = pool.QueryRow(ctx, `SELECT COALESCE(initiator_email,''), tool FROM audit WHERE user_id = $1 AND channel = 'client:hr' ORDER BY at DESC, id DESC LIMIT 1`, uid).Scan(&ini, &tool)
	if ini != "hr-bot@x.org" {
		t.Fatalf("AR-15: the initiator of the client: %q", ini)
	}
	rr, err := acc.RestoreEmails(ctx, []string{"ar1@x.org", "ar-admin@x.org", "nobody@x.org"}, hr)
	if err != nil || rr[0].Result != accounts.Restored || rr[1].Result != accounts.AlreadyActive || rr[2].Result != accounts.NotFound {
		t.Fatalf("AR-07: %+v %v", rr, err)
	}
	su, _, err = repo().SignIn(ctx, users.Identity{Issuer: "t", Subject: "third-subject", Email: "ar1@x.org"}, false)
	if err != nil || su.ID != uid || su.Status != "active" {
		t.Fatalf("AR-07: %+v %v", su, err)
	}
	// the flag works once: one more unknown identity is refused
	if _, _, err := repo().SignIn(ctx, users.Identity{Issuer: "t", Subject: "fourth", Email: "ar1@x.org"}, false); code(err) != "identity_conflict" {
		t.Fatalf("the link flag works once: %v", err)
	}

	// AR-09: the retention with dryRun, then saving recalculates purge_after
	_, _ = pool.Exec(ctx, `UPDATE users SET archived_at = now() - interval '100 days', purge_after = now() + interval '80 days' WHERE id = $1`, other)
	if n, err := acc.SetRetention(ctx, 90, true); err != nil || n != 1 || acc.Retention(ctx) != 180 {
		t.Fatalf("AR-09 dryRun: %d %v %d", n, err, acc.Retention(ctx))
	}
	// AR-10: the filter «deleted within N days»
	if l, _ := repo().List(ctx, "", "archived", 7, 100); len(l) != 0 {
		t.Fatalf("AR-10: %d", len(l))
	}
	if _, err := acc.SetRetention(ctx, 90, false); err != nil || acc.Retention(ctx) != 90 {
		t.Fatal(err)
	}
	if l, _ := repo().List(ctx, "", "archived", 7, 100); len(l) != 1 || l[0].ID != other {
		t.Fatalf("AR-10: %+v", l)
	}
	// AR-11: the purge deletes rows and objects; the audit stays
	if _, err := acc.Archive(ctx, []string{"ar1@x.org"}, accounts.ArchiveOptions{}, admin); err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `UPDATE users SET purge_after = now() - interval '1 hour' WHERE id = $1`, uid)
	purged, err := acc.Purge(ctx)
	if err != nil || purged != 2 {
		t.Fatalf("AR-11: %d %v", purged, err)
	}
	gone, _ := repo().Get(ctx, uid)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM audit WHERE user_id = $1`, uid).Scan(&n)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1`, main.ID).Scan(&msgs)
	if gone != nil || msgs != 0 || n == 0 || len(s3.m) != 1 {
		t.Fatalf("AR-11: %v %d %d %v", gone, msgs, n, s3.m)
	}
	// AR-13 «purged»; AR-12: the sign-in after the purge creates a new account
	if res, _ := acc.Archive(ctx, []string{"ar1@x.org"}, accounts.ArchiveOptions{}, admin); res[0].Result != accounts.Purged {
		t.Fatalf("purged: %+v", res)
	}
	nu, created, err := repo().SignIn(ctx, users.Identity{Issuer: "t", Subject: "new-subject", Email: "ar1@x.org"}, false)
	if err != nil || !created || nu.ID == uid {
		t.Fatalf("AR-12: %+v %v %v", nu, created, err)
	}

	// AR-14: the client API needs its right; CH-08 and user_archived of delegation
	rt := chi.NewRouter()
	cl := &httpx.Client{ID: apiClient, Name: "hammurapi", CanDelegate: true, CanArchive: true}
	rt.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(httpx.WithClient(r.Context(), cl)))
		})
	})
	acc.ClientRoutes(rt)
	rt.Route("/me", func(sub chi.Router) {
		sub.Use(auth.Delegation(repo(), r.Open))
		sub.Get("/ping", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	})
	call := func(method, path, body, behalf string) (int, string) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if behalf != "" {
			req.Header.Set("Nabu-On-Behalf-Of", behalf)
		}
		w := httptest.NewRecorder()
		rt.ServeHTTP(w, req)
		return w.Code, w.Body.String()
	}
	if c, b := call("POST", "/users:restore", `{"emails":["a@x.org"]}`, ""); c != 403 {
		t.Fatalf("AR-14: %d %s", c, b)
	}
	if c, b := call("POST", "/users:archive", `{"emails":["ar-keep@x.org"],"initiator":"hr"}`, ""); c != 200 || !strings.Contains(b, "not_found") {
		t.Fatalf("client archive: %d %s", c, b)
	}
	deleg := newUser(t, "ar-deleg@x.org")
	if c, _ := call("GET", "/me/ping", "", "ar-deleg@x.org"); c != 204 {
		t.Fatalf("delegation: %d", c)
	}
	if _, err := r.Update(ctx, domain.ChannelHammurapi, channels.Patch{AllUsers: t2(false)}, nil); err != nil {
		t.Fatal(err)
	}
	if c, b := call("GET", "/me/ping", "", "ar-deleg@x.org"); c != 403 || !strings.Contains(b, "channel_unavailable") {
		t.Fatalf("CH-08: %d %s", c, b)
	}
	if _, err := r.Update(ctx, domain.ChannelHammurapi, channels.Patch{AllUsers: t2(true)}, nil); err != nil {
		t.Fatal(err)
	}
	_, _ = acc.Archive(ctx, []string{"ar-deleg@x.org"}, accounts.ArchiveOptions{}, admin)
	if c, b := call("GET", "/me/ping", "", "ar-deleg@x.org"); c != 403 || !strings.Contains(b, "user_archived") {
		t.Fatalf("user_archived: %d %s", c, b)
	}
	_ = deleg
}

type sandboxes func() error

func (s sandboxes) Stop(context.Context, uuid.UUID) error { return s() }

func mustList(t *testing.T, gs *groups.Service) []groups.Agent {
	t.Helper()
	l, err := gs.List(context.Background(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var out []groups.Agent
	for _, a := range l {
		if a.ChatID == "-701" || a.ChatID == "-702" {
			out = append(out, a)
		}
	}
	if len(out) != 2 {
		t.Fatalf("group agents: %d", len(out))
	}
	return out
}

func mailLetter(headers, body string) []byte {
	return []byte(strings.ReplaceAll(headers, "\n", "\r\n") + "\r\n" + body)
}

// ML-01, ML-02, ML-06, ML-07, ML-10, ML-11, ML-14, ML-15, ML-21, ML-22, CH-07: the mail channel.
func TestMail(t *testing.T) {
	ctx := context.Background()
	r := registry(t)
	if _, err := r.Update(ctx, domain.ChannelEmail, channels.Patch{Enabled: t2(true), AllUsers: t2(true),
		Settings: json.RawMessage(`{"provider":"yandex360","mailbox":"nabu@company.ru","aliases":["ai@company.ru"],"domains":["company.ru"]}`),
		Secrets:  map[string]string{email.SecretAppPassword: "app-pass"}}, email.Validate); err != nil {
		t.Fatal(err)
	}
	// the sender signs in with the corporate address
	uid := newUser(t, "ivan@company.ru")
	hub := events.NewHub()
	box, _ := crypto.NewBox(key)
	s3 := &mem{m: map[string][]byte{}}
	api := &chat.API{Store: &chat.Store{Pool: pool}, Pool: pool, Users: repo(), Bus: &bus{}, Events: hub, S3: s3, MaxFile: 1 << 20}
	var sent []string
	files := &spaceFiles{}
	m := &email.Mail{Pool: pool, Registry: r, Inbox: api, Attachments: api.Attachments(), Events: hub, WebURL: "https://nabu.company.ru",
		ReplyLimit: 2, AttachMax: 10, InboundMax: 1 << 20, Space: files,
		Accounts: func(ctx context.Context, addr string) (*email.Account, error) {
			u, err := repo().ByEmail(ctx, addr)
			if err != nil || u == nil {
				return nil, err
			}
			return &email.Account{ID: u.ID, Status: u.Status, Language: u.Language}, nil
		},
		Send: func(_ context.Context, _ email.Credentials, _ string, to []string, msg []byte) error {
			sent = append(sent, strings.Join(to, ",")+"\n"+string(msg))
			return nil
		}}
	c, enabled, err := m.Credentials(ctx)
	if err != nil || !enabled || c.AppPassword != "app-pass" || c.Settings.IMAP.Host != "imap.yandex.ru" {
		t.Fatalf("%+v %v", c, err)
	}
	const ok = "Authentication-Results: mxback1.mail.yandex.net; dkim=pass header.d=company.ru; dmarc=pass header.from=company.ru\n"
	first := mailLetter(ok+"From: Иван <ivan@company.ru>\nTo: nabu@company.ru\nSubject: Отчёт за неделю\nMessage-ID: <m1@company.ru>\n"+
		"Content-Type: text/plain; charset=utf-8\n", "Собери отчёт.\n")
	// ML-01: a new letter — a topic titled by the subject
	if res := m.Process(ctx, c, first, "uid:1/1"); res != email.Accepted {
		t.Fatalf("ML-01: %s", res)
	}
	store := &chat.Store{Pool: pool}
	convs, _ := store.List(ctx, uid, false)
	var topic *chat.Conversation
	for i := range convs {
		if convs[i].Kind == "topic" {
			topic = &convs[i]
		}
	}
	if topic == nil || *topic.Title != "Отчёт за неделю" || topic.Source != "email" || !topic.WritesRequireConfirmation {
		t.Fatalf("ML-01: %+v", convs)
	}
	msgs, _ := store.History(ctx, topic.ID, 10)
	if len(msgs) != 1 || msgs[0].Channel != domain.ChannelEmail || msgs[0].Text != "Собери отчёт." || !strings.Contains(strings.ReplaceAll(string(msgs[0].Context), " ", ""), `"mode":"direct"`) {
		t.Fatalf("%+v", msgs)
	}
	// ML-06: the same letter is handled once
	if res := m.Process(ctx, c, first, "uid:1/1"); res != "" {
		t.Fatalf("ML-06: %q", res)
	}
	// ML-02: an answer in the thread continues the topic; the bot in copy — web only (ML-11)
	second := mailLetter(ok+"From: ivan@company.ru\nTo: petr@company.ru\nCc: nabu@company.ru\nSubject: Re: Отчёт за неделю\nMessage-ID: <m2@company.ru>\n"+
		"In-Reply-To: <m1@company.ru>\nReferences: <m1@company.ru>\n", "Пётр, посмотри.\n")
	if res := m.Process(ctx, c, second, "uid:1/2"); res != email.Accepted {
		t.Fatal(res)
	}
	if msgs, _ = store.History(ctx, topic.ID, 10); len(msgs) != 2 {
		t.Fatalf("ML-02: %d messages in the topic", len(msgs))
	}
	since := time.Now().Add(-time.Minute)
	if err := m.Reply(ctx, uid, topic.ID, "Готово.", since); err != nil || len(sent) != 0 {
		t.Fatalf("ML-11: a letter was sent: %v %d", err, len(sent))
	}
	cv, _ := store.Get(ctx, uid, topic.ID)
	if cv.UnreadCount != 1 {
		t.Fatalf("ML-11: unread %d", cv.UnreadCount)
	}
	if err := store.Read(ctx, uid, topic.ID); err != nil {
		t.Fatal(err)
	}
	// ML-10, ML-14, ML-16: the bot is the only recipient — a letter in the thread to the sender only
	third := mailLetter(ok+"From: ivan@company.ru\nTo: ai@company.ru\nSubject: Re: Отчёт за неделю\nMessage-ID: <m3@company.ru>\n"+
		"In-Reply-To: <m2@company.ru>\nReferences: <m1@company.ru> <m2@company.ru>\n", "А теперь таблицей.\n")
	if res := m.Process(ctx, c, third, "uid:1/3"); res != email.Accepted {
		t.Fatal(res)
	}
	files.list = []email.SpaceFile{{Path: "reports/small.csv", Size: 4}, {Path: "reports/big.bin", Size: 15}}
	if err := m.Reply(ctx, uid, topic.ID, "| a | b |\n|---|---|\n| 1 | 2 |", since); err != nil || len(sent) != 1 {
		t.Fatalf("ML-10: %v %d", err, len(sent))
	}
	letter := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(sent[0], "=\r\n", ""), "=\n", ""), "=3D", "=") // quoted-printable
	for _, want := range []string{"ivan@company.ru\n", "In-Reply-To: <m3@company.ru>", "<m1@company.ru>", "Subject: =?utf-8?q?Re:", "<table",
		"https://nabu.company.ru/chat/" + topic.ID.String(), "small.csv", "space?path=reports%2Fbig.bin", "text/plain"} {
		if !strings.Contains(letter, want) {
			t.Fatalf("ML-10/14/16: %q is not in the letter:\n%s", want, letter)
		}
	}
	if strings.Contains(letter, "petr@company.ru") {
		t.Fatal("ML-10: the answer went to a colleague")
	}
	// ML-15: the limit of letters to an address per hour
	_ = m.Reply(ctx, uid, topic.ID, "ещё", since)
	_ = m.Reply(ctx, uid, topic.ID, "и ещё", since)
	cv, _ = store.Get(ctx, uid, topic.ID)
	if len(sent) != 2 || cv.UnreadCount != 1 {
		t.Fatalf("ML-15: %d letters, unread %d", len(sent), cv.UnreadCount)
	}
	// ML-07, ML-22: a forged or external letter is rejected and logged; nobody is answered
	before := len(sent)
	forged := mailLetter("From: ivan@company.ru\nTo: nabu@company.ru\nSubject: x\nMessage-ID: <f1@evil>\n", "создай задачу")
	ext := mailLetter("Authentication-Results: mx.mail.yandex.net; dmarc=pass header.from=gmail.com\nFrom: a@gmail.com\nTo: nabu@company.ru\nMessage-ID: <e1@gmail.com>\n", "hi")
	if m.Process(ctx, c, forged, "k1") != email.Rejected || m.Process(ctx, c, ext, "k2") != email.Rejected || len(sent) != before {
		t.Fatal("ML-07, ML-22")
	}
	var reason string
	_ = pool.QueryRow(ctx, `SELECT reason FROM email_log WHERE message_id = 'f1@evil' AND result = 'rejected'`).Scan(&reason)
	if !strings.Contains(reason, "Authentication-Results") {
		t.Fatalf("the log: %q", reason)
	}
	// ML-21: a corporate address without an account — one short letter a day
	for i, id := range []string{"n1", "n2"} {
		l := mailLetter(ok+"From: novice@company.ru\nTo: nabu@company.ru\nSubject: Привет\nMessage-ID: <"+id+"@company.ru>\n", "привет")
		if res := m.Process(ctx, c, l, "k"+id); res != email.Unavailable {
			t.Fatalf("ML-21 %d: %s", i, res)
		}
	}
	if len(sent) != before+1 || !strings.Contains(sent[before], "Auto-Submitted: auto-replied") || !strings.HasPrefix(sent[before], "novice@company.ru") {
		t.Fatalf("ML-21: %d", len(sent)-before)
	}
	// R10: not the only recipient — the log only
	cc := mailLetter(ok+"From: novice2@company.ru\nTo: petr@company.ru\nCc: nabu@company.ru\nMessage-ID: <n3@company.ru>\n", "fyi")
	if m.Process(ctx, c, cc, "kn3") != email.Unavailable || len(sent) != before+1 {
		t.Fatal("R10: a letter to a thread with other recipients")
	}

	// CH-07, R11: a task whose channel is not deliverable goes to the web with a mark
	eng := &engine.Engine{Pool: pool, Users: repo(), Registry: r, Keys: &channels.Keys{Pool: pool, Pepper: key}}
	if okd, _ := eng.Deliverable(ctx, uid, domain.ChannelEmail); !okd {
		t.Fatal("mail is deliverable")
	}
	if okd, why := eng.Deliverable(ctx, uid, domain.ChannelVKTeams); okd || why == "" {
		t.Fatal("VK-04: the user has not written to the bot")
	}
	ts := &tasks.Service{Pool: pool, Bus: &bus{}, Rules: tasks.Rules{MinInterval: time.Minute, MaxActive: 5}, Catchup: time.Hour, Tick: time.Second,
		MaxFailures: 3, DefaultTimezone: "Europe/Moscow", Deliverable: eng.Deliverable}
	ti := tasks.CreateInput{Title: "Сводка", Instruction: "Сводка"}
	ti.Schedule.Cron = "0 10 * * 5"
	if tk, err := ts.Create(ctx, uid, ti, domain.ChannelEmail, nil); err != nil || tk.Channel != domain.ChannelWeb {
		t.Fatalf("R11: a task from a letter delivers to the web by default: %+v %v", tk, err)
	}
	ti.Channel = domain.ChannelEmail
	if tk, err := ts.Create(ctx, uid, ti, domain.ChannelEmail, nil); err != nil || tk.Channel != domain.ChannelEmail {
		t.Fatalf("R11: mail on request: %+v %v", tk, err)
	}
	ti.Channel = domain.ChannelTelegram
	if _, err := ts.Create(ctx, uid, ti, "web", nil); err == nil {
		t.Fatal("telegram without a binding")
	}
	_ = box
}

type spaceFiles struct{ list []email.SpaceFile }

func (s *spaceFiles) AgentFiles(context.Context, uuid.UUID, time.Time) ([]email.SpaceFile, error) {
	return s.list, nil
}

func (s *spaceFiles) Open(context.Context, uuid.UUID, string) (io.ReadCloser, int64, error) {
	return io.NopCloser(strings.NewReader("1,2\n")), 4, nil
}

// ML-17…ML-20: the confirmation of tools that change data in mail topics.
func TestConfirmations(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	uid := newUser(t, "cf1@x.org")
	var conv uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO conversations (user_id, kind, title, source, writes_require_confirmation) VALUES ($1,'topic','Письмо','email',true) RETURNING id`,
		uid).Scan(&conv); err != nil {
		t.Fatal(err)
	}
	store := &chat.Store{Pool: pool}
	main, _ := store.Main(ctx, uid)
	signer := jwt.NewSigner("https://nabu-api.x", key)
	executed := []string{}
	var follow []string
	builtin := mcp.NewServer(signer)
	builtin.Register(
		mcp.Tool{Name: "task_create", Handler: func(_ context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
			executed = append(executed, "task_create "+string(args))
			return "created", nil
		}},
		mcp.Tool{Name: "task_list", ReadOnly: true, Handler: func(context.Context, mcp.Grant, json.RawMessage) (string, error) {
			executed = append(executed, "task_list")
			return "[]", nil
		}})
	cs := &confirm.Service{Pool: pool, Box: box, TTL: 24 * time.Hour,
		Execute: func(ctx context.Context, p *confirm.Pending, args json.RawMessage) (string, bool, error) {
			return builtin.Execute(ctx, mcp.Grant{UserID: p.UserID, Conversation: p.ConversationID}, p.Tool, args)
		},
		FollowUp: func(_ context.Context, _, c uuid.UUID, text string, _ json.RawMessage) error {
			follow = append(follow, c.String()+" "+text)
			return nil
		}}
	builtin.Hold = func(ctx context.Context, g mcp.Grant, tool string, args json.RawMessage) (string, bool) {
		return cs.Hold(ctx, g.Conversation, g.UserID, nil, confirm.Builtin, tool, args)
	}
	srv := httptest.NewServer(builtin)
	defer srv.Close()
	call := func(c uuid.UUID, tool, args string) string {
		tok := signer.Issue(jwt.Claims{Audience: jwt.AudMCP, Subject: "builtin", User: uid.String(), Conversation: c.String()}, time.Hour)
		text, _, err := mcp.CallTool(ctx, srv.URL, map[string]string{"Authorization": "Bearer " + tok}, tool, json.RawMessage(args))
		if err != nil {
			t.Fatal(err)
		}
		return text
	}
	// outside mail topics tools run at once
	if out := call(main.ID, "task_create", `{"title":"a"}`); out != "created" || len(executed) != 1 {
		t.Fatalf("the main conversation: %q", out)
	}
	// ML-17, ML-20: in a mail topic a call that changes data is held; a read-only tool runs
	if out := call(conv, "task_create", `{"title":"отправить всем","token":"abc"}`); !strings.Contains(out, "waiting for the user's confirmation") || len(executed) != 1 {
		t.Fatalf("ML-17: %q %v", out, executed)
	}
	if out := call(conv, "task_list", `{}`); out != "[]" {
		t.Fatalf("a read-only tool: %q", out)
	}
	list, err := cs.List(ctx, uid, &conv)
	if err != nil || len(list) != 1 || list[0].Tool != "task_create" || !strings.Contains(list[0].ArgsPreview, "отправить всем") {
		t.Fatalf("%+v %v", list, err)
	}
	var enc []byte
	_ = pool.QueryRow(ctx, `SELECT args_enc FROM pending_confirmations WHERE id = $1`, list[0].ID).Scan(&enc)
	if strings.Contains(string(enc), "отправить") || !cs.PendingSince(ctx, conv, time.Now().Add(-time.Minute)) {
		t.Fatal("the arguments are stored in the clear")
	}
	// another user cannot resolve it
	if _, err := cs.Approve(ctx, newUser(t, "cf2@x.org"), list[0].ID); code(err) != "not_found" {
		t.Fatalf("a stranger: %v", err)
	}
	// ML-18: «Execute» runs the stored call and tells the agent
	if _, err := cs.Approve(ctx, uid, list[0].ID); err != nil || len(executed) != 3 || !strings.Contains(executed[2], "отправить всем") {
		t.Fatalf("ML-18: %v %v", err, executed)
	}
	if len(follow) != 1 || !strings.Contains(follow[0], "confirmed") || !strings.Contains(follow[0], "created") {
		t.Fatalf("ML-18: %v", follow)
	}
	if _, err := cs.Approve(ctx, uid, list[0].ID); code(err) != "confirmation_resolved" {
		t.Fatalf("twice: %v", err)
	}
	// «Reject»
	call(conv, "task_create", `{"title":"b"}`)
	list, _ = cs.List(ctx, uid, &conv)
	if _, err := cs.Reject(ctx, uid, list[0].ID); err != nil || len(executed) != 3 || !strings.Contains(follow[1], "rejected") {
		t.Fatalf("reject: %v %v", err, follow)
	}
	// ML-19: older than the TTL
	call(conv, "task_create", `{"title":"c"}`)
	list, _ = cs.List(ctx, uid, &conv)
	_, _ = pool.Exec(ctx, `UPDATE pending_confirmations SET expires_at = now() - interval '1 minute' WHERE id = $1`, list[0].ID)
	if _, err := cs.Approve(ctx, uid, list[0].ID); code(err) != "confirmation_expired" || len(executed) != 3 {
		t.Fatalf("ML-19: %v", err)
	}
	if l, _ := cs.List(ctx, uid, &conv); len(l) != 0 {
		t.Fatal("expired confirmations are listed")
	}
}
