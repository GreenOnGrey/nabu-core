//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/collect"
	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
)

// CMT-09, CMT-11, CMT-12, CMT-13 (FTR.NAB.CMN-0003 R11, tech decision 57): the personal agent saves a commitment
// from the chat, and the collector links the same commitment found in mail to it instead of a second one.
func TestChatCommitments(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	a, b := newUser(t, "chat-cmt-a@x.org"), newUser(t, "chat-cmt-b@x.org")
	_ = s.Consent(ctx, a)
	saveNote(t, s, a, "known", "Знакомство", "Переписка с Ивановым.", []comm.PersonIn{{Email: "ivanov@x.org", Name: "Иванов А."}}, nil)
	var conv uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO conversations (user_id, kind) VALUES ($1, 'topic') RETURNING id`, a).Scan(&conv); err != nil {
		t.Fatal(err)
	}
	tools := map[string]mcp.Tool{}
	for _, tl := range s.Tools(nil, nil) {
		tools[tl.Name] = tl
	}
	create := tools["commitment_create"]
	ga := mcp.Grant{UserID: a, Conversation: conv}

	// CMT-13: offered only to a user who consented — a group agent works as a technical user that never does.
	if create.Handler == nil || create.Available(ctx, mcp.Grant{UserID: b}) {
		t.Fatal("commitment_create is offered without the consent")
	}
	if !create.Available(ctx, ga) {
		t.Fatal("commitment_create is not offered to the user who consented")
	}
	// CMT-09: the person is found by name; the conversation comes from the grant, not from the arguments.
	out, err := create.Handler(ctx, ga, []byte(`{"text":"Прислать смету по релизу","direction":"mine","person":"иванов","due":"2026-10-17"}`))
	if err != nil || !strings.Contains(out, "Прислать смету по релизу") || !strings.Contains(out, "Иванов") {
		t.Fatalf("%q %v", out, err)
	}
	l, _ := s.Commitments(ctx, a, comm.CommitmentFilter{}, 0)
	var chat *comm.Commitment
	for i := range l {
		if l[i].Text == "Прислать смету по релизу" {
			chat = &l[i]
		}
	}
	if chat == nil || chat.Origin != comm.OriginChat || chat.NoteID != nil || chat.ConversationID == nil || *chat.ConversationID != conv ||
		chat.Person == nil || chat.Person.Email != "ivanov@x.org" || chat.Due == nil || chat.BasisURL != "" {
		t.Fatalf("CMT-09: %+v", chat)
	}
	raw, _ := json.Marshal(chat)
	if !strings.Contains(string(raw), `"noteId":null`) || !strings.Contains(string(raw), `"origin":"chat"`) {
		t.Fatalf("CMT-09: the JSON of the API %s", raw)
	}
	// a repeated call returns the commitment already saved
	if again, err := create.Handler(ctx, ga, []byte(`{"text":"прислать смету по релизу","direction":"mine","person":"ivanov@x.org"}`)); err != nil || !strings.Contains(again, chat.ID.String()) {
		t.Fatalf("a repeated create: %q %v", again, err)
	}
	// a name that matches several people is not guessed
	saveNote(t, s, a, "known2", "Ещё", "Переписка с Ивановой.", []comm.PersonIn{{Email: "ivanova@x.org", Name: "Иванова Е."}}, nil)
	if _, err := create.Handler(ctx, ga, []byte(`{"text":"Позвонить","direction":"mine","person":"иванов"}`)); err == nil || !strings.Contains(err.Error(), "Several people") {
		t.Fatalf("an ambiguous person: %v", err)
	}
	if _, err := create.Handler(ctx, ga, []byte(`{"text":"Позвонить","direction":"mine","person":"Неизвестный"}`)); err == nil {
		t.Fatal("an unknown person without an email passed")
	}
	if _, err := create.Handler(ctx, ga, []byte(`{"text":"Позвонить","direction":"later"}`)); err == nil {
		t.Fatal("an unknown direction passed")
	}
	// CMT-13: a secret in the text is masked.
	if out, err := create.Handler(ctx, ga, []byte(`{"text":"Передать доступ: Пароль от стенда: Qwerty!2345","direction":"mine"}`)); err != nil || strings.Contains(out, "Qwerty!2345") {
		t.Fatalf("CMT-13: %q %v", out, err)
	}
	var stored int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM commitments WHERE user_id = $1 AND text LIKE '%Qwerty%'`, a).Scan(&stored)
	if stored != 0 {
		t.Fatal("CMT-13: the secret is in the store")
	}

	// CMT-11: the batch of the collector shows it, and commitment_save with sameAs links it to the thread.
	box, _ := crypto.NewBox(key)
	cat := &catalog.Service{Pool: pool, Box: box, Signer: jwt.NewSigner("https://nabu.test", key)}
	mode, yes := "personal", true
	item, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "gmail-chat-" + uuid.NewString()[:6], Source: &catalog.Source{Kind: "url", URL: "http://gmail.invalid"},
		Mode: &mode, Published: &yes, PersonalAuth: &catalog.PersonalAuth{Kind: "token"}, ReadOnly: ptr(false)})
	if err != nil {
		t.Fatal(err)
	}
	adapter := "gmail"
	if err := s.SetSource(ctx, item.ID, &adapter, &yes); err != nil {
		t.Fatal(err)
	}
	ids, err := s.CreateBatches(ctx, a, nil, item.ID, "gmail", []collect.Thread{{Key: "smeta", Participants: []string{"ivanov@x.org"},
		Messages: []collect.Msg{{ID: "sm1"}}}}, false, 20, "")
	if err != nil || len(ids) != 1 {
		t.Fatal(ids, err)
	}
	run := uuid.New()
	if ok, err := s.StartBatch(ctx, a, ids[0], run, time.Minute); !ok || err != nil {
		t.Fatal(ok, err)
	}
	ctools := map[string]mcp.Tool{}
	for _, tl := range s.CollectTools(func(context.Context, uuid.UUID) comm.UserInfo {
		return comm.UserInfo{Email: "chat-cmt-a@x.org", Timezone: "UTC"}
	}) {
		ctools[tl.Name] = tl
	}
	gr := mcp.Grant{UserID: a, Scope: mcp.ScopeMemoryRun, RunID: run}
	batch, err := ctools["comm_batch"].Handler(ctx, gr, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var bj struct {
		OpenCommitments []struct {
			ID       string `json:"id"`
			Text     string `json:"text"`
			FromChat bool   `json:"fromChat"`
		} `json:"openCommitments"`
	}
	_ = json.Unmarshal([]byte(batch), &bj)
	seen := 0
	for _, c := range bj.OpenCommitments {
		if c.ID == chat.ID.String() {
			seen++
			if !c.FromChat {
				t.Fatalf("CMT-11: without fromChat: %s", batch)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("CMT-11: the commitment from the chat is in the batch %d times: %s", seen, batch)
	}
	if _, err := ctools["comm_note_save"].Handler(ctx, gr, []byte(`{"threadKey":"smeta","title":"Смета","summary":"Пользователь обещал прислать смету.",
		"people":[{"email":"ivanov@x.org"}],"messages":[{"id":"sm1","author":"chat-cmt-a@x.org","date":"2026-10-16"}]}`)); err != nil {
		t.Fatal(err)
	}
	save := ctools["commitment_save"].Handler
	// CMT-12: sameAs of a commitment that is not an open one from the chat without a note is refused.
	collected, err := s.SaveCommitment(ctx, a, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "known", Text: "Вернуть договор", Direction: comm.Theirs, PersonEmail: "ivanov@x.org"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{collected.String(), uuid.NewString()} {
		if _, err := save(ctx, gr, []byte(fmt.Sprintf(`{"threadKey":"smeta","text":"x","direction":"mine","sameAs":%q}`, id))); err == nil {
			t.Fatalf("CMT-12: sameAs %s passed", id)
		}
	}
	if _, err := save(ctx, gr, []byte(fmt.Sprintf(`{"threadKey":"smeta","text":"x","direction":"theirs","sameAs":%q}`, chat.ID))); err == nil {
		t.Fatal("CMT-12: sameAs in the other direction passed")
	}
	out, err = save(ctx, gr, []byte(fmt.Sprintf(`{"threadKey":"smeta","text":"Прислать смету","direction":"mine","personEmail":"ivanov@x.org","basisMessageId":"sm1","sameAs":%q}`, chat.ID)))
	if err != nil || !strings.Contains(out, "Linked") {
		t.Fatalf("CMT-11: %q %v", out, err)
	}
	l, _ = s.Commitments(ctx, a, comm.CommitmentFilter{State: "all"}, 0)
	n := 0
	for i := range l {
		if strings.Contains(l[i].Text, "смет") {
			n++
			if l[i].ID != chat.ID || l[i].NoteID == nil || l[i].Origin != comm.OriginChat || l[i].Text != "Прислать смету по релизу" {
				t.Fatalf("CMT-11: %+v", l[i])
			}
		}
	}
	if n != 1 {
		t.Fatalf("CMT-11: %d commitments about the estimate", n)
	}
	// the same thread read again: sameAs again and a save of the collector's own wording add nothing
	if _, err := save(ctx, gr, []byte(fmt.Sprintf(`{"threadKey":"smeta","text":"Прислать смету","direction":"mine","basisMessageId":"sm1","sameAs":%q}`, chat.ID))); err != nil {
		t.Fatalf("a repeated sameAs: %v", err)
	}
	if _, err := save(ctx, gr, []byte(`{"threadKey":"smeta","text":"Прислать смету Иванову","direction":"mine","personEmail":"ivanov@x.org","basisMessageId":"sm1"}`)); err != nil {
		t.Fatal(err)
	}
	if l, _ = s.Commitments(ctx, a, comm.CommitmentFilter{State: "all"}, 0); countText(l, "смет") != 1 {
		t.Fatalf("the thread read again added a commitment: %+v", l)
	}
	// CMT-12: another user cannot link it.
	if _, err := s.SaveCommitment(ctx, b, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "smeta", Text: "x", Direction: comm.Mine, SameAs: &chat.ID}); err == nil {
		t.Fatal("CMT-12: another user linked the commitment")
	}
	// the user deletes the note: the commitment from the chat stays, without the note
	var note uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT note_id FROM commitments WHERE id = $1`, chat.ID).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, a, note); err != nil {
		t.Fatal(err)
	}
	l, _ = s.Commitments(ctx, a, comm.CommitmentFilter{}, 0)
	if countText(l, "смет") != 1 {
		t.Fatalf("the commitment from the chat went with the note: %+v", l)
	}
	for _, c := range l {
		if c.ID == chat.ID && (c.NoteID != nil || c.BasisURL != "") {
			t.Fatalf("still linked to the deleted note: %+v", c)
		}
	}
}

func countText(l []comm.Commitment, part string) int {
	n := 0
	for _, c := range l {
		if strings.Contains(c.Text, part) {
			n++
		}
	}
	return n
}

// LIFE-11 (R23, R24, tech decision 57): the retention removes a commitment from the chat without a note only once
// it has been closed longer than the retention; «Удалить собранное» removes the rest.
func TestChatCommitmentsRetention(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "chat-ret@x.org")
	mk := func(text string) uuid.UUID {
		c, err := s.CreateCommitment(ctx, u, comm.ChatCommitment{Text: text, Direction: comm.Mine})
		if err != nil {
			t.Fatal(err)
		}
		return c.ID
	}
	open, oldClosed, newClosed := mk("Открытая"), mk("Закрыта давно"), mk("Закрыта недавно")
	for _, id := range []uuid.UUID{oldClosed, newClosed} {
		done := comm.Done
		if _, err := s.UpdateCommitment(ctx, u, id, comm.CommitmentPatch{State: &done}, comm.ByUser); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE commitments SET closed_at = now() - interval '400 days' WHERE id = $1`, oldClosed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	left := map[uuid.UUID]bool{}
	l, _ := s.Commitments(ctx, u, comm.CommitmentFilter{State: "all"}, 0)
	for _, c := range l {
		left[c.ID] = true
	}
	if !left[open] || left[oldClosed] || !left[newClosed] {
		t.Fatalf("LIFE-11: after the retention %v", left)
	}
	if err := s.DeleteAll(ctx, u, "all"); err != nil {
		t.Fatal(err)
	}
	if l, _ := s.Commitments(ctx, u, comm.CommitmentFilter{State: "all"}, 0); len(l) != 0 {
		t.Fatalf("LIFE-11: «Удалить собранное» left %+v", l)
	}
}

// ADM-07 (R29, tech decision 58): thinkingOff takes a value or null.
func TestCommThinkingOffSetting(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	t.Cleanup(func() { _, _ = s.SaveSettings(ctx, comm.SettingsPatch{ThinkingOff: []byte(`null`)}) })
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{ThinkingOff: []byte(`"always"`)}); !isCode(err, "invalid_thinking_off") {
		t.Fatalf("an unknown way: %v", err)
	}
	st, err := s.SaveSettings(ctx, comm.SettingsPatch{ThinkingOff: []byte(`"chat-template"`)})
	if err != nil || st.ThinkingOff == nil || *st.ThinkingOff != "chat-template" {
		t.Fatalf("%+v %v", st.ThinkingOff, err)
	}
	if st, _ = s.LoadSettings(ctx); st.ThinkingOff == nil || *st.ThinkingOff != "chat-template" {
		t.Fatal("not stored")
	}
	for _, v := range []string{"provider", "reasoning-effort", "allow"} {
		st, err = s.SaveSettings(ctx, comm.SettingsPatch{ThinkingOff: []byte(`"` + v + `"`)})
		if err != nil || st.ThinkingOff == nil || *st.ThinkingOff != v {
			t.Fatalf("%s: %+v %v", v, st.ThinkingOff, err)
		}
	}
	if st, err = s.SaveSettings(ctx, comm.SettingsPatch{}); err != nil || st.ThinkingOff == nil {
		t.Fatal("a patch without the field changed it")
	}
	if st, err = s.SaveSettings(ctx, comm.SettingsPatch{ThinkingOff: []byte(`null`)}); err != nil || st.ThinkingOff != nil {
		t.Fatalf("null did not remove it: %+v %v", st.ThinkingOff, err)
	}
}
