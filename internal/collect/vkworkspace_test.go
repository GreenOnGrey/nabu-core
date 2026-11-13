package collect

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// FTR.NAB.ADM-0001 qa: the answers are shaped like those of the VK WorkSpace
// server recorded on 13.11.2026, with the addresses, names and texts replaced.

const vkSelf = "me@corp.example"

type vkFake struct {
	t        *testing.T
	chats    []map[string]any
	messages map[string][]map[string]any // chat_sn → messages, oldest first
	folders  string
	letters  map[string][]map[string]any // folder → letters, newest first
	calls    []string
}

func (f *vkFake) call(_ context.Context, tool string, args any) (json.RawMessage, error) {
	a := args.(map[string]any)
	switch tool {
	case vkCurrentUser:
		f.calls = append(f.calls, tool)
		return json.RawMessage(`{"email":"` + vkSelf + `","name":"Me"}`), nil
	case vkListChats:
		f.calls = append(f.calls, tool)
		off := 0
		if c, ok := a["cursor"].(string); ok {
			_, _ = fmt.Sscanf(c, "off:%d", &off)
		}
		lim := a["limit"].(int)
		end := min(off+lim, len(f.chats))
		var next any
		if end < len(f.chats) {
			next = fmt.Sprintf("off:%d", end)
		}
		b, _ := json.Marshal(map[string]any{"chats": f.chats[off:end], "next_cursor": next})
		return b, nil
	case vkListMsgs:
		sn := a["chat_sn"].(string)
		f.calls = append(f.calls, tool+" "+sn)
		all := f.messages[sn]
		end := len(all)
		if c, ok := a["cursor"].(string); ok {
			_, _ = fmt.Sscanf(c, "end:%d", &end)
		}
		start := max(0, end-a["limit"].(int))
		var next any
		if start > 0 {
			next = fmt.Sprintf("end:%d", start)
		}
		b, _ := json.Marshal(map[string]any{"is_thread": false, "messages": all[start:end], "next_cursor": next})
		return b, nil
	case vkMailFolders:
		f.calls = append(f.calls, tool)
		return json.RawMessage(f.folders), nil
	case vkMailSearch:
		folder := a["folder"].(string)
		f.calls = append(f.calls, tool+" "+folder)
		all := f.letters[folder]
		off := a["offset"].(int)
		end := min(off+a["limit"].(int), len(all))
		if off > len(all) {
			off = len(all)
		}
		b, _ := json.Marshal(map[string]any{"count": len(all), "messages": all[off:end]})
		return b, nil
	}
	f.t.Fatalf("unexpected tool %s", tool)
	return nil, nil
}

func (f *vkFake) called(prefix string) bool {
	for _, c := range f.calls {
		if c == prefix {
			return true
		}
	}
	return false
}

var vkNow = time.Date(2026, 11, 13, 20, 0, 0, 0, time.UTC)

func chat(sn, name, typ string, last time.Time, notify bool) map[string]any {
	return map[string]any{"chat_sn": sn, "name": name, "type": typ, "is_member": true, "last_message_at": last.Format(time.RFC3339),
		"notifications_enabled": notify, "unread_count": 0, "unread_mentions_count": 0}
}

var vkSeq = 7707700000000000000

func msg(chatSN, sender string, at time.Time, mentions ...string) map[string]any {
	vkSeq++
	id := fmt.Sprint(vkSeq)
	if mentions == nil {
		mentions = []string{}
	}
	return map[string]any{"chat_sn": chatSN, "sender_id": sender, "teams_id": id, "mentions": mentions, "text": "secret text",
		"time_sent": at.UTC().Format(time.RFC3339), "url": "https://teams.example/message/v1/" + id}
}

func vk() *VKWorkSpace {
	return &VKWorkSpace{Overlap: time.Hour, MailOverlap: 24 * time.Hour, ChatLimit: 1000}
}

func teams(t *testing.T, f *vkFake, ex Exclusions) ChangeSet {
	t.Helper()
	cursor := vkNow.Add(-6 * time.Hour).Format(time.RFC3339)
	cs, err := vk().Changes(context.Background(), f.call, vkTeamsSource, cursor, vkNow.AddDate(0, 0, -30), vkNow, ex)
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

func threadOf(cs ChangeSet, key string) *Thread {
	for i := range cs.Threads {
		if cs.Threads[i].Key == key {
			return &cs.Threads[i]
		}
	}
	return nil
}

func TestTEAMS01ChannelsAreNotRead(t *testing.T) {
	f := &vkFake{t: t, chats: []map[string]any{chat("100@chat.agent", "news", "channel", vkNow.Add(-time.Hour), true)},
		messages: map[string][]map[string]any{"100@chat.agent": {msg("100@chat.agent", "a@corp.example", vkNow.Add(-time.Hour))}}}
	cs := teams(t, f, Exclusions{})
	if len(cs.Threads) != 0 || f.called(vkListMsgs+" 100@chat.agent") {
		t.Fatalf("channel read: %v", f.calls)
	}
}

func TestTEAMS02And03GroupDayOnlyWithTheUser(t *testing.T) {
	g := "200@chat.agent"
	at := vkNow.Add(-2 * time.Hour)
	var ms []map[string]any
	for i := 0; i < 300; i++ {
		ms = append(ms, msg(g, "a@corp.example", at.Add(time.Duration(i)*time.Second)))
	}
	f := &vkFake{t: t, chats: []map[string]any{chat(g, "tech-leads", "group", vkNow.Add(-time.Hour), true)}, messages: map[string][]map[string]any{g: ms}}
	if cs := teams(t, f, Exclusions{}); len(cs.Threads) != 0 {
		t.Fatalf("TEAMS-03: a day without the user: %+v", cs.Threads)
	}
	ms[150]["mentions"] = []string{vkSelf}
	cs := teams(t, f, Exclusions{})
	if len(cs.Threads) != 1 || cs.Threads[0].DayKey != "2026-11-13" || len(cs.Threads[0].Messages) != 300 {
		t.Fatalf("TEAMS-02: %+v", cs.Threads)
	}
	for _, m := range cs.Threads[0].Messages {
		if m.URL == "" || m.SentAt == nil {
			t.Fatalf("LINK-01: a message without the link of the server: %+v", m)
		}
	}
}

func TestTEAMS04StopsAtTheFirstOldChat(t *testing.T) {
	var chats []map[string]any
	for i := 0; i < 250; i++ {
		chats = append(chats, chat(fmt.Sprintf("p%d@corp.example", i), "P", "private", vkNow.Add(-time.Duration(i)*time.Hour), true))
	}
	f := &vkFake{t: t, chats: chats, messages: map[string][]map[string]any{}}
	teams(t, f, Exclusions{})
	pages := 0
	for _, c := range f.calls {
		if c == vkListChats {
			pages++
		}
	}
	if pages != 1 {
		t.Fatalf("pages of chats = %d, want 1 (chats older than the cursor end the list)", pages)
	}
	if f.called(vkListMsgs + " p8@corp.example") {
		t.Fatal("a chat older than the cursor minus the overlap was read")
	}
}

func TestTEAMS05To07Bots(t *testing.T) {
	g := "300@chat.agent"
	at := vkNow.Add(-time.Hour)
	f := &vkFake{t: t, chats: []map[string]any{
		chat("1011953275", "alerts bot", "private", at, true),
		chat(g, "alerts", "group", at, true),
		chat("400@chat.agent", "team", "group", at, true),
	}, messages: map[string][]map[string]any{
		"1011953275":     {msg("1011953275", "1011953275", at)},
		g:                {msg(g, "1011953275", at), msg(g, "1011953275", at, vkSelf)},
		"400@chat.agent": {msg("400@chat.agent", "1011953275", at), msg("400@chat.agent", vkSelf, at)},
	}}
	cs := teams(t, f, Exclusions{})
	if f.called(vkListMsgs + " 1011953275") {
		t.Fatal("TEAMS-05: a private chat with a bot was read")
	}
	if threadOf(cs, g) != nil {
		t.Fatal("TEAMS-06: a day of bots only made a note")
	}
	th := threadOf(cs, "400@chat.agent")
	if th == nil || len(th.Messages) != 1 || th.Messages[0].Author != vkSelf || strings.Join(th.Participants, ",") != vkSelf {
		t.Fatalf("TEAMS-07: %+v", th)
	}
}

func TestTEAMS08To10MutedChats(t *testing.T) {
	at := vkNow.Add(-time.Hour)
	f := &vkFake{t: t, chats: []map[string]any{
		chat("500@chat.agent", "muted", "group", at, false),
		chat("friend@corp.example", "Friend", "private", at, false),
	}, messages: map[string][]map[string]any{
		"500@chat.agent":      {msg("500@chat.agent", vkSelf, at)},
		"friend@corp.example": {msg("friend@corp.example", "friend@corp.example", at)},
	}}
	cs := teams(t, f, Exclusions{})
	if f.called(vkListMsgs + " 500@chat.agent") {
		t.Fatal("TEAMS-08: a muted group was read")
	}
	if th := threadOf(cs, "friend@corp.example"); th == nil || strings.Join(th.Participants, ",") != "friend@corp.example" {
		t.Fatalf("TEAMS-10: a muted private chat: %+v", th)
	}
	cs = teams(t, f, Exclusions{IncludeMuted: []string{"500@chat.agent"}})
	if threadOf(cs, "500@chat.agent") == nil {
		t.Fatal("TEAMS-09: a returned muted group is not collected")
	}
}

func TestTEAMS13Discussions(t *testing.T) {
	g, th := "600@chat.agent", "601@chat.agent"
	at := vkNow.Add(-3 * time.Hour)
	parent := msg(g, "a@corp.example", at)
	parent["thread_id"] = th
	f := &vkFake{t: t, chats: []map[string]any{chat(g, "team", "group", vkNow.Add(-time.Hour), true)}, messages: map[string][]map[string]any{
		g:  {parent},
		th: {msg(th, vkSelf, at.Add(time.Hour))},
	}}
	cs := teams(t, f, Exclusions{})
	n := threadOf(cs, g)
	if n == nil || len(n.Parts) != 1 || n.Parts[0] != th || len(n.Messages) != 2 {
		t.Fatalf("a day with an answer of the user in a discussion: %+v", n)
	}
	calls := vk().ReadPartCalls(vkTeamsSource, *n)
	if len(calls) != 1 || calls[0].Args["chat_sn"] != th {
		t.Fatalf("part calls = %+v", calls)
	}
	if k, ok := vk().ThreadOf(vkListMsgs, json.RawMessage(`{"chat_sn":"`+th+`","limit":200}`)); !ok || k != th {
		t.Fatalf("ThreadOf = %q %v", k, ok)
	}
}

func TestTEAMS15Limit(t *testing.T) {
	g := "700@chat.agent"
	var ms []map[string]any
	for i := 0; i < 1500; i++ {
		ms = append(ms, msg(g, vkSelf, vkNow.Add(-5*time.Hour).Add(time.Duration(i)*time.Second)))
	}
	f := &vkFake{t: t, chats: []map[string]any{chat(g, "tech-devs", "group", vkNow.Add(-time.Hour), true)}, messages: map[string][]map[string]any{g: ms}}
	cs := teams(t, f, Exclusions{})
	if cs.Truncated["tech-devs"] != 1000 {
		t.Fatalf("truncated = %v", cs.Truncated)
	}
	if n := threadOf(cs, g); n == nil || len(n.Messages) != 1000 || n.Messages[999].ID != ms[1499]["teams_id"] {
		t.Fatal("the latest 1000 messages are not the ones collected")
	}
	if cs.Cursor != vkNow.Format(time.RFC3339) {
		t.Fatalf("cursor = %s", cs.Cursor)
	}
}

func TestTEAMS17ReadCursor(t *testing.T) {
	g := "800@chat.agent"
	loc := time.FixedZone("UTC+3", 3*3600)
	day1 := msg(g, vkSelf, time.Date(2026, 11, 12, 12, 0, 0, 0, loc))
	day2 := msg(g, "friend@corp.example", time.Date(2026, 11, 13, 9, 0, 0, 0, loc))
	f := &vkFake{t: t, chats: []map[string]any{chat(g, "team", "group", vkNow.Add(-time.Hour), true)},
		messages: map[string][]map[string]any{g: {day1, day2}}}
	cs, err := vk().Changes(context.Background(), f.call, vkTeamsSource, "", vkNow.AddDate(0, 0, -30), vkNow, Exclusions{Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	var past *Thread
	for i := range cs.Threads {
		if cs.Threads[i].DayKey == "2026-11-12" {
			past = &cs.Threads[i]
		}
	}
	if past == nil || past.ReadFrom != day2["teams_id"] {
		t.Fatalf("the day before: %+v", past)
	}
	call := vk().ReadCall(vkTeamsSource, *past)
	raw, _ := base64.RawURLEncoding.DecodeString(call.Args["cursor"].(string))
	if call.Tool != vkListMsgs || string(raw) != `{"from_msg_id":`+day2["teams_id"].(string)+`}` {
		t.Fatalf("read call = %+v (%s)", call, raw)
	}
	if c := vk().ReadCall(vkTeamsSource, Thread{Key: g, DayKey: "2026-11-13"}); c.Args["cursor"] != nil {
		t.Fatalf("the current day is read without a cursor: %+v", c)
	}
}

func TestTEAMS19NoText(t *testing.T) {
	g := "900@chat.agent"
	f := &vkFake{t: t, chats: []map[string]any{chat(g, "team", "group", vkNow.Add(-time.Hour), true)},
		messages: map[string][]map[string]any{g: {msg(g, vkSelf, vkNow.Add(-time.Hour))}}}
	b, _ := json.Marshal(teams(t, f, Exclusions{}))
	if strings.Contains(string(b), "secret text") {
		t.Fatalf("the text of a message is in the change set: %s", b)
	}
}

const vkFolders = `{"folders":[
	{"id":"0","name":"Входящие","parent":"-1","type":"inbox"},
	{"id":"13","name":"Найм","parent":"-1","type":"user"},
	{"id":"20","name":"Проекты","parent":"-1","type":"user"},
	{"id":"21","name":"Альфа","parent":"20","type":"user"},
	{"id":"7","name":"Jira","parent":"-1","type":"user"},
	{"id":"500000","name":"Отправленные","parent":"-1","type":"sent"},
	{"id":"500001","name":"Черновики","parent":"-1","type":"drafts"},
	{"id":"950","name":"Спам","parent":"-1","type":"spam"},
	{"id":"500002","name":"Корзина","parent":"-1","type":"trash"},
	{"children":true,"id":"5194304","name":"duty@corp.example","parent":"-1","type":"user"},
	{"id":"4194304","name":"Входящие","parent":"5194304","type":"inbox"},
	{"id":"4694304","name":"Отправленные","parent":"5194304","type":"sent"}]}`

func letter(id, thread, from string, at time.Time, newsletter bool) map[string]any {
	return map[string]any{"id": id, "thread_id": thread, "subject": "s", "search_snippet": "secret text", "date": at.Unix(),
		"correspondents": map[string]any{"from": []map[string]string{{"email": from, "name": "N"}}, "to": []map[string]string{{"email": vkSelf}}},
		"flags":          map[string]any{"newsletter": newsletter, "unread": true}}
}

func TestMAIL01To05Folders(t *testing.T) {
	at := vkNow.Add(-2 * time.Hour)
	f := &vkFake{t: t, folders: vkFolders, letters: map[string][]map[string]any{
		"0":  {letter("17900000000000000001", "1:0123456789abcdef:0", "colleague@corp.example", at, false)},
		"13": {letter("17900000000000000002", "1:0123456789abcde0:13", "candidate@mail.example", at, false)},
		"21": {letter("17900000000000000003", "1:0123456789abcde1:21", "partner@mail.example", at, false)},
		"7":  {letter("17900000000000000004", "1:0123456789abcde2:7", "jira@corp.example", at, true)},
	}}
	cs, err := vk().Changes(context.Background(), f.call, vkMailSource, vkNow.Add(-time.Hour).Format(time.RFC3339), vkNow.AddDate(0, 0, -30), vkNow,
		Exclusions{Folders: []string{"20"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, folder := range []string{"500001", "950", "500002", "5194304", "4194304", "4694304"} {
		if f.called(vkMailSearch + " " + folder) {
			t.Errorf("MAIL-01/02: the folder %s was searched", folder)
		}
	}
	if !f.called(vkMailSearch+" 0") || !f.called(vkMailSearch+" 13") || !f.called(vkMailSearch+" 500000") {
		t.Errorf("MAIL-01: own folders not searched: %v", f.calls)
	}
	if f.called(vkMailSearch + " 20") {
		t.Error("MAIL-05: an excluded folder was searched")
	}
	if threadOf(cs, "1:0123456789abcde2:7") != nil || cs.Skipped != 1 {
		t.Errorf("MAIL-04: a newsletter was collected: skipped = %d", cs.Skipped)
	}
	if threadOf(cs, "1:0123456789abcdef:0") == nil || threadOf(cs, "1:0123456789abcde0:13") == nil {
		t.Errorf("letters of own folders not collected: %+v", cs.Threads)
	}
}

func TestMAIL03SubfoldersWithoutInbox(t *testing.T) {
	f := &vkFake{t: t, folders: vkFolders, letters: map[string][]map[string]any{}}
	if _, err := vk().Changes(context.Background(), f.call, vkMailSource, "", vkNow.AddDate(0, 0, -30), vkNow, Exclusions{}); err != nil {
		t.Fatal(err)
	}
	if !f.called(vkMailSearch+" 20") || !f.called(vkMailSearch+" 21") {
		t.Fatalf("a folder with subfolders and no inbox of its own was not searched: %v", f.calls)
	}
}

func TestLINK02And03(t *testing.T) {
	l := vk().Link(vkMailSource, Thread{Key: "1:0123456789abcdef:7"}, "17900000000000000001")
	if l != "https://e.mail.ru/7/0:17900000000000000001:7/" {
		t.Fatalf("link = %s", l)
	}
	if vk().TimePrecision(vkTeamsSource) != "minute" || vk().TimePrecision(vkMailSource) != "minute" {
		t.Fatal("LINK-03: time precision")
	}
	c := vk().ReadCall(vkMailSource, Thread{Key: "1:0123456789abcdef:7"})
	if c.Tool != vkMailThread || c.Args["id"] != "1:0123456789abcdef:7" {
		t.Fatalf("mail read call = %+v", c)
	}
}

func TestVK02ToolsPerSource(t *testing.T) {
	tools := []string{vkListChats, vkCurrentUser, vkMailFolders, vkMailSearch, vkMailThread}
	a := vk()
	if err := a.CheckSourceTools(vkTeamsSource, tools); err == nil || !strings.Contains(err.Error(), vkListMsgs) {
		t.Fatalf("teams: %v", err)
	}
	if err := a.CheckSourceTools(vkMailSource, tools); err != nil {
		t.Fatalf("mail: %v", err)
	}
	if err := a.CheckTools(tools); err != nil {
		t.Fatalf("the adapter with one source: %v", err)
	}
	if err := a.CheckTools([]string{vkListChats}); err == nil {
		t.Fatal("the adapter without any source passed")
	}
}

// A group without the field of notifications is not muted; two chats of one name stay two in the truncated.
func TestTEAMS08And15Edges(t *testing.T) {
	at := vkNow.Add(-time.Hour)
	g := chat("1100@chat.agent", "Standup", "group", at, true)
	delete(g, "notifications_enabled")
	h := chat("1101@chat.agent", "Standup", "group", at, true)
	var ms, ns []map[string]any
	for i := 0; i < 3; i++ {
		ms = append(ms, msg("1100@chat.agent", vkSelf, at.Add(time.Duration(i)*time.Second)))
		ns = append(ns, msg("1101@chat.agent", vkSelf, at.Add(time.Duration(i)*time.Second)))
	}
	f := &vkFake{t: t, chats: []map[string]any{g, h}, messages: map[string][]map[string]any{"1100@chat.agent": ms, "1101@chat.agent": ns}}
	a := vk()
	a.ChatLimit = 2
	cs, err := a.Changes(context.Background(), f.call, vkTeamsSource, vkNow.Add(-6*time.Hour).Format(time.RFC3339), vkNow.AddDate(0, 0, -30), vkNow, Exclusions{})
	if err != nil {
		t.Fatal(err)
	}
	if threadOf(cs, "1100@chat.agent") == nil {
		t.Fatal("a group without the field of notifications was skipped as muted")
	}
	if len(cs.Truncated) != 2 {
		t.Fatalf("truncated = %v", cs.Truncated)
	}
}

// LINK-02: a letter of the thread found in Sent opens in Sent.
func TestLINK02SentLetter(t *testing.T) {
	at := vkNow.Add(-2 * time.Hour)
	f := &vkFake{t: t, folders: vkFolders, letters: map[string][]map[string]any{
		"0":      {letter("17900000000000000011", "1:0123456789abcdef:0", "colleague@corp.example", at, false)},
		"500000": {letter("17900000000000000012", "1:0123456789abcdef:0", vkSelf, at.Add(time.Minute), false)},
	}}
	cs, err := vk().Changes(context.Background(), f.call, vkMailSource, "", vkNow.AddDate(0, 0, -30), vkNow, Exclusions{})
	if err != nil {
		t.Fatal(err)
	}
	th := threadOf(cs, "1:0123456789abcdef:0")
	if th == nil || len(th.Messages) != 2 || th.Messages[1].URL != "https://e.mail.ru/500000/0:17900000000000000012:500000/" {
		t.Fatalf("thread = %+v", th)
	}
}
