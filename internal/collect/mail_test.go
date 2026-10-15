package collect

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// fakeMail answers like imap-mcp: folders and pages of search_messages by folder.
func fakeMail(t *testing.T, folders string, pages map[string][]string) (Caller, *[]map[string]any) {
	var calls []map[string]any
	return func(_ context.Context, tool string, args any) (json.RawMessage, error) {
		a := args.(map[string]any)
		a["tool"] = tool
		calls = append(calls, a)
		switch tool {
		case mailFolders:
			return json.RawMessage(folders), nil
		case mailSearch:
			p := pages[a["folder"].(string)]
			i := a["offset"].(int) / mailPageSize
			if i >= len(p) {
				return json.RawMessage("[]"), nil
			}
			return json.RawMessage(p[i]), nil
		}
		t.Fatalf("unexpected tool %s", tool)
		return nil, nil
	}, &calls
}

// FTR.NAB.CMN-0006 YML-14 (FTR.NAB.CMN-0003 SRC-04, SRC-10, SRC-13 for mail): folders
// without trash, spam and drafts; automatic letters and mailings left out;
// messages grouped by thread, one message of two folders counted once, the
// link to a message from the server.
func TestMailChanges(t *testing.T) {
	m := &Mail{Overlap: time.Hour}
	folders := `[{"name":"INBOX","special":"Inbox"},{"name":"Отправленные","special":"Sent"},{"name":"Спам","special":"Spam"},
		{"name":"Удалённые","special":"Trash"},{"name":"Проект","special":null},{"name":"Личное","special":null}]`
	inbox := `[
		{"from":"Ivanov@x.org","to":["me@x.org"],"date":"2026-10-12T10:00:00+03:00","messageId":"<m1@x>","threadId":"<m1@x>","webLink":"https://mail.example/m1"},
		{"from":"news@shop.com","to":["me@x.org"],"date":"2026-10-12T11:00:00+00:00","messageId":"<n1@shop>","threadId":"<n1@shop>","bulk":true},
		{"from":"noreply@x.org","to":["me@x.org"],"date":"2026-10-12T11:00:00+00:00","messageId":"<r1@x>","threadId":"<r1@x>"},
		{"from":"kim@x.org","to":["nabu@x.org"],"date":"2026-10-12T12:00:00+00:00","messageId":"<b1@x>","threadId":"<b1@x>"}]`
	sent := `[
		{"from":"me@x.org","to":["ivanov@x.org"],"cc":["Petrova <petrova@x.org>"],"date":"2026-10-12T12:30:00+03:00","messageId":"<m2@x>","threadId":"<m1@x>"},
		{"from":"Ivanov@x.org","to":["me@x.org"],"date":"2026-10-12T10:00:00+03:00","messageId":"<m1@x>","threadId":"<m1@x>"}]`
	call, calls := fakeMail(t, folders, map[string][]string{"INBOX": {inbox}, "Отправленные": {sent}})
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	cursor := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	cs, err := m.Changes(context.Background(), call, mailSource, cursor.Format(time.RFC3339), now.AddDate(0, 0, -30), now,
		Exclusions{Folders: []string{"личное"}, BotAddresses: []string{"nabu@x.org"}})
	if err != nil {
		t.Fatal(err)
	}
	var searched []string
	for _, c := range *calls {
		if c["tool"] == mailSearch {
			searched = append(searched, c["folder"].(string))
			// the overlap is at least a day: IMAP searches by dates
			if c["since"] != "2026-10-12" || c["before"] != "2026-10-15" {
				t.Fatalf("window %v %v", c["since"], c["before"])
			}
		}
	}
	if strings.Join(searched, ",") != "INBOX,Отправленные,Проект" {
		t.Fatalf("folders: %v", searched)
	}
	if len(cs.Threads) != 1 || cs.Skipped != 3 || cs.Cursor != now.Format(time.RFC3339) {
		t.Fatalf("change set: %+v", cs)
	}
	th := cs.Threads[0]
	if th.Key != "<m1@x>" || len(th.Messages) != 2 || th.Messages[0].ID != "<m1@x>" || th.Messages[0].URL != "https://mail.example/m1" ||
		th.Messages[0].SentAt == nil || th.Messages[0].SentAt.Hour() != 7 {
		t.Fatalf("thread: %+v", th)
	}
	if strings.Join(th.Participants, ",") != "ivanov@x.org,me@x.org,petrova@x.org" {
		t.Fatalf("participants: %v", th.Participants)
	}
}

// YML-14: «All Mail» (\All, e.g. Gmail over IMAP) holds every message: one folder is enough.
func TestMailAllMail(t *testing.T) {
	m := &Mail{Overlap: 24 * time.Hour}
	call, calls := fakeMail(t, `[{"name":"INBOX","special":"Inbox"},{"name":"[Gmail]/All Mail","special":"All"},{"name":"[Gmail]/Trash","special":"Trash"}]`,
		map[string][]string{})
	if _, err := m.Changes(context.Background(), call, mailSource, "", time.Now().AddDate(0, 0, -1), time.Now(), Exclusions{}); err != nil {
		t.Fatal(err)
	}
	for _, c := range *calls {
		if c["tool"] == mailSearch && c["folder"] != "[Gmail]/All Mail" {
			t.Fatalf("searched %v", c["folder"])
		}
	}
}

// YML-17: a full page asks for the next one.
func TestMailPages(t *testing.T) {
	page := make([]string, 0, mailPageSize)
	for i := range mailPageSize {
		page = append(page, `{"from":"a@x.org","date":"2026-10-12T10:00:00Z","messageId":"<p`+string(rune('A'+i%26))+string(rune('a'+i/26))+`@x>","threadId":"<t@x>"}`)
	}
	call, calls := fakeMail(t, `[{"name":"INBOX","special":"Inbox"}]`, map[string][]string{"INBOX": {"[" + strings.Join(page, ",") + "]",
		`[{"from":"b@x.org","date":"2026-10-12T10:00:00Z","messageId":"<last@x>","threadId":"<t@x>"}]`}})
	cs, err := (&Mail{}).Changes(context.Background(), call, mailSource, "", time.Now().AddDate(0, 0, -2), time.Now(), Exclusions{})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range *calls {
		if c["tool"] == mailSearch {
			n++
		}
	}
	if n != 2 || len(cs.Threads) != 1 || len(cs.Threads[0].Messages) != mailPageSize+1 {
		t.Fatalf("pages %d, threads %+v", n, len(cs.Threads))
	}
}

// YML-15: the collector agent reads a thread of its batch with read_thread.
func TestMailReadCall(t *testing.T) {
	m := &Mail{}
	d1, d2 := time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC), time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	c := m.ReadCall(mailSource, Thread{Key: "<m1@x>", Messages: []Msg{{ID: "<m2@x>", SentAt: &d2}, {ID: "<m1@x>", SentAt: &d1}}})
	if c.Args["since"] != "2026-10-11" {
		t.Fatalf("since: %v", c.Args)
	}
	args, _ := json.Marshal(c.Args)
	if key, ok := m.ThreadOf(c.Tool, args); !ok || key != "<m1@x>" {
		t.Fatalf("%v %s", ok, key)
	}
	if _, ok := m.ThreadOf("search_messages", args); ok {
		t.Fatal("only read_thread reads a thread")
	}
	if m.CheckTools([]string{"list_folders", "search_messages"}) == nil {
		t.Fatal("read_thread is needed")
	}
}
