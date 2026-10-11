package collect

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// SRC-10, SRC-11, SRC-12, SRC-13: what the query of Gmail leaves out.
func TestGmailQuery(t *testing.T) {
	at := time.Unix(1790000000, 0)
	q := gmailQuery(at, Exclusions{Senders: []string{"hr@x.org", "bad value) OR in:anywhere"}, Folders: []string{"Label_12"}, BotAddresses: []string{"nabu@x.org"}})
	for _, want := range []string{"after:1790000000", "-in:draft", "-category:promotions", "-category:social", "-category:updates", "-category:forums",
		"-from:hr@x.org", "-label:Label_12", "-from:nabu@x.org", "-to:nabu@x.org"} {
		if !strings.Contains(q, want) {
			t.Errorf("the query lacks %q: %s", want, q)
		}
	}
	if strings.Contains(q, "anywhere") {
		t.Errorf("an exclusion broke out of its operator: %s", q)
	}
	for addr, want := range map[string]bool{"noreply@x.org": true, "no-reply+abc@x.org": true, "notifications@github.com": true, "mailer-daemon@x.org": true,
		"ivanov@x.org": false, "notify.me@x.org": false} {
		if automatic(addr) != want {
			t.Errorf("automatic(%s) = %v", addr, !want)
		}
	}
}

// TASK-11, SRC-04, SRC-10: pages, the overlap of the cursor, automatic senders.
func TestGmailChanges(t *testing.T) {
	g := &Gmail{Overlap: 24 * time.Hour}
	var queries []string
	pages := map[string]string{
		"": `{"threads":[
			{"id":"t1","messages":[
				{"id":"m1","sender":"Иванов <Ivanov@x.org>","toRecipients":["me@x.org"],"date":"2026-10-12","labelIds":["INBOX"]},
				{"id":"m2","sender":"me@x.org","toRecipients":["ivanov@x.org"],"ccRecipients":["Petrova <petrova@x.org>"],"date":"2026-10-12","labelIds":["SENT"]}]},
			{"id":"t2","messages":[{"id":"m3","sender":"noreply@shop.com","toRecipients":["me@x.org"],"date":"2026-10-12"}]}],
			"nextPageToken":"p2"}`,
		"p2": `{"threads":[{"id":"t3","messages":[
			{"id":"m4","sender":"kim@x.org","date":"2026-10-13"},
			{"id":"m5","sender":"notifications@jira.x.org","date":"2026-10-13"},
			{"id":"m6","sender":"kim@x.org","date":"2026-10-13","labelIds":["DRAFT"]}]}]}`,
	}
	call := func(_ context.Context, tool string, args any) (json.RawMessage, error) {
		a := args.(map[string]any)
		if tool != "search_threads" || a["view"] != "THREAD_VIEW_METADATA_ONLY" {
			t.Fatalf("unexpected call %s %v", tool, a)
		}
		queries = append(queries, a["query"].(string))
		tok, _ := a["pageToken"].(string)
		return json.RawMessage(pages[tok]), nil
	}
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	cursor := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	cs, err := g.Changes(context.Background(), call, "gmail", cursor.Format(time.RFC3339), now.AddDate(0, 0, -30), now, Exclusions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 || !strings.HasPrefix(queries[0], "after:"+itoa(cursor.Add(-24*time.Hour).Unix())+" ") {
		t.Fatalf("queries: %v", queries)
	}
	if len(cs.Threads) != 2 || cs.Threads[0].Key != "t1" || len(cs.Threads[0].Messages) != 2 || cs.Threads[1].Key != "t3" || len(cs.Threads[1].Messages) != 1 || cs.Skipped != 3 {
		t.Fatalf("%+v", cs)
	}
	if got := strings.Join(cs.Threads[0].Participants, ","); got != "ivanov@x.org,me@x.org,petrova@x.org" {
		t.Fatalf("participants: %s", got)
	}
	if m := cs.Threads[0].Messages[0]; m.Author != "ivanov@x.org" || m.SentAt == nil || m.SentAt.Format("2006-01-02") != "2026-10-12" {
		t.Fatalf("%+v", m)
	}
	if cs.Cursor != now.Format(time.RFC3339) {
		t.Fatalf("cursor %s", cs.Cursor)
	}
	// TASK-10: the first collection starts at the depth, without a cursor.
	queries = nil
	if _, err := g.Changes(context.Background(), call, "gmail", "", now.AddDate(0, 0, -30), now, Exclusions{}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(queries[0], "after:"+itoa(now.AddDate(0, 0, -30).Unix())+" ") {
		t.Fatalf("first query: %s", queries[0])
	}
	// TASK-14: no access is passed up as it is.
	denied := func(context.Context, string, any) (json.RawMessage, error) { return nil, ErrAccess }
	if _, err := g.Changes(context.Background(), denied, "gmail", "", now, now, Exclusions{}); err != ErrAccess {
		t.Fatalf("%v", err)
	}
	// the proxy guard and the link
	th := cs.Threads[0]
	rc := g.ReadCall("gmail", th)
	raw, _ := json.Marshal(rc.Args)
	if key, ok := g.ThreadOf(rc.Tool, raw); !ok || key != "t1" || g.Link("gmail", th, "m1") != "https://mail.google.com/mail/#all/t1" {
		t.Fatalf("%s %v", key, ok)
	}
	if _, ok := g.ThreadOf("create_draft", raw); ok {
		t.Fatal("a tool that is not a read tool names a thread")
	}
	// SRC-03
	if err := g.CheckTools([]string{"search_threads", "list_labels"}); err == nil {
		t.Fatal("a server without get_thread passed the check")
	}
}

func itoa(v int64) string { b, _ := json.Marshal(v); return string(b) }

// SRC-11, SRC-13: an excluded sender, an excluded label and the letters of the
// Nabu bot stay out even when the search returns their thread by another letter.
func TestGmailExclusionsByMetadata(t *testing.T) {
	g := &Gmail{Overlap: time.Hour}
	page := `{"threads":[
		{"id":"t1","messages":[
			{"id":"m1","sender":"HR <hr@x.org>","toRecipients":["me@x.org"],"date":"2026-10-12"},
			{"id":"m2","sender":"ivanov@x.org","toRecipients":["me@x.org"],"date":"2026-10-12"},
			{"id":"m3","sender":"me@x.org","toRecipients":["Nabu <nabu@x.org>"],"date":"2026-10-12"},
			{"id":"m4","sender":"nabu@x.org","toRecipients":["me@x.org"],"date":"2026-10-12"},
			{"id":"m5","sender":"kim@x.org","toRecipients":["me@x.org"],"date":"2026-10-12","labelIds":["Label_7"]}]},
		{"id":"t2","messages":[{"id":"m6","sender":"hr@x.org","toRecipients":["me@x.org"],"date":"2026-10-12"}]}]}`
	call := func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(page), nil }
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	cs, err := g.Changes(context.Background(), call, "gmail", "", now.AddDate(0, 0, -30), now,
		Exclusions{Senders: []string{"HR@x.org"}, Folders: []string{"Label_7"}, BotAddresses: []string{"nabu@x.org"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cs.Threads) != 1 || len(cs.Threads[0].Messages) != 1 || cs.Threads[0].Messages[0].ID != "m2" || cs.Skipped != 5 {
		t.Fatalf("%+v", cs)
	}
	if got := strings.Join(cs.Threads[0].Participants, ","); got != "ivanov@x.org,me@x.org" {
		t.Fatalf("participants: %s", got)
	}
}

// TASK-11: a mailbox with more threads than one tick reads is collected in
// steps — the window is halved and the cursor moves to the end of what was read.
func TestGmailLargeWindow(t *testing.T) {
	g := &Gmail{Overlap: time.Hour}
	now := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	since := now.AddDate(0, 0, -32)
	busy := 8 * 24 * time.Hour // a window longer than this never ends: every page has a next one
	var windows []time.Duration
	call := func(_ context.Context, _ string, args any) (json.RawMessage, error) {
		q := args.(map[string]any)["query"].(string)
		var after, before int64
		for _, f := range strings.Fields(q) {
			if v, ok := strings.CutPrefix(f, "after:"); ok {
				after = atoi(v)
			}
			if v, ok := strings.CutPrefix(f, "before:"); ok {
				before = atoi(v)
			}
		}
		w := time.Duration(before-after) * time.Second
		if _, paged := args.(map[string]any)["pageToken"]; !paged {
			windows = append(windows, w)
		}
		if w > busy {
			return json.RawMessage(`{"threads":[],"nextPageToken":"more"}`), nil
		}
		return json.RawMessage(`{"threads":[{"id":"t1","messages":[{"id":"m1","sender":"a@x.org","date":"2026-09-13"}]}]}`), nil
	}
	cs, err := g.Changes(context.Background(), call, "gmail", "", since, now, Exclusions{})
	if err != nil {
		t.Fatal(err)
	}
	// 32 days → 16 → 8: the third window fits
	if len(windows) != 3 || len(cs.Threads) != 1 {
		t.Fatalf("windows %v, threads %d", windows, len(cs.Threads))
	}
	cur, _ := time.Parse(time.RFC3339, cs.Cursor)
	if !cur.After(since) || !cur.Before(now) || cur.Sub(since) > busy+time.Minute {
		t.Fatalf("the cursor %s is not the end of the window that was read", cs.Cursor)
	}
	// the next tick starts where this one ended (minus the overlap) and not from the beginning
	windows = nil
	if _, err := g.Changes(context.Background(), call, "gmail", cs.Cursor, since, now, Exclusions{}); err != nil {
		t.Fatal(err)
	}
	if first := now.Sub(cur) + time.Hour; windows[0] < first-time.Minute || windows[0] > first+time.Minute {
		t.Fatalf("the next tick searched %s, want about %s", windows[0], first)
	}
}

func atoi(s string) int64 {
	var v int64
	_ = json.Unmarshal([]byte(s), &v)
	return v
}
