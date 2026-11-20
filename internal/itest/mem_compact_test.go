//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// FTR.NAB.MEM-0001 qa §2.1: the compaction with a model that answers by a script.

// fakeModel answers the compaction: the title and the summary name the records it got, the topics are the first
// of them, the first message is a key one. fail makes it answer without JSON.
type fakeModel struct {
	calls  atomic.Int32
	fail   atomic.Bool
	inputs []string
}

func (f *fakeModel) complete(_ context.Context, _ comm.Choice, system, user string) (string, float64, error) {
	f.calls.Add(1)
	f.inputs = append(f.inputs, user)
	if f.fail.Load() {
		return "not json", 0.01, nil
	}
	var in struct {
		Level   string `json:"level"`
		Records []struct {
			Title   string   `json:"title"`
			Summary string   `json:"summary"`
			Topics  []string `json:"topics"`
		} `json:"records"`
		Messages []struct {
			N int `json:"n"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(user), &in); err != nil {
		return "", 0, err
	}
	var titles []string
	var topics []string
	for _, r := range in.Records {
		titles = append(titles, r.Title)
		topics = append(topics, r.Topics...)
	}
	key := []int{}
	if len(in.Messages) > 0 {
		key = append(key, in.Messages[0].N)
	}
	b, _ := json.Marshal(map[string]any{"title": in.Level + ": " + strings.Join(titles, ", "), "summary": "Свод: " + strings.Join(titles, "; "),
		"topics": topics, "keyMessages": key})
	return string(b), 0.01, nil
}

func saveChat(t *testing.T, s *comm.Service, uid uuid.UUID, chat, day, title string, people []comm.PersonIn, topics []string, msgs ...comm.Message) *comm.Saved {
	t.Helper()
	res, err := s.SaveNote(context.Background(), uid, comm.SaveNote{ItemID: commItem, Source: "vkteams", ThreadKey: chat, DayKey: day, Title: title,
		Summary: "Обсуждение " + title, People: people, Topics: topics, Messages: msgs, UserEmail: "me@x.org"})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func msgAt(id string, y int, m time.Month, d, h int) comm.Message {
	v := time.Date(y, m, d, h, 0, 0, 0, time.UTC)
	return comm.Message{ID: id, SentAt: &v, Author: "a@x.org"}
}

func compactNow(t *testing.T, s *comm.Service, uid uuid.UUID, f *fakeModel) comm.CompactOutcome {
	t.Helper()
	ch := &comm.Choice{ConnectionID: uuid.NewString(), Model: "m"}
	audit := func(_ context.Context, _ uuid.UUID, level string, counts map[string]int, failed error) {
		if failed != nil {
			t.Logf("the compaction of a %s failed: %v %+v", level, failed, counts)
		}
	}
	out, err := s.CompactUser(context.Background(), uid, ch, f.complete, comm.CompactConfig{Groups: 20, Timeout: time.Minute, MaxKey: 20, MaxMessages: 300}, audit)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func records(t *testing.T, s *comm.Service, uid uuid.UUID) map[string]comm.Note {
	t.Helper()
	l, err := s.List(context.Background(), uid, comm.Filter{}, httpx.Page{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]comm.Note{}
	for _, n := range l.Items {
		out[n.Level+" "+n.PeriodFrom+".."+n.PeriodTo] = n
	}
	return out
}

// MEM-CP-02, MEM-CP-04, MEM-CP-05, MEM-CP-06, MEM-CP-11, MEM-CP-16, MEM-VW-05: days → parts of weeks → a month;
// a single record is promoted without the model; the commitments, the messages and the days follow.
func TestMemCompactChat(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "mem-compact@x.org")
	ivanov := []comm.PersonIn{{Email: "ivanov@x.org", Name: "Иванов"}}
	// the week of 5–11 October: three days; the week of 26 October – 1 November crosses the month
	saveChat(t, s, u, "chat-1", "2026-10-05", "Пн", ivanov, []string{"Релиз"}, msgAt("m1", 2026, 10, 5, 9), msgAt("m2", 2026, 10, 5, 10))
	saveChat(t, s, u, "chat-1", "2026-10-06", "Вт", ivanov, []string{"Релиз"}, msgAt("m3", 2026, 10, 6, 9))
	saveChat(t, s, u, "chat-1", "2026-10-07", "Ср", ivanov, []string{"Релиз"}, msgAt("m4", 2026, 10, 7, 9))
	saveChat(t, s, u, "chat-1", "2026-10-30", "Пт", ivanov, []string{"Релиз"}, msgAt("m5", 2026, 10, 30, 9))
	saveChat(t, s, u, "chat-1", "2026-11-01", "Вс", ivanov, []string{"Релиз"}, msgAt("m6", 2026, 11, 1, 9))
	if _, err := s.SaveCommitment(ctx, u, comm.SaveCommitment{ItemID: commItem, Source: "vkteams", ThreadKey: "chat-1", DayKey: "2026-10-06",
		Text: "Прислать смету", Direction: comm.Theirs, PersonEmail: "ivanov@x.org", BasisMessageID: "m3"}); err != nil {
		t.Fatal(err)
	}
	nodes, _ := graphOf(t, s, u, comm.Filter{})
	daysBefore := nodes["Иванов"].Days

	f := &fakeModel{}
	out := compactNow(t, s, u, f)
	if out.Groups != 3 || out.Failed != 0 {
		t.Fatalf("the parts of weeks: %+v", out)
	}
	r := records(t, s, u)
	week := r["week 2026-10-05..2026-10-11"]
	if week.ID == uuid.Nil || week.Days != 3 || week.MessageCount != 4 || *week.CompactedFrom != 3 || len(r) != 3 {
		t.Fatalf("MEM-CP-02: %+v", r)
	}
	if _, ok := r["week 2026-10-26..2026-10-31"]; !ok {
		t.Fatalf("MEM-CP-04: the part of the week in October: %+v", r)
	}
	if _, ok := r["week 2026-11-01..2026-11-01"]; !ok {
		t.Fatalf("MEM-CP-04: the part of the week in November: %+v", r)
	}
	if f.calls.Load() != 1 {
		t.Fatalf("MEM-CP-05: a single record of a part of a week asks no model: %d calls", f.calls.Load())
	}
	full, err := s.Get(ctx, u, week.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys := 0
	for _, m := range full.Messages {
		if m.Key {
			keys++
		}
	}
	if len(full.Commitments) != 1 || full.Commitments[0].Text != "Прислать смету" || keys < 2 || !full.Messages[0].Key {
		t.Fatalf("MEM-CP-11, MEM-CP-16: the commitment and the key messages: %+v", full)
	}
	if nodes, _ = graphOf(t, s, u, comm.Filter{}); nodes["Иванов"].Days != daysBefore {
		t.Fatalf("MEM-VW-05: the days of communication changed: %d → %d", daysBefore, nodes["Иванов"].Days)
	}

	// MEM-CP-06: October has only weeks now — the next run makes a month of it
	out = compactNow(t, s, u, f)
	r = records(t, s, u)
	month, ok := r["month 2026-10-01..2026-10-31"]
	if !ok || out.Groups != 1 || month.Days != 4 || len(r) != 2 {
		t.Fatalf("the month: %+v %+v", out, r)
	}
	// MEM-CP-15: the month of November would see October as the previous record
	if nodes, _ = graphOf(t, s, u, comm.Filter{}); nodes["Иванов"].Days != daysBefore {
		t.Fatalf("MEM-VW-05 after the month: %d", nodes["Иванов"].Days)
	}
}

// MEM-CP-21, MEM-CP-19, MEM-CP-20: a late message goes into the compacted week; the compaction leaves no tombstones;
// a deleted week keeps its days from being collected again.
func TestMemCompactLateAndDeleted(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "mem-late@x.org")
	saveChat(t, s, u, "chat-2", "2026-10-12", "Пн", nil, nil, msgAt("a1", 2026, 10, 12, 9))
	saveChat(t, s, u, "chat-2", "2026-10-13", "Вт", nil, nil, msgAt("a2", 2026, 10, 13, 9))
	f := &fakeModel{}
	compactNow(t, s, u, f)
	week := records(t, s, u)["week 2026-10-12..2026-10-18"]
	if week.ID == uuid.Nil {
		t.Fatalf("%+v", records(t, s, u))
	}
	res := saveChat(t, s, u, "chat-2", "2026-10-14", "Ср", nil, nil, msgAt("a3", 2026, 10, 14, 9))
	if res.ID != week.ID || res.Created {
		t.Fatalf("MEM-CP-21: a late message made %+v", res)
	}
	full, _ := s.Get(ctx, u, week.ID)
	if len(full.Appended) != 1 || full.MessageCount != 3 {
		t.Fatalf("MEM-CP-21: the block of the late message: %+v", full)
	}
	var tombs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM comm_tombstones WHERE user_id = $1`, u).Scan(&tombs); err != nil || tombs != 0 {
		t.Fatalf("MEM-CP-20: tombstones after the compaction: %d %v", tombs, err)
	}
	if err := s.Delete(ctx, u, week.ID); err != nil {
		t.Fatal(err)
	}
	if res = saveChat(t, s, u, "chat-2", "2026-10-15", "Чт", nil, nil, msgAt("a4", 2026, 10, 15, 9)); !res.Skipped {
		t.Fatalf("MEM-CP-19: a day of a deleted week is collected again: %+v", res)
	}
}

// MEM-CP-07, MEM-CP-08, MEM-CP-18: a mail thread with added blocks is rewritten in place; without them it is not;
// a topic the user took off stays off.
func TestMemCompactThread(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "mem-thread@x.org")
	n := saveNote(t, s, u, "mail-1", "Договор", "Обсуждали договор.", nil, []string{"Договор", "Юристы"}, comm.Message{ID: "x1", SentAt: at(5, 9)})
	edit := "Исправлено пользователем."
	if _, err := s.Update(ctx, u, n.ID, comm.NotePatch{Summary: &edit}); err != nil {
		t.Fatal(err)
	}
	saveNote(t, s, u, "mail-1", "Договор", "Пришла правка юристов.", nil, []string{"Договор", "Юристы"}, comm.Message{ID: "x2", SentAt: at(6, 9)})
	saveNote(t, s, u, "mail-2", "Счёт", "Пришёл счёт.", nil, nil, comm.Message{ID: "y1", SentAt: at(6, 9)})
	full, _ := s.Get(ctx, u, n.ID)
	var lawyers uuid.UUID
	for _, tp := range full.Topics {
		if tp.Label == "Юристы" {
			lawyers = tp.ID
		}
	}
	if _, err := s.Update(ctx, u, n.ID, comm.NotePatch{RemoveTags: []comm.TagRef{{Kind: comm.KindTopic, ID: &lawyers}}}); err != nil {
		t.Fatal(err)
	}
	// the blocks were added «today»: the thread waits for the week to close
	if _, err := pool.Exec(ctx, `UPDATE comm_notes SET updated_at = now() - interval '10 days' WHERE user_id = $1`, u); err != nil {
		t.Fatal(err)
	}
	f := &fakeModel{}
	out := compactNow(t, s, u, f)
	if out.Groups != 1 || f.calls.Load() != 1 {
		t.Fatalf("MEM-CP-07, MEM-CP-08: only the thread with blocks: %+v, %d calls", out, f.calls.Load())
	}
	full, _ = s.Get(ctx, u, n.ID)
	if len(full.Appended) != 0 || full.ID != n.ID || full.Level != comm.LevelThread || !strings.HasPrefix(*full.Summary, "Свод") || !full.UserEdited {
		t.Fatalf("MEM-CP-07: the thread after the compaction: %+v", full)
	}
	if !strings.Contains(f.inputs[0], "Исправлено пользователем") || !strings.Contains(f.inputs[0], `"userEdited":true`) {
		t.Fatalf("MEM-CP-17: the edit of the user is in the input: %s", f.inputs[0])
	}
	for _, tp := range full.Topics {
		if tp.Label == "Юристы" {
			t.Fatal("MEM-CP-18: a topic taken off came back")
		}
	}
}

// MEM-CP-10, MEM-CP-28, MEM-CP-25: an answer that fails the check twice keeps the records and marks the night; a
// batch of the collection makes the compaction wait.
func TestMemCompactFailureAndBusy(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "mem-fail@x.org")
	saveChat(t, s, u, "chat-3", "2026-10-19", "Пн", nil, nil, msgAt("b1", 2026, 10, 19, 9))
	saveChat(t, s, u, "chat-3", "2026-10-20", "Вт", nil, nil, msgAt("b2", 2026, 10, 20, 9))
	f := &fakeModel{}
	f.fail.Store(true)
	out := compactNow(t, s, u, f)
	if out.Failed != 1 || f.calls.Load() != 2 || len(records(t, s, u)) != 2 {
		t.Fatalf("MEM-CP-10: %+v, %d calls, %d records", out, f.calls.Load(), len(records(t, s, u)))
	}
	var failed int
	if err := pool.QueryRow(ctx, `SELECT failed_nights FROM comm_compact_state WHERE user_id = $1`, u).Scan(&failed); err != nil || failed != 1 {
		t.Fatalf("MEM-CP-28: the failed night: %d %v", failed, err)
	}
	// MEM-CP-25: a batch of the collection at work
	if _, err := pool.Exec(ctx, `INSERT INTO comm_batches (user_id, item_id, source, threads, status) VALUES ($1, $2, 'vkteams', '[]', 'running')`, u, commItem); err != nil {
		t.Fatal(err)
	}
	f.fail.Store(false)
	if out = compactNow(t, s, u, f); !out.Busy {
		t.Fatalf("MEM-CP-25: %+v", out)
	}
}
