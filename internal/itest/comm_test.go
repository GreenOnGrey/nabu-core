//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
)

var (
	appPoolOnce sync.Once
	appPoolVal  *pgxpool.Pool
)

// appPool connects as a role that is not a superuser: the test server's own
// user bypasses row policies, the application's user on a stand does not
// (FTR.NAB.CMN-0003 tech §11).
func appPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	appPoolOnce.Do(func() {
		ctx := context.Background()
		role := "nabu_app_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
		for _, q := range []string{
			"CREATE ROLE " + role + " LOGIN PASSWORD 'app' NOSUPERUSER NOBYPASSRLS",
			"GRANT ALL ON ALL TABLES IN SCHEMA public TO " + role,
			"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO " + role,
		} {
			if _, err := pool.Exec(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
		cfg, err := pgxpool.ParseConfig(pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.User, cfg.ConnConfig.Password = role, "app"
		if appPoolVal, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	})
	if appPoolVal == nil {
		t.Fatal("no application pool")
	}
	return appPoolVal
}

func commStore(t *testing.T) *comm.Service { return &comm.Service{Pool: appPool(t)} }

var commItem = uuid.MustParse("11111111-1111-1111-1111-111111111111")

func at(day, hour int) *time.Time {
	v := time.Date(2026, 10, day, hour, 0, 0, 0, time.UTC)
	return &v
}

func saveNote(t *testing.T, s *comm.Service, uid uuid.UUID, thread, title, summary string, people []comm.PersonIn, topics []string, msgs ...comm.Message) *comm.Saved {
	t.Helper()
	res, err := s.SaveNote(context.Background(), uid, comm.SaveNote{ItemID: commItem, Source: "gmail", ThreadKey: thread, Title: title, Summary: summary,
		People: people, Topics: topics, Messages: msgs, UserEmail: "me@x.org"})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func isCode(err error, code string) bool {
	e, ok := apperr.As(err)
	return ok && e.Code == code
}

// ISO-01, ISO-02, ISO-03, LIFE-10
func TestCommIsolation(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	a, b := newUser(t, "iso-a@x.org"), newUser(t, "iso-b@x.org")
	ivanov := []comm.PersonIn{{Email: "Ivanov@x.org", Name: "Иванов А."}}
	na := saveNote(t, s, a, "thr-1", "Перенос выпуска", "Согласовали перенос на 24 октября.", ivanov, []string{"Релиз 2.3"}, comm.Message{ID: "m1", SentAt: at(12, 9)})
	nb := saveNote(t, s, b, "thr-1", "Перенос выпуска", "Выпуск сдвигается.", ivanov, []string{"Релиз"}, comm.Message{ID: "m1", SentAt: at(12, 9)})
	if na.ID == nb.ID {
		t.Fatal("two users share a note")
	}

	// ISO-01: a connection that did not name the user sees nothing.
	for _, table := range []string{"comm_notes", "comm_note_messages", "comm_note_tags", "comm_people", "comm_topics", "commitments", "comm_tombstones"} {
		var n int
		if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s without a user: %d rows, %v", table, n, err)
		}
	}
	// … and naming a user shows only that user's rows.
	_ = postgres.AsUser(ctx, s.Pool, a, func(tx pgx.Tx) error {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM comm_notes`).Scan(&n); err != nil || n != 1 {
			t.Fatalf("user A sees %d notes, %v", n, err)
		}
		return nil
	})
	// A write for another user is refused by the policy.
	err := postgres.AsUser(ctx, s.Pool, a, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO comm_people (user_id, email) VALUES ($1, 'x@x.org')`, b)
		return err
	})
	if err == nil {
		t.Fatal("user A wrote a row of user B")
	}

	// ISO-02: the ids of another user are not found — to read, change or delete.
	if _, err := s.Get(ctx, b, na.ID); !isCode(err, "not_found") {
		t.Fatalf("B read the note of A: %v", err)
	}
	text := "взлом"
	if _, err := s.Update(ctx, b, na.ID, comm.NotePatch{Summary: &text}); !isCode(err, "not_found") {
		t.Fatalf("B changed the note of A: %v", err)
	}
	if err := s.Delete(ctx, b, na.ID); !isCode(err, "not_found") {
		t.Fatalf("B deleted the note of A: %v", err)
	}
	cid, err := s.SaveCommitment(ctx, a, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "thr-1", Text: "Прислать план", Direction: comm.Mine, PersonEmail: "ivanov@x.org"})
	if err != nil {
		t.Fatal(err)
	}
	done := comm.Done
	if _, err := s.UpdateCommitment(ctx, b, cid, comm.CommitmentPatch{State: &done}, comm.ByUser); !isCode(err, "not_found") {
		t.Fatalf("B closed the commitment of A: %v", err)
	}
	topicsA, _ := s.Tags(ctx, a, comm.KindTopic, "", 0)
	if err := s.RenameTopic(ctx, b, topicsA[0].ID, "чужая"); !isCode(err, "not_found") {
		t.Fatalf("B renamed the topic of A: %v", err)
	}
	if l, _ := s.Search(ctx, b, comm.Filter{Query: "24 октября"}, 10); len(l) != 0 {
		t.Fatalf("B found the note of A: %+v", l)
	}

	// ISO-03: an edit of one participant is not seen by the other.
	if _, err := s.Update(ctx, a, na.ID, comm.NotePatch{Summary: &text}); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Get(ctx, b, nb.ID); n == nil || *n.Summary != "Выпуск сдвигается." {
		t.Fatalf("the note of B changed: %+v", n)
	}

	// LIFE-10: the rows go with the account although the policies are forced.
	if _, err := pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, a); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM comm_notes WHERE user_id = $1) + (SELECT count(*) FROM commitments WHERE user_id = $1)
		+ (SELECT count(*) FROM comm_people WHERE user_id = $1) + (SELECT count(*) FROM comm_note_tags WHERE user_id = $1)`, a).Scan(&left); err != nil || left != 0 {
		t.Fatalf("rows of the deleted account left: %d, %v", left, err)
	}
}

// NOTE-02, NOTE-03, NOTE-07, NOTE-09, NOTE-11, NOTE-14, NOTE-15
func TestCommNotes(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "notes@x.org")
	people := []comm.PersonIn{{Email: "Ivanov@x.org", Name: "Иванов А."}, {Email: "me@x.org", Name: "Я"}}
	first := saveNote(t, s, u, "thr", "Сроки 2.3", "Обсуждаем сроки.", people, []string{"Релиз 2.3"},
		comm.Message{ID: "m1", SentAt: at(12, 9)}, comm.Message{ID: "m2", SentAt: at(12, 10)}, comm.Message{ID: "m3", SentAt: at(12, 11)})
	if !first.Created || first.NewMessages != 3 {
		t.Fatalf("%+v", first)
	}
	// NOTE-03: the same batch again changes nothing.
	again := saveNote(t, s, u, "thr", "Сроки 2.3", "Обсуждаем сроки.", people, []string{"Релиз 2.3"},
		comm.Message{ID: "m1", SentAt: at(12, 9)}, comm.Message{ID: "m2", SentAt: at(12, 10)}, comm.Message{ID: "m3", SentAt: at(12, 11)})
	if again.Created || again.NewMessages != 0 || again.ID != first.ID {
		t.Fatalf("repeated batch: %+v", again)
	}
	// NOTE-02: two more letters — the same note, five messages, a new summary.
	more := saveNote(t, s, u, "thr", "Перенос выпуска 2.3", "Выпуск переносится на 24 октября.", []comm.PersonIn{{Email: "ivanov@x.org", Name: "Иванов Андрей"}}, []string{"релиз  2.3"},
		comm.Message{ID: "m4", SentAt: at(12, 14)}, comm.Message{ID: "m5", SentAt: at(13, 9)})
	n, err := s.Get(ctx, u, first.ID)
	if err != nil || more.ID != first.ID || n.MessageCount != 5 || len(n.Messages) != 5 || *n.Summary != "Выпуск переносится на 24 октября." ||
		!n.FirstAt.Equal(*at(12, 9)) || !n.LastAt.Equal(*at(13, 9)) {
		t.Fatalf("%+v %v", n, err)
	}
	// NOTE-09: one address is one person whatever the name and the case; the user is not a tag (R8).
	// NOTE-10: the same topic in another spelling is the same topic.
	if len(n.People) != 1 || n.People[0].Label != "Иванов А." || len(n.Topics) != 1 || n.Topics[0].Label != "Релиз 2.3" {
		t.Fatalf("tags: %+v %+v", n.People, n.Topics)
	}
	if st, _ := s.State(ctx, u); st.Notes != 1 {
		t.Fatalf("notes counter %d", st.Notes)
	}

	// NOTE-11: more than five topics are refused; a note without topics is saved (R8 v1.5).
	if _, err := s.SaveNote(ctx, u, comm.SaveNote{ItemID: commItem, Source: "gmail", ThreadKey: "x", Title: "t", Summary: "s", Topics: []string{"a", "b", "c", "d", "e", "f"}}); !isCode(err, "invalid_topics") {
		t.Fatalf("six topics: %v", err)
	}
	if bare, err := s.SaveNote(ctx, u, comm.SaveNote{ItemID: commItem, Source: "gmail", ThreadKey: "notice", Title: "Подтверждение регистрации", Summary: "Автоматическое письмо", Topics: nil}); err != nil {
		t.Fatalf("no topics: %v", err)
	} else if n, _ := s.Get(ctx, u, bare.ID); len(n.Topics) != 0 {
		t.Fatalf("topics of a bare note: %+v", n.Topics)
	} else if err := s.Delete(ctx, u, bare.ID); err != nil {
		t.Fatalf("delete the bare note: %v", err)
	}

	// NOTE-07, NOTE-08: a secret the model carried over is masked by the handler.
	sec := saveNote(t, s, u, "sec", "Доступ к стенду", "Иванов передал доступ. Пароль: Qwerty!2345, ключ AKIAIOSFODNN7EXAMPLE.", nil, []string{"Стенд"}, comm.Message{ID: "s1"})
	if n, _ := s.Get(ctx, u, sec.ID); strings.Contains(*n.Summary, "Qwerty") || strings.Contains(*n.Summary, "AKIA") || !strings.Contains(*n.Summary, comm.Masked) {
		t.Fatalf("a secret in the summary: %s", *n.Summary)
	}

	// NOTE-14: the user's text stays, the news go to a separate block.
	mine := "Мой текст: перенос согласован."
	if _, err := s.Update(ctx, u, first.ID, comm.NotePatch{Summary: &mine}); err != nil {
		t.Fatal(err)
	}
	saveNote(t, s, u, "thr", "Заголовок сборщика", "Петрова подтвердила заметки о выпуске.", nil, []string{"Найм"}, comm.Message{ID: "m6", SentAt: at(13, 12)})
	n, _ = s.Get(ctx, u, first.ID)
	if *n.Summary != mine || !n.UserEdited || len(n.Appended) != 1 || n.Appended[0].Text != "Петрова подтвердила заметки о выпуске." || n.MessageCount != 6 {
		t.Fatalf("edited note: %+v", n)
	}
	if len(n.Topics) != 2 { // an edited note only gains tags
		t.Fatalf("topics of an edited note: %+v", n.Topics)
	}
	// … and a repeated batch without new messages appends nothing.
	saveNote(t, s, u, "thr", "Заголовок сборщика", "Ещё раз то же.", nil, []string{"Найм"}, comm.Message{ID: "m6", SentAt: at(13, 12)})
	if n, _ = s.Get(ctx, u, first.ID); len(n.Appended) != 1 {
		t.Fatalf("appended twice: %+v", n.Appended)
	}

	// NOTE-15: a deleted note is not collected again.
	if err := s.Delete(ctx, u, first.ID); err != nil {
		t.Fatal(err)
	}
	if res := saveNote(t, s, u, "thr", "Сроки 2.3", "Снова.", nil, []string{"Релиз 2.3"}, comm.Message{ID: "m7"}); !res.Skipped {
		t.Fatalf("a deleted note came back: %+v", res)
	}
	if st, _ := s.State(ctx, u); st.Notes != 1 {
		t.Fatalf("notes counter after delete %d", st.Notes)
	}
}

// NOTE-12, NOTE-13, NOTE-16, VIEW-01, VIEW-04, VIEW-05, VIEW-09
func TestCommTagsAndGraph(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "graph@x.org")
	p := func(names ...string) []comm.PersonIn {
		var out []comm.PersonIn
		for _, n := range names {
			out = append(out, comm.PersonIn{Email: n + "@x.org", Name: n})
		}
		return out
	}
	// Six notes with known tags.
	saveNote(t, s, u, "g1", "Перенос", "Перенос выпуска, договор 4500123456.", p("ivanov", "petrova"), []string{"Релиз"}, comm.Message{ID: "1", SentAt: at(1, 9)})
	saveNote(t, s, u, "g2", "Миграция", "Скрипт миграции.", p("sidorov"), []string{"Релиз", "Миграция"}, comm.Message{ID: "1", SentAt: at(2, 9)})
	saveNote(t, s, u, "g3", "Сбой", "Истёк сертификат.", p("kim"), []string{"Релиз", "Инциденты"}, comm.Message{ID: "1", SentAt: at(3, 9)})
	saveNote(t, s, u, "g4", "Дежурство", "Кто дежурит.", p("ivanov"), []string{"Релиз"}, comm.Message{ID: "1", SentAt: at(4, 9)})
	saveNote(t, s, u, "g5", "Смета", "Смета на квартал.", p("ivanov"), []string{"Бюджет"}, comm.Message{ID: "1", SentAt: at(5, 9)})
	saveNote(t, s, u, "g6", "Найм", "Собеседование.", p("petrova"), []string{"Найм"}, comm.Message{ID: "1", SentAt: at(6, 9)})

	tag := func(kind, label string) uuid.UUID {
		t.Helper()
		l, err := s.Tags(ctx, u, kind, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range l {
			if x.Label == label {
				return x.ID
			}
		}
		t.Fatalf("no tag %s", label)
		return uuid.Nil
	}
	release, ivanov := tag(comm.KindTopic, "Релиз"), tag(comm.KindPerson, "ivanov")

	// VIEW-01: filters and their combination.
	count := func(f comm.Filter) int {
		t.Helper()
		l, err := s.List(ctx, u, f, httpx.Page{Limit: 50})
		if err != nil {
			t.Fatal(err)
		}
		return len(l.Items)
	}
	from, to := at(2, 0), at(5, 0)
	if a, b, c, d := count(comm.Filter{Topic: &release}), count(comm.Filter{Person: &ivanov}), count(comm.Filter{Topic: &release, Person: &ivanov}),
		count(comm.Filter{From: from, To: to}); a != 4 || b != 3 || c != 2 || d != 3 {
		t.Fatalf("filters: %d %d %d %d", a, b, c, d)
	}
	if count(comm.Filter{Source: "vkteams"}) != 0 || count(comm.Filter{Source: "gmail"}) != 6 {
		t.Fatal("source filter")
	}
	// Pages: the latest first, no overlaps.
	l1, _ := s.List(ctx, u, comm.Filter{}, httpx.Page{Limit: 4})
	if len(l1.Items) != 4 || l1.NextCursor == nil || l1.Items[0].Title != "Найм" {
		t.Fatalf("page 1: %+v", l1)
	}
	// VIEW-09: an exact number is found by words.
	if l, _ := s.Search(ctx, u, comm.Filter{Query: "4500123456"}, 10); len(l) != 1 || l[0].Title != "Перенос" {
		t.Fatalf("search by a number: %+v", l)
	}
	if l, _ := s.Search(ctx, u, comm.Filter{Query: "сертиф"}, 10); len(l) != 1 { // a part of a word
		t.Fatalf("search by a part of a word: %+v", l)
	}

	// VIEW-04: the nodes and the edges of the «Релиз» notes.
	g, err := s.Graph(ctx, u, comm.Filter{Topic: &release}, 0, 150)
	if err != nil {
		t.Fatal(err)
	}
	nodes := map[string]comm.Node{}
	for _, n := range g.Nodes {
		nodes[n.Label] = n
	}
	if len(g.Nodes) != 7 || g.TotalNodes != 7 || nodes["Релиз"].Notes != 4 || nodes["Релиз"].Links != 6 || nodes["ivanov"].Notes != 2 || nodes["ivanov"].Kind != comm.KindPerson {
		t.Fatalf("nodes: %+v", g.Nodes)
	}
	edge := func(a, b string) int {
		for _, e := range g.Edges {
			if (e.A == nodes[a].ID && e.B == nodes[b].ID) || (e.A == nodes[b].ID && e.B == nodes[a].ID) {
				return e.Notes
			}
		}
		return 0
	}
	if edge("Релиз", "ivanov") != 2 || edge("ivanov", "petrova") != 1 || edge("Релиз", "Миграция") != 1 || edge("sidorov", "Миграция") != 1 || edge("kim", "sidorov") != 0 {
		t.Fatalf("edges: %+v", g.Edges)
	}
	// VIEW-05: the most linked nodes stay, the total is reported; minLinks cuts.
	g, _ = s.Graph(ctx, u, comm.Filter{}, 0, 3)
	if len(g.Nodes) != 3 || g.TotalNodes != 9 || g.Nodes[0].Label != "Релиз" {
		t.Fatalf("limited graph: %+v total %d", g.Nodes, g.TotalNodes)
	}
	for _, e := range g.Edges {
		in := 0
		for _, n := range g.Nodes {
			if n.ID == e.A || n.ID == e.B {
				in++
			}
		}
		if in != 2 {
			t.Fatalf("an edge to a node that was cut: %+v", e)
		}
	}
	if g, _ = s.Graph(ctx, u, comm.Filter{}, 3, 150); len(g.Nodes) == 0 || g.Nodes[len(g.Nodes)-1].Links < 3 {
		t.Fatalf("minLinks: %+v", g.Nodes)
	}

	// NOTE-12: rename and merge.
	budget, hiring := tag(comm.KindTopic, "Бюджет"), tag(comm.KindTopic, "Найм")
	if err := s.RenameTopic(ctx, u, budget, "Бюджет Q4"); err != nil {
		t.Fatal(err)
	}
	if err := s.RenameTopic(ctx, u, budget, "найм"); !isCode(err, "topic_exists") {
		t.Fatalf("rename to an existing topic: %v", err)
	}
	if err := s.MergeTopic(ctx, u, hiring, release); err != nil {
		t.Fatal(err)
	}
	if count(comm.Filter{Topic: &release}) != 5 || count(comm.Filter{Topic: &hiring}) != 0 {
		t.Fatal("merge did not move the notes")
	}
	// NOTE-13: a deleted topic leaves its notes.
	if err := s.DeleteTopic(ctx, u, budget); err != nil {
		t.Fatal(err)
	}
	if count(comm.Filter{}) != 6 {
		t.Fatal("deleting a topic deleted notes")
	}
	// NOTE-16: every note of a person — only with the confirmation.
	if _, err := s.DeleteByTag(ctx, u, comm.KindPerson, ivanov, ""); !isCode(err, "confirmation_required") {
		t.Fatalf("%v", err)
	}
	if n, err := s.DeleteByTag(ctx, u, comm.KindPerson, ivanov, ivanov.String()); err != nil || n != 3 || count(comm.Filter{}) != 3 {
		t.Fatalf("delete by a person: %d %v", n, err)
	}
	// LIFE-08: delete everything collected.
	if err := s.DeleteAll(ctx, u, ""); !isCode(err, "confirmation_required") {
		t.Fatalf("%v", err)
	}
	if err := s.DeleteAll(ctx, u, "all"); err != nil {
		t.Fatal(err)
	}
	if l, _ := s.Tags(ctx, u, comm.KindPerson, "", 0); count(comm.Filter{}) != 0 || len(l) != 0 {
		t.Fatal("something is left after deleting everything")
	}
}

// CMT-04, CMT-08, DIG-01, DIG-02
func TestCommitments(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "cmt@x.org")
	saveNote(t, s, u, "c1", "Выпуск", "Обсудили выпуск.", []comm.PersonIn{{Email: "petrova@x.org", Name: "Петрова М."}}, []string{"Релиз"}, comm.Message{ID: "m1", URL: "https://mail/1"})
	day := func(d int) *comm.Date { return &comm.Date{Time: time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC)} }
	add := func(text, dir string, due *comm.Date) uuid.UUID {
		t.Helper()
		id, err := s.SaveCommitment(ctx, u, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "c1", Text: text, Direction: dir,
			PersonEmail: "petrova@x.org", Due: due, BasisMessageID: "m1"})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	// Wednesday 14 October 2026 is "today".
	today := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	late := add("Проверить формулировки", comm.Mine, day(10))
	now := add("Отправить смету", comm.Mine, day(14))
	owed := add("Прислать отчёт", comm.Theirs, day(14))
	week := add("Согласовать перенос", comm.Mine, day(16))
	next := add("Собеседование", comm.Mine, day(22))
	none := add("Добавить проверку в чек-лист", comm.Mine, nil)
	if again := add("отправить смету", comm.Mine, day(14)); again != now { // the same text and basis
		t.Fatal("a duplicate commitment")
	}
	if _, err := s.SaveCommitment(ctx, u, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "nope", Text: "x", Direction: comm.Mine}); !isCode(err, "note_missing") {
		t.Fatalf("a commitment without a note: %v", err)
	}

	ids := func(items []comm.DigestItem) string {
		var b strings.Builder
		for _, it := range items {
			fmt.Fprintf(&b, "%s:%s ", it.Group, map[uuid.UUID]string{late: "late", now: "now", owed: "owed", week: "week", next: "next", none: "none"}[it.ID])
		}
		return strings.TrimSpace(b.String())
	}
	// DIG-01: the daily digest — overdue, today, owed to me; nothing without a day.
	d, err := s.Digest(ctx, u, "day", today)
	if err != nil || ids(d) != "overdue:late due:now theirs:owed" {
		t.Fatalf("day: %q %v", ids(d), err)
	}
	if d[0].Person == nil || d[0].Person.Label != "Петрова М." || d[0].BasisURL != "https://mail/1" || !d[0].Overdue || len(d[0].Topics) != 1 {
		t.Fatalf("digest item: %+v", d[0])
	}
	// The weekly one — up to Sunday the 18th, plus those without a day.
	w, _ := s.Digest(ctx, u, "week", today)
	if ids(w) != "overdue:late due:now due:week theirs:owed undated:none" {
		t.Fatalf("week: %q", ids(w))
	}
	// DIG-02: "today" is the user's day: late on the 13th in UTC is already the 14th in Novosibirsk.
	nsk, _ := time.LoadLocation("Asia/Novosibirsk")
	evening := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	if d, _ := s.Digest(ctx, u, "day", evening.In(nsk)); ids(d) != "overdue:late due:now theirs:owed" {
		t.Fatalf("day in Novosibirsk: %q", ids(d))
	}
	if d, _ := s.Digest(ctx, u, "day", evening); ids(d) != "overdue:late" {
		t.Fatalf("day in UTC: %q", ids(d))
	}

	// CMT-04: close and return.
	done, open := comm.Done, comm.Open
	c, err := s.UpdateCommitment(ctx, u, now, comm.CommitmentPatch{State: &done}, comm.ByUser)
	if err != nil || c.State != comm.Done || c.ClosedBy == nil || *c.ClosedBy != comm.ByUser {
		t.Fatalf("%+v %v", c, err)
	}
	// CMT-06 (the store part): the collector closes with the message it relied on.
	c, _ = s.UpdateCommitment(ctx, u, owed, comm.CommitmentPatch{State: &done, CloseBasisMessageID: "m1"}, comm.ByCollector)
	if *c.ClosedBy != comm.ByCollector || c.CloseBasisURL != "https://mail/1" {
		t.Fatalf("closed by the collector: %+v", c)
	}
	if d, _ := s.Digest(ctx, u, "day", today); ids(d) != "overdue:late" {
		t.Fatalf("after closing: %q", ids(d))
	}
	if c, _ = s.UpdateCommitment(ctx, u, now, comm.CommitmentPatch{State: &open}, comm.ByUser); c.State != comm.Open || c.ClosedBy != nil {
		t.Fatalf("returned: %+v", c)
	}
	// CMT-08: a new due day moves the commitment between the groups.
	if _, err := s.UpdateCommitment(ctx, u, late, comm.CommitmentPatch{SetDue: true, Due: day(20)}, comm.ByUser); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateCommitment(ctx, u, none, comm.CommitmentPatch{SetDue: true, Due: day(14)}, comm.ByUser); err != nil {
		t.Fatal(err)
	}
	if d, _ := s.Digest(ctx, u, "day", today); ids(d) != "due:now due:none" {
		t.Fatalf("after changing the days: %q", ids(d))
	}
	closed, _ := s.Commitments(ctx, u, comm.CommitmentFilter{State: "closed"}, 0)
	theirs, _ := s.Commitments(ctx, u, comm.CommitmentFilter{State: "all", Direction: comm.Theirs}, 0)
	if len(closed) != 1 || len(theirs) != 1 {
		t.Fatalf("filters: %d closed, %d theirs", len(closed), len(theirs))
	}
	// The note shows its commitments; deleting it deletes them.
	notes, _ := s.List(ctx, u, comm.Filter{}, httpx.Page{Limit: 5})
	if notes.Items[0].OpenCommitments != 5 {
		t.Fatalf("open commitments of the note: %d", notes.Items[0].OpenCommitments)
	}
	if err := s.Delete(ctx, u, notes.Items[0].ID); err != nil {
		t.Fatal(err)
	}
	if all, _ := s.Commitments(ctx, u, comm.CommitmentFilter{State: "all"}, 0); len(all) != 0 {
		t.Fatalf("commitments of a deleted note: %d", len(all))
	}
}

// R21, R29: settings and the consent.
func TestCommSettings(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "set@x.org")
	if _, err := s.Allowed(ctx); !isCode(err, "comm_disabled") {
		t.Fatalf("allowed by default: %v", err)
	}
	on, days := true, 90
	st, err := s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &on, RetentionDays: &days, Model: []byte(`{"connectionId":"c1","model":"m"}`)})
	if err != nil || !st.Enabled || st.RetentionDays != 90 || st.Model == nil || st.Model.Model != "m" {
		t.Fatalf("%+v %v", st, err)
	}
	zero := 0
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{RetentionDays: &zero}); !isCode(err, "invalid_retention") {
		t.Fatalf("%v", err)
	}
	if st, _ = s.Allowed(ctx); st.RetentionDays != 90 || st.Model == nil {
		t.Fatalf("a partial patch dropped values: %+v", st)
	}
	if ok, _ := s.Consented(ctx, u); ok {
		t.Fatal("consented before accepting")
	}
	if err := s.Consent(ctx, u); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Consented(ctx, u); !ok {
		t.Fatal("the consent is not recorded")
	}
	off := false
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off}); err != nil {
		t.Fatal(err)
	}
}

// SRC-01, SRC-02, TASK-01, TASK-02, TASK-05…TASK-09, TASK-14…TASK-17, LIFE-08
func TestMemoryTasks(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	on := true
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &on}); err != nil {
		t.Fatal(err)
	}
	uid := newUser(t, "mt@x.org")
	ts := &tasks.Service{Pool: pool, Bus: &bus{}, Rules: tasks.Rules{MinInterval: 15 * time.Minute, MaxActive: 1}, Catchup: time.Hour, Tick: time.Second,
		MaxFailures: 3, DefaultTimezone: "Europe/Moscow"}
	ts.MemoryGate = func(ctx context.Context, uid uuid.UUID, kind string, settings []byte) error {
		if kind != tasks.KindCollect {
			return nil
		}
		return s.CheckCollect(ctx, uid, settings)
	}
	yes := true
	enable := func(kind, settings string) (*tasks.MemoryTask, error) {
		p := tasks.MemoryPatch{Enabled: &yes}
		if settings != "" {
			p.Settings = []byte(settings)
		}
		return ts.SetMemoryTask(ctx, uid, kind, p)
	}

	// SRC-01: only an item with an adapter becomes a source.
	var item uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO catalog_items (type, name, title, source, mode, published) VALUES ('mcp', $1, 'Gmail', '{}', 'personal', true) RETURNING id`,
		"gmail-"+uuid.NewString()[:6]).Scan(&item); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSource(ctx, item, nil, &yes); !isCode(err, "comm_adapter_missing") {
		t.Fatalf("a source without an adapter: %v", err)
	}
	bad, gmail := "imap", "gmail"
	if err := s.SetSource(ctx, item, &bad, &yes); !isCode(err, "comm_adapter_missing") {
		t.Fatalf("an unknown adapter: %v", err)
	}
	if err := s.SetSource(ctx, item, &gmail, &yes); err != nil {
		t.Fatal(err)
	}
	// SRC-03: a server whose checked tools lack what the adapter calls is not a source.
	no := false
	_ = s.SetSource(ctx, item, nil, &no)
	if _, err := pool.Exec(ctx, `UPDATE catalog_items SET tools = '[{"name":"search_threads"},{"name":"list_labels"}]' WHERE id = $1`, item); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSource(ctx, item, nil, &yes); !isCode(err, "comm_source_tools_missing") {
		t.Fatalf("a server without get_thread: %v", err)
	}
	_, _ = pool.Exec(ctx, `UPDATE catalog_items SET tools = '[{"name":"search_threads"},{"name":"get_thread"}]' WHERE id = $1`, item)
	if err := s.SetSource(ctx, item, nil, &yes); err != nil {
		t.Fatal(err)
	}
	src := fmt.Sprintf(`{"sources":[{"itemId":%q,"source":"gmail"}]}`, item)

	// TASK-01: no consent. TASK-02: the source is not connected.
	if _, err := enable("collect", src); !isCode(err, "comm_consent_required") {
		t.Fatalf("collection without the consent: %v", err)
	}
	if err := s.Consent(ctx, uid); err != nil {
		t.Fatal(err)
	}
	if _, err := enable("collect", ""); !isCode(err, "comm_source_not_connected") {
		t.Fatalf("collection without sources: %v", err)
	}
	if _, err := enable("collect", src); !isCode(err, "comm_source_not_connected") {
		t.Fatalf("collection from a source that is not connected: %v", err)
	}
	// SRC-02: the source appears when the user connected the item.
	if _, err := pool.Exec(ctx, `INSERT INTO user_connections (user_id, item_id) VALUES ($1, $2)`, uid, item); err != nil {
		t.Fatal(err)
	}
	l0, err := s.Sources(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, x := range l0 { // other tests leave their sources in the catalog
		if x.ItemID == item {
			found = x.Source == "gmail" && x.Connected
		}
	}
	if !found {
		t.Fatalf("sources: %+v", l0)
	}
	// TASK-02: a source the administrator unmarked is skipped, as the collection
	// skips it (the user no longer sees it to switch it off); alone it is no source.
	var gone uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO catalog_items (type, name, title, source, mode, published) VALUES ('mcp', $1, 'Old', '{}', 'personal', true) RETURNING id`,
		"old-"+uuid.NewString()[:6]).Scan(&gone); err != nil {
		t.Fatal(err)
	}
	if _, err := enable("collect", fmt.Sprintf(`{"sources":[{"itemId":%q,"source":"gmail"}]}`, gone)); !isCode(err, "comm_source_not_connected") {
		t.Fatalf("collection from an unmarked source only: %v", err)
	}
	if col, err := enable("collect", fmt.Sprintf(`{"sources":[{"itemId":%q,"source":"gmail"},{"itemId":%q,"source":"gmail"}]}`, gone, item)); err != nil || !col.Enabled {
		t.Fatalf("collection with an unmarked source next to a connected one: %+v %v", col, err)
	}
	col, err := enable("collect", src)
	if err != nil || !col.Enabled || col.Status != "active" || col.NextRunAt == nil || col.TaskID == nil {
		t.Fatalf("%+v %v", col, err)
	}
	var set tasks.CollectSettings
	_ = json.Unmarshal(col.Settings, &set)
	if set.Interval != "1h" || len(set.Sources) != 1 || set.RoundTheClock {
		t.Fatalf("collect settings: %+v", set)
	}
	if _, err := enable("collect", `{"interval":"5m"}`); !isCode(err, "invalid_interval") {
		t.Fatalf("%v", err)
	}
	// a patch of one field keeps the others
	col, _ = enable("collect", `{"interval":"4h","exclusions":{"senders":["hr@x.org"," hr@x.org "]}}`)
	_ = json.Unmarshal(col.Settings, &set)
	if set.Interval != "4h" || len(set.Sources) != 1 || len(set.Exclusions.Senders) != 1 {
		t.Fatalf("patched settings: %+v", set)
	}

	day, err := enable("digest_day", `{"time":"08:30","days":[1,3,7]}`)
	if err != nil || day.Schedule == nil || day.Schedule.Cron != "30 8 * * 0,1,3" {
		t.Fatalf("%+v %v", day, err)
	}
	if _, err := enable("digest_day", `{"days":[]}`); !isCode(err, "invalid_day") {
		t.Fatalf("%v", err)
	}
	if _, err := enable("digest_week", `{"channel":"telegram"}`); err != nil { // no Deliverable in the test: accepted
		t.Fatal(err)
	}
	week, _ := enable("digest_week", `{"weekday":5,"time":"10:00","channel":"web"}`)
	if week.Schedule.Cron != "0 10 * * 5" {
		t.Fatalf("%+v", week.Schedule)
	}

	// TASK-05: they are tasks of the Tasks section, with their kind.
	l, err := ts.List(ctx, uid, "active", httpx.Page{Limit: 10})
	if err != nil || len(l.Items) != 3 {
		t.Fatalf("%+v %v", l, err)
	}
	kinds := map[string]bool{}
	for _, x := range l.Items {
		kinds[x.Kind] = true
	}
	if !kinds[tasks.KindCollect] || !kinds[tasks.KindDigestDay] || !kinds[tasks.KindDigestWeek] {
		t.Fatalf("kinds: %v", kinds)
	}
	// TASK-07: they do not take the limit of active tasks (1 here).
	in := tasks.CreateInput{Title: "Metrics", Instruction: "Send metrics"}
	in.Schedule.Cron = "0 10 * * 1"
	own, err := ts.Create(ctx, uid, in, "web", nil)
	if err != nil || own.Kind != tasks.KindAgent {
		t.Fatalf("an ordinary task next to memory tasks: %+v %v", own, err)
	}
	if _, err := ts.Create(ctx, uid, in, "web", nil); err == nil {
		t.Fatal("the limit of ordinary tasks is not checked")
	}

	// TASK-09: pause and resume in the Tasks section.
	if p, err := ts.Pause(ctx, uid, *day.TaskID); err != nil || p.Status != "paused" || p.PauseKind == nil || *p.PauseKind != tasks.PauseUser || p.NextRunAt != nil {
		t.Fatalf("%+v %v", p, err)
	}
	if mt, _ := ts.MemoryTaskOf(ctx, uid, tasks.KindDigestDay); !mt.Enabled || mt.Status != "paused" {
		t.Fatalf("a paused task in Memory: %+v", mt)
	}
	if r, err := ts.Resume(ctx, uid, *day.TaskID); err != nil || r.Status != "active" || r.PauseKind != nil || r.NextRunAt == nil {
		t.Fatalf("%+v %v", r, err)
	}

	// TASK-14, TASK-15: no access to the source pauses without a failure and resumes by itself.
	if ok, err := ts.PauseMemory(ctx, *col.TaskID, tasks.PauseSourceAccess, "no access"); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	if ok, _ := ts.PauseMemory(ctx, *col.TaskID, tasks.PauseSourceAccess, "no access"); ok {
		t.Fatal("paused twice: the user would get two messages")
	}
	cur, _ := ts.Get(ctx, uid, *col.TaskID)
	if cur.Status != "paused" || cur.Failures != 0 || *cur.PauseKind != tasks.PauseSourceAccess {
		t.Fatalf("%+v", cur)
	}
	if n, _ := ts.ResumeMemory(ctx, &uid, tasks.PauseBudget); n != 0 { // TASK-17 resumes only its own kind
		t.Fatal("resumed a task paused for another reason")
	}
	if n, err := ts.ResumeMemory(ctx, &uid, tasks.PauseSourceAccess); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	if cur, _ = ts.Get(ctx, uid, *col.TaskID); cur.Status != "active" || cur.NextRunAt == nil || cur.PauseKind != nil {
		t.Fatalf("%+v", cur)
	}
	// TASK-16, TASK-17: the budget.
	_, _ = ts.PauseMemory(ctx, *col.TaskID, tasks.PauseBudget, "limit")
	if n, _ := ts.ResumeMemory(ctx, nil, tasks.PauseBudget); n != 1 {
		t.Fatalf("budget resume: %d", n)
	}
	if ok, _ := ts.PauseMemory(ctx, own.ID, tasks.PauseBudget, "x"); ok {
		t.Fatal("an ordinary task paused as a memory task")
	}

	// TASK-06: switched off in Memory — not among the active, the row and its runs stay.
	off, err := ts.SetMemoryTask(ctx, uid, "digest_week", tasks.MemoryPatch{Enabled: &no})
	if err != nil || off.Enabled || off.Status != "off" || *off.TaskID != *week.TaskID {
		t.Fatalf("%+v %v", off, err)
	}
	if l, _ = ts.List(ctx, uid, "active", httpx.Page{Limit: 10}); len(l.Items) != 3 {
		t.Fatalf("active after switching off: %d", len(l.Items))
	}
	back, _ := enable("digest_week", "")
	if *back.TaskID != *week.TaskID || back.Status != "active" || back.Schedule.Cron != "0 10 * * 5" {
		t.Fatalf("switched on again: %+v", back)
	}
	// «Отменить» in the Tasks section switches a memory task off.
	if c, err := ts.Cancel(ctx, uid, *back.TaskID); err != nil || c.Status != "cancelled" {
		t.Fatalf("%+v %v", c, err)
	}
	if mt, _ := ts.MemoryTaskOf(ctx, uid, tasks.KindDigestWeek); mt.Enabled {
		t.Fatal("cancelled in Tasks, still on in Memory")
	}
	// LIFE-08: deleting everything collected switches the memory tasks off, the ordinary one stays.
	if err := ts.DisableMemoryTasks(ctx, uid); err != nil {
		t.Fatal(err)
	}
	if l, _ = ts.List(ctx, uid, "active", httpx.Page{Limit: 10}); len(l.Items) != 1 || l.Items[0].ID != own.ID {
		t.Fatalf("after disabling the memory tasks: %+v", l.Items)
	}
	off2 := false
	_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off2})
}

// DIG-01, DIG-03, DIG-04, DIG-07, LIFE-07
func TestDigestRun(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	on := true
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &on}); err != nil {
		t.Fatal(err)
	}
	uid := newUser(t, "dig@x.org")
	if _, err := pool.Exec(ctx, `UPDATE users SET language = 'ru', timezone = 'Asia/Novosibirsk' WHERE id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	ev := &capture{}
	ts := &tasks.Service{Pool: pool, Events: ev, Bus: &bus{}, Rules: tasks.Rules{MinInterval: time.Minute, MaxActive: 5}, Catchup: time.Hour, Tick: time.Second,
		MaxFailures: 3, DefaultTimezone: "Europe/Moscow"}
	store := &chat.Store{Pool: pool}
	// DIG-07: no operator, no pods, no models — a digest needs none of them.
	eng := &engine.Engine{Pool: pool, Users: repo(), Chat: store, Tasks: ts, Comm: s, Events: ev}
	yes := true
	mt, err := ts.SetMemoryTask(ctx, uid, "digest_day", tasks.MemoryPatch{Enabled: &yes})
	if err != nil {
		t.Fatal(err)
	}
	run := func() (status, summary string, message *uuid.UUID, found bool) {
		t.Helper()
		var id uuid.UUID
		if err := pool.QueryRow(ctx, `INSERT INTO task_runs (task_id, scheduled_for, status) VALUES ($1, now(), 'running') RETURNING id`, *mt.TaskID).Scan(&id); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(tasks.RunMessage{RunID: id, TaskID: *mt.TaskID, UserID: uid})
		if err := eng.HandleTaskRun(ctx, []byte(uid.String()), b); err != nil {
			t.Fatal(err)
		}
		var sum *string
		err := pool.QueryRow(ctx, `SELECT status, summary, message_id FROM task_runs WHERE id = $1`, id).Scan(&status, &sum, &message)
		if postgres.IsNoRows(err) {
			return "", "", nil, false
		}
		if err != nil {
			t.Fatal(err)
		}
		if sum != nil {
			summary = *sum
		}
		return status, summary, message, true
	}
	// DIG-03: nothing to remind of — no message, the run is successful.
	if st, sum, msg, _ := run(); st != "succeeded" || sum != "nothing_to_remind" || msg != nil {
		t.Fatalf("empty digest: %s %q %v", st, sum, msg)
	}
	// DIG-04: the digest works on what is collected, the collection is off.
	nsk, _ := time.LoadLocation("Asia/Novosibirsk")
	today := time.Now().In(nsk)
	due := func(shift int) *comm.Date {
		d := today.AddDate(0, 0, shift)
		return &comm.Date{Time: time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)}
	}
	saveNote(t, s, uid, "d1", "Выпуск", "Обсудили выпуск.", []comm.PersonIn{{Email: "petrova@x.org", Name: "Петрова М."}}, []string{"Релиз"},
		comm.Message{ID: "m1", URL: "https://mail.google.com/mail/#all/abc"})
	for _, c := range []struct {
		text, dir string
		due       *comm.Date
	}{{"Проверить формулировки", comm.Mine, due(-3)}, {"Отправить смету", comm.Mine, due(0)}, {"Прислать отчёт", comm.Theirs, due(0)}, {"Без срока", comm.Mine, nil}, {"Позже", comm.Mine, due(9)}} {
		if _, err := s.SaveCommitment(ctx, uid, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "d1", Text: c.text, Direction: c.dir,
			PersonEmail: "petrova@x.org", Due: c.due, BasisMessageID: "m1"}); err != nil {
			t.Fatal(err)
		}
	}
	st, sum, msgID, _ := run()
	if st != "succeeded" || sum != "items:3" || msgID == nil {
		t.Fatalf("digest: %s %q %v", st, sum, msgID)
	}
	// DIG-01: the message is in the main conversation: the text by groups with links and the card.
	m, err := store.Message(ctx, uid, *msgID)
	if err != nil {
		t.Fatal(err)
	}
	main, _ := store.Main(ctx, uid)
	if m.ConversationID != main.ID || !strings.HasPrefix(m.Channel, "task:") || m.Role != "assistant" {
		t.Fatalf("%+v", m)
	}
	for _, want := range []string{"**Не забыть сегодня**", "**Просрочено**\n1. Проверить формулировки — Петрова М.", "**Сегодня**\n2. Отправить смету",
		"**Мне должны**\n3. Прислать отчёт", "[источник](https://mail.google.com/mail/#all/abc)", "«1 и 2 сделал»"} {
		if !strings.Contains(m.Text, want) {
			t.Fatalf("the text lacks %q:\n%s", want, m.Text)
		}
	}
	if strings.Contains(m.Text, "Без срока") || strings.Contains(m.Text, "Позже") {
		t.Fatalf("the daily digest has what is not for today:\n%s", m.Text)
	}
	var card comm.DigestCard
	if err := json.Unmarshal(m.Card, &card); err != nil || card.Type != "digest" || card.Period != "day" || len(card.Items) != 3 ||
		card.Items[0].Group != comm.GroupOverdue || card.Items[2].Group != comm.GroupTheirs || card.Items[0].Person == nil || card.Items[0].URL == "" {
		t.Fatalf("card: %s %v", m.Card, err)
	}
	// LIFE-07: the administrator switched the function off — nothing runs and nothing fails.
	off := false
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, found := run(); found {
		t.Fatal("a run of a switched-off function is kept")
	}
	if cur, _ := ts.Get(ctx, uid, *mt.TaskID); cur.Failures != 0 || cur.Status != "active" {
		t.Fatalf("%+v", cur)
	}
}

// ISO-04, ISO-07, CMT-05, VIEW-08 (the tool part): the tools of the personal agent.
func TestCommTools(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	a, b := newUser(t, "tool-a@x.org"), newUser(t, "tool-b@x.org")
	saveNote(t, s, a, "t1", "Сроки выпуска", "Согласовали с Ивановым перенос выпуска на 24 октября.", []comm.PersonIn{{Email: "ivanov@x.org", Name: "Иванов А."}}, []string{"Релиз"},
		comm.Message{ID: "m1", URL: "https://mail/1"})
	cid, err := s.SaveCommitment(ctx, a, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "t1", Text: "Прислать отчёт", Direction: comm.Mine, PersonEmail: "ivanov@x.org", BasisMessageID: "m1"})
	if err != nil {
		t.Fatal(err)
	}
	tools := map[string]mcp.Tool{}
	for _, tl := range s.Tools(func(id uuid.UUID) string { return "https://nabu/memory?note=" + id.String() }, nil) {
		tools[tl.Name] = tl
	}
	ga, gb := mcp.Grant{UserID: a}, mcp.Grant{UserID: b}
	// ISO-07: offered only to a user who consented — a group agent works as a
	// technical user that never consents. R24: they stay with the user after the
	// administrator switched the function off.
	off, on := false, true
	_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &on})
	defer s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off}) //nolint:errcheck
	if tools["comm_search"].Available(ctx, ga) || tools["commitment_update"].Available(ctx, gb) {
		t.Fatal("offered without the consent")
	}
	_ = s.Consent(ctx, a)
	if !tools["comm_search"].Available(ctx, ga) || tools["comm_search"].Available(ctx, gb) || tools["commitment_update"].Available(ctx, gb) {
		t.Fatal("offered by the consent of another user")
	}
	_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off})
	if !tools["comm_search"].Available(ctx, ga) || tools["comm_search"].Available(ctx, gb) {
		t.Fatal("R24: the agent lost what is collected when the function was switched off")
	}
	// the server hides and refuses a tool that is not offered
	srv := mcp.NewServer(nil)
	srv.Register(tools["comm_search"])
	if out, isErr, err := srv.Execute(ctx, gb, "comm_search", []byte(`{"query":"выпуск"}`)); err == nil || !isErr {
		t.Fatalf("a tool that is not offered ran: %q", out)
	}

	out, err := tools["comm_search"].Handler(ctx, ga, []byte(`{"query":"перенос выпуска","person":"иванов"}`))
	if err != nil || !strings.Contains(out, "24 октября") || !strings.Contains(out, "https://mail/1") || !strings.Contains(out, "https://nabu/memory?note=") ||
		!strings.Contains(out, "Open commitment: Прислать отчёт") {
		t.Fatalf("%q %v", out, err)
	}
	// ISO-04: whatever the arguments, the search of B never returns a record of A.
	for _, args := range []string{`{"query":"перенос выпуска"}`, `{}`, `{"query":"24 октября","source":"gmail"}`} {
		if out, err := tools["comm_search"].Handler(ctx, gb, []byte(args)); err != nil || out != "No notes found." {
			t.Fatalf("B searched with %s: %q %v", args, out, err)
		}
	}
	if _, err := tools["comm_search"].Handler(ctx, gb, []byte(`{"person":"иванов"}`)); err == nil {
		t.Fatal("B found a person of A")
	}
	if out, _ := tools["commitment_list"].Handler(ctx, gb, []byte(`{}`)); out != "No commitments." {
		t.Fatalf("B listed: %q", out)
	}
	if _, err := tools["commitment_update"].Handler(ctx, gb, []byte(fmt.Sprintf(`{"id":%q,"state":"done"}`, cid))); err == nil {
		t.Fatal("B closed a commitment of A")
	}
	// CMT-05: the agent closes what the user said is done.
	if out, _ := tools["commitment_list"].Handler(ctx, ga, []byte(`{}`)); !strings.Contains(out, cid.String()) {
		t.Fatalf("%q", out)
	}
	out, err = tools["commitment_update"].Handler(ctx, ga, []byte(fmt.Sprintf(`{"id":%q,"state":"done"}`, cid)))
	if err != nil || !strings.Contains(out, "Прислать отчёт") || !strings.Contains(out, "done") {
		t.Fatalf("%q %v", out, err)
	}
	l, _ := s.Commitments(ctx, a, comm.CommitmentFilter{State: "closed"}, 0)
	if len(l) != 1 || *l[0].ClosedBy != comm.ByAgent {
		t.Fatalf("%+v", l)
	}
}

// LIFE-03, LIFE-04, LIFE-05
func TestCommRetention(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "ret@x.org")
	old := time.Now().AddDate(0, 0, -200)
	fresh := time.Now().AddDate(0, 0, -10)
	saveNote(t, s, u, "old", "Старая", "Давняя переписка.", []comm.PersonIn{{Email: "old@x.org"}}, []string{"Архив"}, comm.Message{ID: "1", SentAt: &old})
	kept := saveNote(t, s, u, "old-open", "Старая с долгом", "Давняя, но с открытой договорённостью.", []comm.PersonIn{{Email: "debt@x.org"}}, []string{"Долги"}, comm.Message{ID: "1", SentAt: &old})
	saveNote(t, s, u, "new", "Свежая", "Недавняя переписка.", []comm.PersonIn{{Email: "new@x.org"}}, []string{"Текущее"}, comm.Message{ID: "1", SentAt: &fresh})
	cid, err := s.SaveCommitment(ctx, u, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "old-open", Text: "Вернуть долг", Direction: comm.Mine, PersonEmail: "debt@x.org"})
	if err != nil {
		t.Fatal(err)
	}
	titles := func() string {
		l, err := s.List(ctx, u, comm.Filter{}, httpx.Page{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, n := range l.Items {
			out = append(out, n.Title)
		}
		return strings.Join(out, ",")
	}
	// LIFE-03: an old note without open commitments goes with its tags; LIFE-04: an open commitment keeps its note.
	p, err := s.Purge(ctx)
	if err != nil || p.Notes != 1 || titles() != "Свежая,Старая с долгом" {
		t.Fatalf("%+v %v %s", p, err, titles())
	}
	if people, _ := s.Tags(ctx, u, comm.KindPerson, "", 0); len(people) != 2 {
		t.Fatalf("people after the purge: %+v", people)
	}
	if topics, _ := s.Tags(ctx, u, comm.KindTopic, "", 0); len(topics) != 2 {
		t.Fatalf("topics after the purge: %+v", topics)
	}
	if st, _ := s.State(ctx, u); st.Notes != 2 {
		t.Fatalf("notes counter %d", st.Notes)
	}
	done := comm.Done
	if _, err := s.UpdateCommitment(ctx, u, cid, comm.CommitmentPatch{State: &done}, comm.ByUser); err != nil {
		t.Fatal(err)
	}
	if p, _ = s.Purge(ctx); p.Notes != 1 || titles() != "Свежая" {
		t.Fatalf("after closing the commitment: %+v %s", p, titles())
	}
	if _, err := s.Get(ctx, u, kept.ID); !isCode(err, "not_found") {
		t.Fatal("the note is still there")
	}
	// LIFE-05: a shorter retention applies at the next purge.
	days := 5
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{RetentionDays: &days}); err != nil {
		t.Fatal(err)
	}
	// the purge is global: with five days it also takes the old notes of other tests
	if p, _ = s.Purge(ctx); p.Notes < 1 || titles() != "" {
		t.Fatalf("after shortening the retention: %+v %s", p, titles())
	}
	def := comm.DefaultRetentionDays
	_, _ = s.SaveSettings(ctx, comm.SettingsPatch{RetentionDays: &def})
}

// fakeEmbeddings is an OpenAI-compatible /embeddings: a text becomes a vector
// of the concepts it mentions, so texts of one meaning in different words are
// close. dims changes with the "model".
type fakeEmbeddings struct {
	mu    sync.Mutex
	dims  int
	fail  bool
	calls int
}

var concepts = [][]string{
	{"релиз", "выпуск", "версии", "поставк"},
	{"бюджет", "смет", "расход", "деньг"},
	{"найм", "собеседован", "кандидат", "вакан"},
	{"сбой", "инцидент", "авари", "упал"},
}

func (f *fakeEmbeddings) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail || !strings.HasSuffix(r.URL.Path, "/embeddings") {
		http.Error(w, "down", http.StatusInternalServerError)
		return
	}
	var in struct {
		Input []string `json:"input"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	type item struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	}
	var out struct {
		Data []item `json:"data"`
	}
	for i, text := range in.Input {
		v := make([]float32, f.dims)
		v[f.dims-1] = 0.05 // never a zero vector
		low := strings.ToLower(text)
		for c, words := range concepts {
			for _, wd := range words {
				if strings.Contains(low, wd) {
					v[c] = 1
				}
			}
		}
		out.Data = append(out.Data, item{Index: i, Embedding: v})
	}
	_ = json.NewEncoder(w).Encode(out)
}

// VIEW-08, VIEW-09, VIEW-10, VIEW-11, ISO-04 (the search by meaning)
func TestCommSemanticSearch(t *testing.T) {
	ctx := context.Background()
	if !(&comm.Service{Pool: pool}).EnsureVectors(ctx) { // the owner creates the extension and the table
		t.Skip("the vector extension is not installed in this Postgres")
	}
	s := commStore(t)
	// the role of the application was granted the tables before this one existed
	if _, err := pool.Exec(ctx, `GRANT ALL ON comm_note_vectors TO PUBLIC`); err != nil {
		t.Fatal(err)
	}
	if !s.EnsureVectors(ctx) {
		t.Fatal("the application does not see the table of vectors")
	}
	emb := &fakeEmbeddings{dims: 8}
	srv := httptest.NewServer(emb)
	defer srv.Close()
	model := "emb-1"
	s.Embedder = func(context.Context) *comm.Embedder {
		if model == "" {
			return nil
		}
		return &comm.Embedder{BaseURL: srv.URL + "/v1", Key: "k", Model: model}
	}
	a, b := newUser(t, "sem-a@x.org"), newUser(t, "sem-b@x.org")
	ivanov := []comm.PersonIn{{Email: "ivanov@x.org", Name: "Иванов А."}}
	saveNote(t, s, a, "s1", "Сроки поставки", "Согласовали перенос выпуска на 24 октября, договор 4500123456.", ivanov, []string{"Проект А"}, comm.Message{ID: "1", SentAt: at(1, 9)})
	saveNote(t, s, a, "s2", "Смета на квартал", "Обсудили расходы на подрядчиков.", ivanov, []string{"Проект Б"}, comm.Message{ID: "1", SentAt: at(2, 9)})
	saveNote(t, s, a, "s3", "Собеседование", "Кандидат на бэкенд придёт в среду.", nil, []string{"Команда"}, comm.Message{ID: "1", SentAt: at(3, 9)})
	saveNote(t, s, b, "s1", "Сроки поставки", "Перенос выпуска у другого пользователя.", ivanov, []string{"Проект А"}, comm.Message{ID: "1", SentAt: at(1, 9)})
	if !s.Semantic(ctx) {
		t.Fatal("the search by meaning is reported off")
	}
	titles := func(uid uuid.UUID, f comm.Filter) string {
		t.Helper()
		l, err := s.Search(ctx, uid, f, 10)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, n := range l {
			out = append(out, n.Title)
		}
		return strings.Join(out, ",")
	}
	// before the vectors are computed the search is by words: «релиз» is in no note
	if got := titles(a, comm.Filter{Query: "релиз"}); got != "" {
		t.Fatalf("before embedding: %q", got)
	}
	n, err := s.EmbedPending(ctx, 64)
	if err != nil || n < 4 {
		t.Fatalf("embedded %d, %v", n, err)
	}
	if n, _ = s.EmbedPending(ctx, 64); n != 0 {
		t.Fatalf("embedded again: %d", n)
	}
	// VIEW-08: found by meaning, with no word in common; the closest first.
	if got := titles(a, comm.Filter{Query: "о чём договорились по релизу"}); !strings.HasPrefix(got, "Сроки поставки") {
		t.Fatalf("by meaning: %q", got)
	}
	if got := titles(a, comm.Filter{Query: "деньги на проект"}); !strings.HasPrefix(got, "Смета на квартал") {
		t.Fatalf("by meaning: %q", got)
	}
	// VIEW-09: an exact number is found by words and comes first.
	if got := titles(a, comm.Filter{Query: "4500123456"}); !strings.HasPrefix(got, "Сроки поставки") {
		t.Fatalf("by a number: %q", got)
	}
	// the filters apply to both branches
	from := at(2, 0)
	if got := titles(a, comm.Filter{Query: "релиз", From: from}); strings.Contains(got, "Сроки поставки") {
		t.Fatalf("the period filter is ignored by the vector branch: %q", got)
	}
	// ISO-04: the vectors of another user are never read.
	if got := titles(b, comm.Filter{Query: "релиз"}); got != "Сроки поставки" {
		t.Fatalf("user B: %q", got)
	}
	l, _ := s.Search(ctx, b, comm.Filter{Query: "смета расходы собеседование"}, 10)
	for _, n := range l {
		if n.Excerpt != "Перенос выпуска у другого пользователя." {
			t.Fatalf("B got a note of A: %+v", n)
		}
	}
	// an edited note is embedded again
	text := "Теперь это про инцидент: сервис упал."
	notes, _ := s.Search(ctx, a, comm.Filter{Query: "Собеседование"}, 1)
	if _, err := s.Update(ctx, a, notes[0].ID, comm.NotePatch{Summary: &text}); err != nil {
		t.Fatal(err)
	}
	if n, _ = s.EmbedPending(ctx, 64); n != 1 {
		t.Fatalf("an edited note: embedded %d", n)
	}
	if got := titles(a, comm.Filter{Query: "авария"}); !strings.HasPrefix(got, "Собеседование") {
		t.Fatalf("after the edit: %q", got)
	}
	// VIEW-10: the model does not answer — the search goes by words, without an error.
	emb.mu.Lock()
	emb.fail = true
	emb.mu.Unlock()
	if got := titles(a, comm.Filter{Query: "релиз"}); got != "" {
		t.Fatalf("with the model down: %q", got)
	}
	if got := titles(a, comm.Filter{Query: "Смета"}); got != "Смета на квартал" {
		t.Fatalf("by words with the model down: %q", got)
	}
	emb.mu.Lock()
	emb.fail = false
	emb.mu.Unlock()
	// … and with no model chosen at all.
	model = ""
	if s.Semantic(ctx) {
		t.Fatal("semantic without a model")
	}
	if got := titles(a, comm.Filter{Query: "Смета"}); got != "Смета на квартал" {
		t.Fatalf("without a model: %q", got)
	}
	// VIEW-11: another model with another dimension — the old vectors are not compared, everything is recounted.
	model = "emb-2"
	emb.mu.Lock()
	emb.dims = 12
	emb.mu.Unlock()
	if got := titles(a, comm.Filter{Query: "релиз"}); got != "" { // no vector of the new model yet, no error
		t.Fatalf("right after the change of the model: %q", got)
	}
	if n, err = s.EmbedPending(ctx, 64); err != nil || n < 4 {
		t.Fatalf("recount: %d, %v", n, err)
	}
	if got := titles(a, comm.Filter{Query: "релиз"}); !strings.HasPrefix(got, "Сроки поставки") {
		t.Fatalf("after the recount: %q", got)
	}
	if p, _ := s.PendingVectors(ctx); p != 0 {
		t.Fatalf("pending after the recount: %d", p)
	}
	// the vectors go with the note
	if err := s.DeleteAll(ctx, a, "all"); err != nil {
		t.Fatal(err)
	}
	var left int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM comm_note_vectors WHERE user_id = $1`, a).Scan(&left)
	if left != 0 {
		t.Fatalf("vectors of deleted notes: %d", left)
	}
}

// commRouter serves the API of a user and of the administration as the application does.
func commRouter(s *comm.Service, ts *tasks.Service, uid uuid.UUID) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(httpx.WithPrincipal(req.Context(), &domain.Principal{UserID: uid})))
		})
	})
	api := &comm.API{Store: s, Collect: func(ctx context.Context, uid uuid.UUID) (comm.CollectState, error) {
		st := comm.CollectState{Status: "off"}
		var err error
		st.BudgetUsedPct, err = s.BudgetUsedPct(ctx, uid)
		return st, err
	}}
	r.Route("/api/v1", func(r chi.Router) {
		api.Routes(r)
		ts.MemoryRoutes(r, func(ctx context.Context) error { _, err := s.Allowed(ctx); return err })
	})
	r.Route("/admin/api/v1", s.AdminRoutes)
	return r
}

// NOTE-01, TASK-08, LIFE-01, LIFE-06, LIFE-07 (R24), ISO-08, ADM-03
func TestCommAPI(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	uid := newUser(t, "api-"+uuid.NewString()[:6]+"@x.org")
	ts := &tasks.Service{Pool: pool, Bus: &bus{}, Rules: tasks.Rules{MinInterval: time.Minute, MaxActive: 5}, Catchup: time.Hour, Tick: time.Second, MaxFailures: 3, DefaultTimezone: "UTC"}
	srv := httptest.NewServer(commRouter(s, ts, uid))
	defer srv.Close()
	call := func(method, path, body string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	on, off := true, false
	limit := 0.10
	_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off, MonthlyLimitUSD: &limit})
	t.Cleanup(func() {
		zero := 0.0
		_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off, MonthlyLimitUSD: &zero})
	})

	// LIFE-01: not allowed — the state says so, everything else is refused.
	if code, body := call("GET", "/api/v1/comm/state", ""); code != 200 || !strings.Contains(body, `"allowed":false`) || !strings.Contains(body, `"available":false`) {
		t.Fatalf("state: %d %s", code, body)
	}
	for _, p := range []string{"/api/v1/comm/notes", "/api/v1/comm/graph", "/api/v1/commitments", "/api/v1/memory-tasks", "/api/v1/comm/people"} {
		if code, body := call("GET", p, ""); code != 403 || !strings.Contains(body, "comm_disabled") {
			t.Fatalf("%s while not allowed: %d %s", p, code, body)
		}
	}
	if code, _ := call("PUT", "/api/v1/comm/consent", `{"accepted":true}`); code != 403 {
		t.Fatalf("consent while not allowed: %d", code)
	}

	_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &on})
	if code, body := call("PUT", "/api/v1/comm/consent", `{"accepted":false}`); code != 422 || !strings.Contains(body, "consent_required") {
		t.Fatalf("consent not accepted: %d %s", code, body)
	}
	if code, _ := call("PUT", "/api/v1/comm/consent", `{"accepted":true}`); code != 204 {
		t.Fatalf("consent: %d", code)
	}

	// NOTE-01: what is collected does not get into the instructions of the agent — the facts are as they were.
	facts := &memory.Service{Pool: pool}
	if _, err := facts.Add(ctx, uid, "Prefers short answers", "user", false, nil); err != nil {
		t.Fatal(err)
	}
	before, _ := facts.ForInstructions(ctx, uid)
	secret := "Согласовали перенос выпуска на двадцать четвёртое"
	for i := 0; i < 40; i++ {
		saveNote(t, s, uid, fmt.Sprintf("api-%d", i), fmt.Sprintf("Заметка %d", i), secret, []comm.PersonIn{{Email: "ivanov@x.org"}}, []string{"Релиз"}, comm.Message{ID: "1"})
	}
	after, _ := facts.ForInstructions(ctx, uid)
	if len(after) != 1 || memory.Hash(before) != memory.Hash(after) {
		t.Fatalf("the instructions changed after the collection: %d records", len(after))
	}
	if _, err := s.SaveCommitment(ctx, uid, comm.SaveCommitment{ItemID: commItem, Source: "gmail", ThreadKey: "api-0", Text: "Прислать секретный план", Direction: comm.Mine, PersonEmail: "ivanov@x.org"}); err != nil {
		t.Fatal(err)
	}

	// ADM-03: the user sees the share of the monthly limit that is spent.
	_ = s.AddUsage(ctx, uid, 0.04)
	if code, body := call("GET", "/api/v1/comm/state", ""); code != 200 || !strings.Contains(body, `"budgetUsedPct":40`) || !strings.Contains(body, `"notes":40`) {
		t.Fatalf("state: %d %s", code, body)
	}

	// ISO-08: no route of the administration returns what is collected.
	for _, p := range []string{"/admin/api/v1/settings/comm", "/admin/api/v1/comm/sources", "/admin/api/v1/comm/usage"} {
		code, body := call("GET", p, "")
		if code != 200 || strings.Contains(body, "перенос выпуска") || strings.Contains(body, "секретный план") || strings.Contains(body, "Заметка 1") || strings.Contains(body, "ivanov@x.org") {
			t.Fatalf("%s: %d %s", p, code, body)
		}
	}
	if _, body := call("GET", "/admin/api/v1/comm/usage", ""); !strings.Contains(body, `"notes":40`) || !strings.Contains(body, `"costUsd":0.04`) {
		t.Fatalf("usage: %s", body)
	}
	for _, p := range []string{"/admin/api/v1/comm/notes", "/admin/api/v1/commitments", "/admin/api/v1/comm/graph", "/admin/api/v1/comm/people", "/admin/api/v1/comm/topics"} {
		if code, _ := call("GET", p, ""); code != 404 && code != 405 {
			t.Fatalf("the administration has a route to the content: %s → %d", p, code)
		}
	}

	// TASK-08: a memory task is not cancelled with the tool of the agent.
	yes := true
	day, err := ts.SetMemoryTask(ctx, uid, "digest_day", tasks.MemoryPatch{Enabled: &yes})
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range ts.Tools() {
		if tool.Name != "task_cancel" {
			continue
		}
		if out, err := tool.Handler(ctx, mcp.Grant{UserID: uid}, []byte(fmt.Sprintf(`{"id":%q}`, *day.TaskID))); err == nil || !strings.Contains(err.Error(), "Memory") {
			t.Fatalf("task_cancel of a memory task: %q %v", out, err)
		}
	}
	if cur, _ := ts.Get(ctx, uid, *day.TaskID); cur.Status != "active" {
		t.Fatalf("the memory task was cancelled by the tool: %+v", cur)
	}
	// an exclusion the source cannot apply is refused, not stored (R4)
	if code, body := call("PATCH", "/api/v1/memory-tasks/collect", `{"settings":{"exclusions":{"senders":["bad value) OR in:anywhere"]}}}`); code != 422 || !strings.Contains(body, "invalid_exclusion") {
		t.Fatalf("an exclusion with spaces: %d %s", code, body)
	}

	// LIFE-06, LIFE-07, R24: the function is switched off — what is collected stays readable and can be deleted;
	// nothing new can be switched on.
	_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off})
	if code, body := call("GET", "/api/v1/comm/state", ""); code != 200 || !strings.Contains(body, `"allowed":false`) || !strings.Contains(body, `"available":true`) || !strings.Contains(body, `"notes":40`) {
		t.Fatalf("state after switching off: %d %s", code, body)
	}
	if code, body := call("GET", "/api/v1/comm/notes?limit=5", ""); code != 200 || !strings.Contains(body, "Заметка") {
		t.Fatalf("notes after switching off: %d %s", code, body)
	}
	if code, _ := call("GET", "/api/v1/commitments", ""); code != 200 {
		t.Fatalf("commitments after switching off: %d", code)
	}
	if code, body := call("PATCH", "/api/v1/memory-tasks/digest_week", `{"enabled":true}`); code != 403 || !strings.Contains(body, "comm_disabled") {
		t.Fatalf("a task switched on while the function is off: %d %s", code, body)
	}
	other := newUser(t, "api-other-"+uuid.NewString()[:6]+"@x.org")
	osrv := httptest.NewServer(commRouter(s, ts, other))
	defer osrv.Close()
	if resp, err := http.Get(osrv.URL + "/api/v1/comm/notes"); err != nil || resp.StatusCode != 403 { // a user without the consent has nothing to see
		t.Fatalf("a user without the consent while the function is off: %v %v", resp, err)
	}
	if code, _ := call("DELETE", "/api/v1/comm", `{"confirm":"all"}`); code != 204 {
		t.Fatalf("deleting everything while the function is off: %d", code)
	}
	if code, body := call("GET", "/api/v1/comm/state", ""); code != 200 || !strings.Contains(body, `"available":false`) {
		t.Fatalf("state after deleting everything: %d %s", code, body)
	}
}

// NOTE-17, NOTE-18
func TestCommTopicFind(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "topics@x.org")
	saveNote(t, s, u, "a", "Стратегия", "s", nil, []string{"ИТ Стратегия"}, comm.Message{ID: "a1"})
	saveNote(t, s, u, "b", "StarLine", "s", nil, []string{"Миграция StarLine"}, comm.Message{ID: "b1"})
	// NOTE-17: the key subject is looked for among all topics; the same subject is suggested, a new one is not,
	// a one-word name inside a longer topic is only shown.
	found, err := s.FindTopics(ctx, u, []string{"стратегия ИТ 2027", "Проект Альфа", "StarLine"})
	if err != nil || len(found) != 3 {
		t.Fatalf("%+v %v", found, err)
	}
	if found[0].Suggestion == nil || *found[0].Suggestion != "ИТ Стратегия" {
		t.Fatalf("strategy: %+v", found[0])
	}
	if found[1].Suggestion != nil || len(found[1].Matches) != 0 {
		t.Fatalf("a new subject matched: %+v", found[1])
	}
	if found[2].Suggestion != nil || len(found[2].Matches) != 1 || found[2].Matches[0].Name != "Миграция StarLine" {
		t.Fatalf("starline: %+v", found[2])
	}
	// … among all of them, not only the most used: 600 topics, the one asked for is used once.
	if _, err := pool.Exec(ctx, `INSERT INTO comm_topics (user_id, name, name_key) SELECT $1, 'Тема '||i, 'тема '||i FROM generate_series(1, 600) i`, u); err != nil {
		t.Fatal(err)
	}
	if found, err := s.FindTopics(ctx, u, []string{"Тема 599"}); err != nil || found[0].Suggestion == nil || *found[0].Suggestion != "Тема 599" {
		t.Fatalf("a rare topic: %+v %v", found, err)
	}
	// NOTE-18: a name that differs only by case, punctuation or ё becomes the existing topic — from the collector …
	res := saveNote(t, s, u, "c", "Стратегия", "s", nil, []string{"ИТ-стратегия"}, comm.Message{ID: "c1"})
	if n, _ := s.Get(ctx, u, res.ID); len(n.Topics) != 1 || n.Topics[0].Label != "ИТ Стратегия" {
		t.Fatalf("topics: %+v", n.Topics)
	}
	// … and from the user.
	other := saveNote(t, s, u, "d", "Другое", "s", nil, nil, comm.Message{ID: "d1"})
	if n, err := s.Update(ctx, u, other.ID, comm.NotePatch{AddTags: []comm.TagRef{{Kind: comm.KindTopic, Name: "ит стратегия!"}}}); err != nil ||
		len(n.Topics) != 1 || n.Topics[0].Label != "ИТ Стратегия" {
		t.Fatalf("a user's tag: %+v %v", n, err)
	}
	var starline uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM comm_topics WHERE user_id = $1 AND name = 'Миграция StarLine'`, u).Scan(&starline); err != nil {
		t.Fatal(err)
	}
	if err := s.RenameTopic(ctx, u, starline, "ИТ-Стратегия"); !isCode(err, "topic_exists") {
		t.Fatalf("a rename into another topic: %v", err)
	}
	var dups int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM comm_topics WHERE user_id = $1 AND name NOT LIKE 'Тема %'`, u).Scan(&dups); err != nil || dups != 2 {
		t.Fatalf("topics: %d %v", dups, err)
	}
	// Topics stored before the rule that share a loose key: the most used one is taken, every time.
	if _, err := pool.Exec(ctx, `INSERT INTO comm_topics (user_id, name, name_key) VALUES ($1, 'ИТ.Стратегия', 'ит.стратегия')`, u); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		res := saveNote(t, s, u, fmt.Sprintf("e%d", i), "Стратегия", "s", nil, []string{"ИТ_стратегия"}, comm.Message{ID: fmt.Sprintf("e%d", i)})
		if n, _ := s.Get(ctx, u, res.ID); len(n.Topics) != 1 || n.Topics[0].Label != "ИТ Стратегия" {
			t.Fatalf("legacy duplicates: %+v", n.Topics)
		}
	}
}
