//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// FTR.NAB.MEM-0001: the levels and the periods of the records, the days of communication, the graph by day, the
// search of tags and the usage of the administration.

func noteByTitle(t *testing.T, s *comm.Service, uid uuid.UUID, f comm.Filter, title string) *comm.Note {
	t.Helper()
	l, err := s.List(context.Background(), uid, f, httpx.Page{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range l.Items {
		if n.Title == title {
			return &n
		}
	}
	return nil
}

func graphOf(t *testing.T, s *comm.Service, uid uuid.UUID, f comm.Filter) (map[string]comm.Node, func(a, b string) int) {
	t.Helper()
	g, err := s.Graph(context.Background(), uid, f, 0, comm.GraphLimits{MaxNodes: 150, NodeEdges: 8, MaxEdges: 600})
	if err != nil {
		t.Fatal(err)
	}
	nodes := map[string]comm.Node{}
	for _, n := range g.Nodes {
		nodes[n.Label] = n
	}
	edge := func(a, b string) int {
		for _, e := range g.Edges {
			if (e.A == nodes[a].ID && e.B == nodes[b].ID) || (e.A == nodes[b].ID && e.B == nodes[a].ID) {
				return e.Days
			}
		}
		return 0
	}
	return nodes, edge
}

// MEM-VW-05, MEM-GR-08, MEM-GR-09: the days of a record, the graph by day from the aggregate and over the records,
// and the aggregate follows a change of tags and a deletion.
func TestMemDaysAndGraph(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "mem-days@x.org")
	ivanov := comm.PersonIn{Email: "ivanov@x.org", Name: "Иванов"}
	petrova := comm.PersonIn{Email: "petrova@x.org", Name: "Петрова"}
	saveNote(t, s, u, "t1", "Сроки", "Обсуждали сроки.", []comm.PersonIn{ivanov}, []string{"Релиз"},
		comm.Message{ID: "a1", SentAt: at(12, 9)}, comm.Message{ID: "a2", SentAt: at(12, 15)}, comm.Message{ID: "a3", SentAt: at(13, 10)})
	t2 := saveNote(t, s, u, "t2", "Смета", "Обсуждали смету.", []comm.PersonIn{ivanov, petrova}, []string{"Релиз"},
		comm.Message{ID: "b1", SentAt: at(13, 11)})

	n := noteByTitle(t, s, u, comm.Filter{}, "Сроки")
	if n == nil || n.Level != comm.LevelThread || n.Days != 2 || n.PeriodFrom != "2026-10-12" || n.PeriodTo != "2026-10-13" {
		t.Fatalf("the record of a thread: %+v", n)
	}
	nodes, edge := graphOf(t, s, u, comm.Filter{})
	if nodes["Иванов"].Days != 2 || nodes["Петрова"].Days != 1 || nodes["Релиз"].Days != 2 || edge("Иванов", "Релиз") != 2 || edge("Иванов", "Петрова") != 1 {
		t.Fatalf("the graph from the aggregate: %+v", nodes)
	}
	// the same set counted over the records, by a filter of the topic, gives the same days
	live, liveEdge := graphOf(t, s, u, comm.Filter{Topic: ptr(nodes["Релиз"].ID)})
	if live["Иванов"].Days != 2 || live["Петрова"].Days != 1 || liveEdge("Иванов", "Релиз") != 2 {
		t.Fatalf("the graph over the records: %+v", live)
	}
	people, _ := s.Tags(ctx, u, comm.KindPerson, "", 0, nil)
	if len(people) != 2 || people[0].Label != "Иванов" || people[0].Days != 2 {
		t.Fatalf("the days of the people: %+v", people)
	}

	// MEM-GR-09: a tag taken off, then a record deleted: the aggregate follows
	if _, err := s.Update(ctx, u, t2.ID, comm.NotePatch{RemoveTags: []comm.TagRef{{Kind: comm.KindPerson, ID: ptr(nodes["Петрова"].ID)}}}); err != nil {
		t.Fatal(err)
	}
	if nodes, _ = graphOf(t, s, u, comm.Filter{}); nodes["Петрова"].Days != 0 {
		t.Fatalf("a removed tag is still in the graph: %+v", nodes)
	}
	if err := s.Delete(ctx, u, noteByTitle(t, s, u, comm.Filter{}, "Сроки").ID); err != nil {
		t.Fatal(err)
	}
	if nodes, _ = graphOf(t, s, u, comm.Filter{}); nodes["Иванов"].Days != 1 || nodes["Релиз"].Days != 1 {
		t.Fatalf("a deleted record is still in the graph: %+v", nodes)
	}
	// «Delete everything collected» takes the graph by day along
	if err := s.DeleteAll(ctx, u, "all"); err != nil {
		t.Fatal(err)
	}
	if nodes, _ = graphOf(t, s, u, comm.Filter{}); len(nodes) != 0 {
		t.Fatalf("the graph after deleting everything: %+v", nodes)
	}
}

// MEM-VW-03: a record is found by a period that overlaps it, not only by its last message.
func TestMemPeriodOverlap(t *testing.T) {
	s := commStore(t)
	u := newUser(t, "mem-overlap@x.org")
	saveNote(t, s, u, "long", "Долгая цепочка", "Цепочка на две недели.", nil, nil,
		comm.Message{ID: "1", SentAt: at(3, 9)}, comm.Message{ID: "2", SentAt: at(19, 9)})
	saveNote(t, s, u, "early", "Ранняя", "До периода.", nil, nil, comm.Message{ID: "1", SentAt: at(1, 9)})
	from, to := at(10, 0), at(13, 0)
	if noteByTitle(t, s, u, comm.Filter{From: from, To: to}, "Долгая цепочка") == nil {
		t.Fatal("a thread over the period is not found")
	}
	if noteByTitle(t, s, u, comm.Filter{From: from, To: to}, "Ранняя") != nil {
		t.Fatal("a record before the period is found")
	}
}

// MEM-GR-02: suggestions by a part of a name or an email, regardless of case and «ё»; a limit; one by id.
func TestMemTagSearch(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "mem-tags@x.org")
	saveNote(t, s, u, "x", "Ёлка", "Про ёлку.", []comm.PersonIn{{Email: "m.petrova@corp.ru", Name: "Мария Петрова"}, {Email: "i.petrov@corp.ru", Name: "Илья Петров"}},
		[]string{"Ёлка"}, comm.Message{ID: "1", SentAt: at(5, 9)})
	for _, q := range []string{"петр", "ПЕТР", "m.pet"} {
		l, err := s.Tags(ctx, u, comm.KindPerson, q, 8, nil)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, x := range l {
			found = found || x.Label == "Мария Петрова"
		}
		if !found {
			t.Fatalf("%q: %+v", q, l)
		}
	}
	if l, _ := s.Tags(ctx, u, comm.KindTopic, "елк", 8, nil); len(l) != 1 || l[0].Label != "Ёлка" {
		t.Fatalf("a topic with «ё»: %+v", l)
	}
	l, _ := s.Tags(ctx, u, comm.KindPerson, "петр", 1, nil)
	if len(l) != 1 {
		t.Fatalf("the limit: %+v", l)
	}
	if one, _ := s.Tags(ctx, u, comm.KindPerson, "", 0, &l[0].ID); len(one) != 1 || one[0].ID != l[0].ID {
		t.Fatalf("one by id: %+v", one)
	}
}

// MEM-GR-10 (the part of the store): without a link to the thread a record leads to its last message.
func TestMemSourceURL(t *testing.T) {
	s := commStore(t)
	u := newUser(t, "mem-url@x.org")
	saveNote(t, s, u, "chat", "Чат", "Переписка в чате.", nil, nil,
		comm.Message{ID: "1", SentAt: at(5, 9), URL: "https://chat.example/1"}, comm.Message{ID: "2", SentAt: at(5, 11), URL: "https://chat.example/2"})
	if n := noteByTitle(t, s, u, comm.Filter{}, "Чат"); n == nil || n.SourceURL != "https://chat.example/2" {
		t.Fatalf("%+v", n)
	}
}

// MEM-AD-01, MEM-AD-02, MEM-AD-04, MEM-AD-06: the usage of a month and its days, as numbers only.
func TestMemUsage(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	u := newUser(t, "mem-usage-"+uuid.NewString()[:6]+"@x.org")
	saveNote(t, s, u, "secret-thread", "Секретная тема", "Текст, которого администратор не видит.", []comm.PersonIn{{Email: "hidden@x.org", Name: "Скрытый Человек"}},
		[]string{"Тайный проект"}, comm.Message{ID: "1", SentAt: at(5, 9)})
	if err := s.AddUsage(ctx, u, 1.5); err != nil {
		t.Fatal(err)
	}
	if err := s.AddCompactUsage(ctx, u, 0.25, 9000, 2000, 3, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.SnapshotLevels(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	out, err := s.Usage(ctx, first, false)
	if err != nil {
		t.Fatal(err)
	}
	var mine *comm.UsageRow
	for _, x := range out["items"].([]comm.UsageRow) {
		if x.UserID == u {
			mine = &x
		}
	}
	if mine == nil || mine.Levels.Thread != 1 || mine.CostCompact != 0.25 || mine.CostCollect != 1.5 || mine.Runs != 1 {
		t.Fatalf("the row of the user: %+v", mine)
	}
	cost := out["cost"].(map[string]float64)
	if cost["compact"] < 0.25 || out["compaction"].(map[string]any)["ok"].(int) < 3 {
		t.Fatalf("the cards: %+v", out)
	}
	b, _ := json.Marshal(out)
	for _, secret := range []string{"Секретная тема", "Тайный проект", "Скрытый Человек", "hidden@x.org", "администратор не видит"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("the usage shows %q", secret)
		}
	}
	days, err := s.UsageDaily(ctx, first, &u)
	if err != nil {
		t.Fatal(err)
	}
	today := days[now.Day()-1]
	if len(days) < 28 || today.CostCollect != 1.5 || today.CostCompact != 0.25 || today.Levels.Thread != 1 {
		t.Fatalf("the day of the user: %+v", today)
	}
}
