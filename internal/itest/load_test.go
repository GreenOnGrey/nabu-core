//go:build integration && load

package itest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func p95(d []time.Duration) time.Duration {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[len(d)*95/100]
}

// PERF-01, PERF-02, PERF-05 (FTR.NAB.CMN-0003 R28, qa §11): the table, the
// search and the graph of one user stay fast when the instance holds the
// notes of many users.
//
//	NABU_LOAD_USERS=1000 NABU_LOAD_NOTES=10000 NABU_LOAD_DIMS=1024 \
//	  go test -tags 'integration load' -count=1 -timeout 120m -run TestCommLoad ./internal/itest/
//
// The defaults (50 users × 10 000 notes, vectors of 256) fit a laptop; the
// scale of R28 is 1000 users and vectors of 1024.
func TestCommLoad(t *testing.T) {
	ctx := context.Background()
	users, perUser, dims := envInt("NABU_LOAD_USERS", 50), envInt("NABU_LOAD_NOTES", 10000), envInt("NABU_LOAD_DIMS", 256)
	vectors := (&comm.Service{Pool: pool}).EnsureVectors(ctx)
	s := commStore(t)
	if vectors {
		if _, err := pool.Exec(ctx, `GRANT ALL ON comm_note_vectors TO PUBLIC`); err != nil {
			t.Fatal(err)
		}
		s.EnsureVectors(ctx)
	}
	words := "релиз выпуск перенос сроки смета бюджет расходы договор правки юристы кандидат собеседование найм миграция скрипт стенд сбой инцидент сертификат отчёт план оценка заказчик подрядчик согласование встреча итоги доступ интеграция тесты"
	start := time.Now()
	ids := make([]uuid.UUID, users)
	for i := range ids {
		ids[i] = newUser(t, fmt.Sprintf("load-%d-%s@x.org", i, uuid.NewString()[:6]))
	}
	for i, uid := range ids {
		err := postgres.InTx(ctx, pool, func(tx pgx.Tx) error {
			for _, q := range []string{
				`INSERT INTO comm_people (user_id, email, name) SELECT $1, 'p' || g || '@x.org', 'Человек ' || g FROM generate_series(1, 60) g`,
				`INSERT INTO comm_topics (user_id, name, name_key) SELECT $1, 'Тема ' || g, 'тема ' || g FROM generate_series(1, 40) g`,
			} {
				if _, err := tx.Exec(ctx, q, uid); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `INSERT INTO comm_notes (user_id, item_id, source, thread_key, title, summary, first_at, last_at, message_count)
				SELECT $1, $2, 'gmail', 't' || g, 'Заметка ' || g,
					(SELECT string_agg(w.arr[1 + (floor(random() * array_length(w.arr, 1))::int + 0 * x)], ' ') FROM generate_series(1, 40) x WHERE g > 0),
					now() - g * interval '25 minutes', now() - g * interval '25 minutes', 3
				FROM generate_series(1, $3) g, (SELECT string_to_array($4, ' ') AS arr) w`, uid, commItem, perUser, words); err != nil {
				return err
			}
			// two people and two topics per note
			if _, err := tx.Exec(ctx, `INSERT INTO comm_note_tags (user_id, note_id, kind, tag_id)
				SELECT n.user_id, n.id, 'person', p.id FROM comm_notes n
				JOIN LATERAL (SELECT id FROM comm_people WHERE user_id = n.user_id ORDER BY md5(id::text || n.id::text) LIMIT 2) p ON true WHERE n.user_id = $1
				UNION ALL
				SELECT n.user_id, n.id, 'topic', tp.id FROM comm_notes n
				JOIN LATERAL (SELECT id FROM comm_topics WHERE user_id = n.user_id ORDER BY md5(id::text || n.id::text) LIMIT 2) tp ON true WHERE n.user_id = $1`, uid); err != nil {
				return err
			}
			if vectors {
				if _, err := tx.Exec(ctx, `INSERT INTO comm_note_vectors (user_id, note_id, model, embedding)
					SELECT n.user_id, n.id, 'load', (SELECT array_agg(random())::real[] FROM generate_series(1, $2) x WHERE n.id IS NOT NULL)::halfvec
					FROM comm_notes n WHERE n.user_id = $1`, uid, dims); err != nil {
					return err
				}
			}
			_, err := tx.Exec(ctx, `INSERT INTO comm_settings (user_id, consent_at, notes_count) VALUES ($1, now(), $2)
				ON CONFLICT (user_id) DO UPDATE SET notes_count = EXCLUDED.notes_count`, uid, perUser)
			return err
		})
		if err != nil {
			t.Fatalf("seed user %d: %v", i, err)
		}
	}
	if _, err := pool.Exec(ctx, `ANALYZE comm_notes; ANALYZE comm_note_tags`); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d users × %d notes (vectors: %v, %d dims) in %s", users, perUser, vectors, dims, time.Since(start).Round(time.Second))

	if vectors {
		// the vector of a query comes from a model; here — a constant one
		emb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			v := make([]string, dims)
			for i := range v {
				v[i] = "0.5"
			}
			fmt.Fprintf(w, `{"data":[{"index":0,"embedding":[%s]}]}`, strings.Join(v, ","))
		}))
		defer emb.Close()
		s.Embedder = func(context.Context) *comm.Embedder { return &comm.Embedder{BaseURL: emb.URL, Model: "load"} }
	}
	uid := ids[len(ids)/2]
	topics, _ := s.Tags(ctx, uid, comm.KindTopic, "", 5)
	people, _ := s.Tags(ctx, uid, comm.KindPerson, "", 5)
	month := time.Now().AddDate(0, 0, -30)
	measure := func(name string, limit time.Duration, fn func(i int) error) {
		t.Helper()
		var d []time.Duration
		for i := 0; i < 40; i++ {
			at := time.Now()
			if err := fn(i); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			d = append(d, time.Since(at))
		}
		got := p95(d)
		t.Logf("%-28s p95 %s", name, got.Round(time.Millisecond))
		if got > limit {
			t.Errorf("%s: p95 %s is over %s", name, got, limit)
		}
	}
	queries := strings.Fields("релиз смета кандидат сбой договор")
	// PERF-01: the table and the search — 2 seconds.
	measure("table, 30 days", 2*time.Second, func(int) error {
		_, err := s.List(ctx, uid, comm.Filter{From: &month}, httpx.Page{Limit: 100})
		return err
	})
	measure("table, person and topic", 2*time.Second, func(i int) error {
		_, err := s.List(ctx, uid, comm.Filter{Person: &people[i%len(people)].ID, Topic: &topics[i%len(topics)].ID}, httpx.Page{Limit: 100})
		return err
	})
	measure("search", 2*time.Second, func(i int) error {
		_, err := s.Search(ctx, uid, comm.Filter{Query: queries[i%len(queries)]}, 20)
		return err
	})
	measure("search with a topic", 2*time.Second, func(i int) error {
		_, err := s.Search(ctx, uid, comm.Filter{Query: queries[i%len(queries)], Topic: &topics[i%len(topics)].ID}, 20)
		return err
	})
	// PERF-02: the graph of a period — 3 seconds.
	measure("graph, 30 days", 3*time.Second, func(int) error {
		_, err := s.Graph(ctx, uid, comm.Filter{From: &month}, 0, 150)
		return err
	})
	measure("graph, all the notes", 3*time.Second, func(int) error {
		_, err := s.Graph(ctx, uid, comm.Filter{}, 0, 150)
		return err
	})

	// PERF-05: a query of one user reads one partition and no table whole.
	var plan strings.Builder
	rows, err := pool.Query(ctx, `EXPLAIN SELECT id FROM comm_notes n WHERE n.user_id = $1 AND n.last_at >= $2 ORDER BY n.last_at DESC, n.id DESC LIMIT 100`, uid, month)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		_ = rows.Scan(&line)
		plan.WriteString(line + "\n")
	}
	rows.Close()
	parts := map[string]bool{}
	for _, m := range regexp.MustCompile(`comm_notes_p\d\d`).FindAllString(plan.String(), -1) {
		parts[m] = true
	}
	if len(parts) != 1 || strings.Contains(plan.String(), "Seq Scan") {
		t.Errorf("the plan reads %d partitions or scans a table:\n%s", len(parts), plan.String())
	}
}
