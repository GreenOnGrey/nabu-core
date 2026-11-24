//go:build integration

package itest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/comm"
)

// FTR.NAB.CMN-0003 R6, R20: the days of a record and the graph of the days are refreshed in the transaction of the
// note, by a function that deletes the rows of the day and inserts them again. Two refreshes of one user at the same
// moment both deleted nothing and inserted the same rows: the second failed with a duplicate key (the audit of the
// stand showed comm_tag_days_p04_pkey), and the note of the collection was lost with its transaction.
func TestCommRefreshConcurrent(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	uid := newUser(t, "comm-parallel@x.org")
	// the day of the graph exists: the concurrent calls delete it and build it again
	saveNote(t, s, uid, "parallel-seed", "Выпуск", "Обсудили выпуск.", nil, []string{"Выпуск"},
		comm.Message{ID: "m1", SentAt: at(10, 9), URL: "https://mail.example/1"})

	const calls = 8
	for round := 0; round < 3; round++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, calls)
		for i := range calls {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, errs[i] = pool.Exec(ctx, `SELECT comm_refresh_days($1, ARRAY['2026-10-10'::date])`, uid)
			}()
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d, call %d: %v", round, i, err)
			}
		}
	}

	// the day is built: the tag of the topic stands on it once, not twice
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM comm_tag_days WHERE user_id = $1 AND day = '2026-10-10' AND kind = 'topic'`, uid).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows of the day: %d, want 1", rows)
	}
}

// The same through the service: notes of one day saved at the same moment keep all their rows (R6).
func TestCommRefreshConcurrentNotes(t *testing.T) {
	ctx := context.Background()
	s := commStore(t)
	uid := newUser(t, "comm-parallel-notes@x.org")
	const notes = 4
	sent := at(11, 9)

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, notes)
	for i := range notes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// the same day and the same topic: the rows of the day are the same for every note
			_, errs[i] = s.SaveNote(ctx, uid, comm.SaveNote{ItemID: commItem, Source: "gmail", ThreadKey: fmt.Sprintf("parallel-%d", i),
				Title: "Выпуск", Summary: "Обсудили выпуск.", Topics: []string{"Выпуск"}, UserEmail: "me@x.org",
				Messages: []comm.Message{{ID: "m1", SentAt: sent, URL: "https://mail.example/1"}}})
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("note %d was lost: %v", i, err)
		}
	}
	var kept int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM comm_notes WHERE user_id = $1`, uid).Scan(&kept); err != nil {
		t.Fatal(err)
	}
	if kept != notes {
		t.Fatalf("notes kept: %d, want %d", kept, notes)
	}
}
