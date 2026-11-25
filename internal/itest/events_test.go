//go:build integration

package itest

import (
	"context"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
)

// CONV-13: the stream of a user is resumable — the events missed while the
// listener of the api pod was away come back on reconnect by Last-Event-ID
// (FTR.NAB.CMN-0001 R30, tech §3.2 decision 27).
func TestEventJournal(t *testing.T) {
	ctx := context.Background()
	uid := newUser(t, "events@x.org")
	pub := events.NewPGPublisher(pool)

	// a delta is not kept: a reconnected site reads the message itself
	pub.Publish(ctx, events.Event{Type: events.MessageDelta, UserID: &uid, Data: map[string]any{"delta": "x"}})
	pub.Publish(ctx, events.Event{Type: events.MessageCreated, UserID: &uid, Data: map[string]any{"id": "m1"}})
	pub.Publish(ctx, events.Event{Type: events.MessageDone, UserID: &uid, Data: map[string]any{"id": "m1", "status": "done"}})

	all, err := events.Replay(ctx, pool, uid, 0, events.ReplayLimit)
	if err != nil {
		t.Fatalf("CONV-13: replay: %v", err)
	}
	if len(all) != 2 || all[0].Type != events.MessageCreated || all[1].Type != events.MessageDone || all[0].Seq == 0 || all[1].Seq <= all[0].Seq {
		t.Fatalf("CONV-13: replay %+v", all)
	}
	// the fragment: only what came after the last id the site saw
	rest, err := events.Replay(ctx, pool, uid, all[0].Seq, events.ReplayLimit)
	if err != nil {
		t.Fatalf("CONV-13: replay after an id: %v", err)
	}
	if len(rest) != 1 || rest[0].Type != events.MessageDone || rest[0].Seq != all[1].Seq {
		t.Fatalf("CONV-13: after id %d: %+v", all[0].Seq, rest)
	}
	// a gap wider than the limit keeps the newest events: the end of a turn
	// matters more than its beginning
	newest, err := events.Replay(ctx, pool, uid, 0, 1)
	if err != nil {
		t.Fatalf("CONV-13: replay with a limit: %v", err)
	}
	if len(newest) != 1 || newest[0].Type != events.MessageDone {
		t.Fatalf("CONV-13: the newest event: %+v", newest)
	}
	// the journal keeps an hour and no more
	if _, err := pool.Exec(ctx, `UPDATE events SET at = now() - interval '2 hours' WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	if err := events.Cleanup(ctx, pool); err != nil {
		t.Fatalf("CONV-13: cleanup: %v", err)
	}
	old, err := events.Replay(ctx, pool, uid, 0, events.ReplayLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 0 {
		t.Fatalf("CONV-13: the journal kept %d events older than the retention", len(old))
	}
}
