package comm

import (
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/collect"
)

func batchOf(threads ...collect.Thread) *Batch { return &Batch{Threads: threads} }

// CMT-14: the day of a thread may be left out when the batch has one thread
// with the key; two days of one key need the day (FTR.NAB.CMN-0003 R6).
func TestCMT14ThreadDayOptional(t *testing.T) {
	b := batchOf(
		collect.Thread{Key: "chat-1", DayKey: "2026-11-26"},
		collect.Thread{Key: "chat-1", DayKey: "2026-11-25"},
		collect.Thread{Key: "mail-1"},
	)
	if got := b.thread("mail-1", ""); got == nil || got.Key != "mail-1" {
		t.Fatalf("the only thread of the key: %+v", got)
	}
	if got := b.thread("chat-1", ""); got != nil {
		t.Fatalf("two days of one key must need the day: %+v", got)
	}
	if err := b.threadRefusal("chat-1", ""); err == nil || !strings.Contains(err.Error(), "give dayKey") {
		t.Fatalf("the refusal of a key with several days asks for the day: %v", err)
	}
	if err := b.threadRefusal("nope", ""); err == nil || strings.Contains(err.Error(), "give dayKey") {
		t.Fatalf("the refusal of an unknown key: %v", err)
	}
	if got := b.thread("chat-1", "2026-11-25"); got == nil || got.DayKey != "2026-11-25" {
		t.Fatalf("the exact day: %+v", got)
	}
	if got := b.thread("nope", ""); got != nil {
		t.Fatalf("an unknown key: %+v", got)
	}
}

// CMT-15: the basis message is found in any thread of the batch, and a refusal
// names the threads of the batch (FTR.NAB.CMN-0003 R12).
func TestCMT15BasisAcrossThreads(t *testing.T) {
	b := batchOf(
		collect.Thread{Key: "mail-1", Messages: []collect.Msg{{ID: "m1"}}},
		collect.Thread{Key: "chat-2", DayKey: "2026-11-26", Messages: []collect.Msg{{ID: "m2"}, {ID: "m3"}}},
	)
	if got := b.threadOf("m3"); got == nil || got.Key != "chat-2" {
		t.Fatalf("the message of the second thread: %+v", got)
	}
	if b.threadOf("") != nil || b.threadOf("m9") != nil {
		t.Fatal("a message outside the batch was found")
	}
	hint := b.threadHint()
	if !strings.Contains(hint, `threadKey "mail-1"`) || !strings.Contains(hint, `threadKey "chat-2"`) {
		t.Fatalf("the hint does not name the threads: %s", hint)
	}
}

// CMT-16: the hint of a long batch names the count and the first threads, not
// all of them (FTR.NAB.CMN-0003 R6).
func TestCMT16ThreadHintLong(t *testing.T) {
	var threads []collect.Thread
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		threads = append(threads, collect.Thread{Key: k})
	}
	hint := batchOf(threads...).threadHint()
	if !strings.Contains(hint, "5 threads") || !strings.Contains(hint, "call comm_batch") || strings.Contains(hint, `"e"`) {
		t.Fatalf("the hint of a long batch: %s", hint)
	}
	if got := batchOf().threadHint(); got != "the batch has no threads" {
		t.Fatalf("the hint of an empty batch: %q", got)
	}
}
