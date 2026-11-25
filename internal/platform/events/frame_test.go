package events

import (
	"net/http"
	"strings"
	"testing"
)

// FTR.NAB.CMN-0001 CONV-13: a kept event carries its journal id, so the browser
// sends it back as Last-Event-ID and the site gets what it missed (tech §3.2,
// decision 27); a delta carries no id and is restored by reading the message.
func TestFrame(t *testing.T) {
	f := frame(Event{Type: MessageDone, Data: map[string]any{"id": "m1"}, Seq: 42})
	if !strings.HasPrefix(f, "id: 42\n") || !strings.Contains(f, "event: message.done\n") || !strings.HasSuffix(f, "\n\n") {
		t.Fatalf("CONV-13: frame %q", f)
	}
	if f := frame(Event{Type: MessageDelta, Data: map[string]any{"delta": "x"}}); strings.HasPrefix(f, "id: ") {
		t.Fatalf("CONV-13: a delta has an id: %q", f)
	}
	if frame(Event{Data: func() {}}) != "" {
		t.Fatal("CONV-13: data that does not encode gave a frame")
	}
}

// FTR.NAB.CMN-0001 CONV-13: Last-Event-ID of a reconnect is the journal id; a
// missing or broken header replays nothing.
func TestLastEventID(t *testing.T) {
	for header, want := range map[string]int64{"": 0, "42": 42, " 7 ": 7, "abc": 0, "-1": 0} {
		r, _ := http.NewRequest(http.MethodGet, "/events", nil)
		if header != "" {
			r.Header.Set("Last-Event-ID", header)
		}
		if got := lastEventID(r); got != want {
			t.Fatalf("Last-Event-ID %q: %d, want %d", header, got, want)
		}
	}
}
