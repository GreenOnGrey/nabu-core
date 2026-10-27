package events

import (
	"encoding/json"
	"strings"
	"testing"
)

// FTR.NAB.CMN-0001 CONV-12: an event too large for NOTIFY keeps the ids of the
// message, so the client reads it again instead of waiting.
func TestStub(t *testing.T) {
	msg := map[string]any{"id": "m1", "conversationId": "c1", "role": "assistant", "status": "done",
		"text": strings.Repeat("x", 9000), "attachments": []any{map[string]any{"id": "a1"}}}
	b, _ := json.Marshal(Event{Type: MessageDone, Data: Stub(msg)})
	if len(b) > maxNotify {
		t.Fatalf("stub is %d bytes", len(b))
	}
	var got struct {
		Data map[string]any `json:"data"`
	}
	_ = json.Unmarshal(b, &got)
	if got.Data["id"] != "m1" || got.Data["conversationId"] != "c1" || got.Data["status"] != "done" || got.Data["truncated"] != true || got.Data["text"] != nil {
		t.Fatalf("stub %v", got.Data)
	}
	if Stub("text") != nil || Stub(nil) != nil {
		t.Fatal("a stub of not an object")
	}
}
