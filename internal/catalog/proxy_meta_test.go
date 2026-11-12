package catalog

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
)

// ATR-01, ATR-02: the answer of a call carries the code and the duration in
// result._meta["nabu/call"]; the engine takes the audit from it (R3).
func TestWithCallMeta(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"ok"}]}}`)
	out := withCallMeta(raw, "auth", 1200*time.Millisecond)
	var msg struct {
		Result struct {
			Content []map[string]string       `json:"content"`
			Meta    map[string]map[string]any `json:"_meta"`
		} `json:"result"`
	}
	if err := json.Unmarshal(out, &msg); err != nil {
		t.Fatal(err)
	}
	call := msg.Result.Meta["nabu/call"]
	if call["code"] != "auth" {
		t.Fatalf("code = %v, want auth", call["code"])
	}
	if call["durationMs"] != float64(1200) {
		t.Fatalf("duration = %v, want 1200", call["durationMs"])
	}
	if len(msg.Result.Content) != 1 || msg.Result.Content[0]["text"] != "ok" {
		t.Fatalf("the result was rewritten: %s", out)
	}
	// a message without a result (a notification) stays as it is
	notif := []byte(`{"jsonrpc":"2.0","method":"notifications/progress"}`)
	if string(withCallMeta(notif, "ok", time.Second)) != string(notif) {
		t.Fatal("a notification must not change")
	}
	// an existing _meta keeps its keys
	withMeta := []byte(`{"jsonrpc":"2.0","id":1,"result":{"_meta":{"other":1}}}`)
	out = withCallMeta(withMeta, "ok", time.Second)
	var m struct {
		Result struct {
			Meta map[string]json.RawMessage `json:"_meta"`
		} `json:"result"`
	}
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Result.Meta["other"]; !ok {
		t.Fatal("an existing _meta key was lost")
	}
	if _, ok := m.Result.Meta["nabu/call"]; !ok {
		t.Fatal("nabu/call is missing")
	}
}

// OBS-02: the label of the caller of a blocked call.
func TestCallerKind(t *testing.T) {
	cases := []struct {
		c    jwt.Claims
		want string
	}{
		{jwt.Claims{}, "user"},
		{jwt.Claims{Channel: "run"}, "service"},
		{jwt.Claims{Channel: "group:11111111-1111-1111-1111-111111111111"}, "group"},
		{jwt.Claims{Channel: "client:hammurapi"}, "client"},
	}
	for _, c := range cases {
		if got := callerKind(c.c); got != c.want {
			t.Fatalf("callerKind(%+v) = %q, want %q", c.c, got, c.want)
		}
	}
}
