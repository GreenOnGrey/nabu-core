package catalog

import (
	"net/http"
	"testing"
	"time"
)

// FTR.NAB.CMN-0003 decision 54: the proxy reads the wait of a 429 in seconds and as a date.
func TestRetryAfterHeader(t *testing.T) {
	if d := retryAfterHeader(" 90 "); d != 90*time.Second {
		t.Fatalf("seconds: %s", d)
	}
	at := time.Now().Add(2 * time.Minute).UTC().Format(http.TimeFormat)
	if d := retryAfterHeader(at); d < 110*time.Second || d > 2*time.Minute {
		t.Fatalf("a date: %s", d)
	}
	for _, v := range []string{"", "soon", "-5", time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)} {
		if d := retryAfterHeader(v); d != 0 {
			t.Fatalf("%q: %s", v, d)
		}
	}
}

// FTR.NAB.CMN-0003 decision 54: the text of the answer to a call is read from JSON and from a stream of events,
// where the last message with a result is the answer.
func TestCallText(t *testing.T) {
	limit := `{"jsonrpc":"2.0","id":3,"result":{"isError":true,"content":[{"type":"text","text":"Rate limit exceeded. Retry in 276 seconds."}]}}`
	for name, raw := range map[string]string{
		"json":   limit,
		"stream": "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\nevent: message\ndata: " + limit + "\n\n",
	} {
		text, isErr := callText([]byte(raw))
		if !isErr || text != "Rate limit exceeded. Retry in 276 seconds." {
			t.Fatalf("%s: %q %v", name, text, isErr)
		}
		if after, ok := rateLimitAnswer(text); !ok || after != 276*time.Second {
			t.Fatalf("%s: the limit %s %v", name, after, ok)
		}
	}
	if text, isErr := callText([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}`)); !isErr || text != "boom" {
		t.Fatalf("an error of the protocol: %q %v", text, isErr)
	}
	if text, isErr := callText([]byte("not json")); isErr || text != "" {
		t.Fatalf("an unreadable answer: %q %v", text, isErr)
	}
}
