package catalog

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// CAT-11 (FTR.NAB.CMN-0001 tech, decision 11): an MCP client probes the proxy
// with GET for a stream it does not serve, and the answer is 405 before any
// token or access check — not the 403 of a call the client cannot make.
func TestProxyRefusesNonPost(t *testing.T) {
	s := &Service{}
	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		w := httptest.NewRecorder()
		s.proxy(w, httptest.NewRequest(m, "/internal/v1/mcp-proxy/x", nil), nil)
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
			t.Fatalf("%s: %d %q", m, w.Code, w.Header().Get("Allow"))
		}
	}
}

// RATE-02 (FTR.NAB.CMN-0003 qa; FTR.NAB.ADM-0001 tech §4.3): a source of a company system answers the limit of its budget in the
// result of a tool, not as HTTP 429 — «Rate limit exceeded for budget "short" (5m0s). Retry in 276 seconds.»
// The tick reads it as a limit with the wait the server named, not as a failure of the source.
func TestRateLimitAnswer(t *testing.T) {
	for _, tc := range []struct {
		text  string
		after time.Duration
		ok    bool
	}{
		{`Rate limit exceeded for budget "short" (5m0s). Retry in 276 seconds.`, 276 * time.Second, true},
		{`rate limit: retry after 2m`, 2 * time.Minute, true},
		{`Too many requests, retry in 1 hour`, time.Hour, true},
		{`Превышен лимит запросов, повторите через 30`, 0, true}, // the phrase, no time
		{`the tool failed: HTTP 500`, 0, false},
		{`no such thread`, 0, false},
		{``, 0, false},
	} {
		after, ok := rateLimitAnswer(tc.text)
		if ok != tc.ok || after != tc.after {
			t.Fatalf("%q: got %v %v, want %v %v", tc.text, after, ok, tc.after, tc.ok)
		}
	}
}

// The error of a limited source is ErrSourceRateLimited for every caller that maps it to collect.ErrRateLimited,
// and it names the wait the server asked for.
func TestRateLimitedError(t *testing.T) {
	var err error = &RateLimited{After: 90 * time.Second}
	if !errors.Is(err, ErrSourceRateLimited) {
		t.Fatalf("errors.Is: %v", err)
	}
	var rl *RateLimited
	if !errors.As(err, &rl) || rl.After != 90*time.Second {
		t.Fatalf("errors.As: %v", err)
	}
	if got := err.Error(); got != "the source limits the rate of requests, retry in 1m30s" {
		t.Fatalf("text: %q", got)
	}
	if got := (&RateLimited{}).Error(); got != ErrSourceRateLimited.Error() {
		t.Fatalf("text without a wait: %q", got)
	}
}
