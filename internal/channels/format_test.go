package channels

import (
	"net/http"
	"net/http/httptest"

	"github.com/go-chi/chi/v5"
	"strings"
	"testing"
	"unicode/utf8"
)

// CH-02: special characters are escaped; code is kept.
func TestMarkdownV2(t *testing.T) {
	got := ToMarkdownV2("Total: 5.0 (ok)! **Done** — `a_b` and_more")
	want := "Total: 5\\.0 \\(ok\\)\\! *Done* — `a_b` and\\_more"
	if got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
	code := ToMarkdownV2("```go\nfmt.Println(\"a`b\")\n```")
	if code != "```go\nfmt.Println(\"a\\`b\")\n```" {
		t.Fatal(code)
	}
}

// CH-01: long answers are split; every part fits and code blocks stay closed.
func TestSplit(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 60; i++ {
		b.WriteString(strings.Repeat("word. ", 20) + "\n\n")
	}
	b.WriteString("```\n")
	for i := 0; i < 400; i++ {
		b.WriteString("line of code number ...\n")
	}
	b.WriteString("```\n\nThe end.")
	parts := Split(b.String(), TelegramLimit)
	if len(parts) < 3 {
		t.Fatalf("parts: %d", len(parts))
	}
	for i, p := range parts {
		r := ToMarkdownV2(p)
		if n := utf8.RuneCountInString(r); n > TelegramLimit {
			t.Fatalf("part %d: %d characters", i, n)
		}
		if strings.Count(p, "```")%2 != 0 {
			t.Fatalf("part %d has an unclosed code block", i)
		}
	}
	if !strings.HasSuffix(parts[len(parts)-1], "The end.") {
		t.Fatal("lost the tail")
	}
}

// CH-03: the webhook without the secret answers 401.
func TestWebhookSecret(t *testing.T) {
	h := &Webhook{Bot: NewTelegram("http://x", "t", "s3cret")}
	r := chi.NewRouter()
	h.Route(r)
	for _, c := range []struct {
		path, hdr string
		want      int
	}{{"/hooks/v1/telegram/s3cret", "", 401}, {"/hooks/v1/telegram/wrong", "s3cret", 401}, {"/hooks/v1/telegram/s3cret", "s3cret", 200}} {
		req := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(`{}`))
		if c.hdr != "" {
			req.Header.Set("X-Telegram-Bot-Api-Secret-Token", c.hdr)
		}
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s %q: %d", c.path, c.hdr, rec.Code)
		}
	}
}
