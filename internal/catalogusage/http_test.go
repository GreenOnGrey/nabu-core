package catalogusage

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CTR-01: the query parameters of the table are read as the contract says.
func TestFilterParsing(t *testing.T) {
	now := time.Date(2026, 3, 2, 10, 0, 0, 0, time.UTC)
	svc := &Service{Cfg: DefaultConfig(), Now: func() time.Time { return now }}
	r := httptest.NewRequest("GET", "/catalog/usage", nil)
	f, err := svc.filter(r)
	if err != nil {
		t.Fatal(err)
	}
	if !f.To.Equal(now.Truncate(24*time.Hour)) || !f.From.Equal(now.AddDate(0, 0, -29).Truncate(24*time.Hour)) {
		t.Fatalf("default period = %s..%s, want the last 30 days", f.From, f.To)
	}
	if f.Step != "day" || f.Limit != 50 {
		t.Fatalf("step=%q limit=%d", f.Step, f.Limit)
	}
	group := uuid.New()
	r = httptest.NewRequest("GET", "/catalog/usage?from=2026-01-01&to=2026-02-01&step=month&group="+group.String()+
		"&callerKind=service&channel=web&mode=platform&includeUnpublished=true&hint=unused&limit=200", nil)
	f, err = svc.filter(r)
	if err != nil {
		t.Fatal(err)
	}
	if f.Step != "month" || len(f.Groups) != 1 || f.Groups[0] != group || f.CallerKind != CallerService ||
		f.Channel != "web" || f.Mode != "platform" || !f.IncludeUnpublished || f.Hint != HintUnused || f.Limit != 200 {
		t.Fatalf("filter = %+v", f)
	}
	// the invalid values are 422 invalid_period, not a silent default
	for _, q := range []string{"step=year", "mode=group", "hint=maybe", "from=xx", "group=not-a-uuid", "callerKind=robot",
		"from=2026-03-01&to=2026-02-01"} {
		if _, err := svc.filter(httptest.NewRequest("GET", "/catalog/usage?"+q, nil)); err == nil {
			t.Fatalf("%q must be refused", q)
		}
	}
}
