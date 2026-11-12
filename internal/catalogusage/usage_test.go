package catalogusage

import (
	"encoding/csv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// cfg is the configuration of the documented defaults.
func cfg() Config { return DefaultConfig() }

func day(s string) time.Time {
	t, _ := time.Parse("2006-01-02", s)
	return t
}

func ts(t time.Time) *time.Time { return &t }

// base is an item with an explicit grant, available for the whole window and
// published long ago: no suppression applies.
func base(now time.Time) HintInput {
	return HintInput{ItemType: "mcp", Published: true, ExplicitGrant: true, PublishedAt: ts(now.AddDate(0, 0, -200)),
		Now: now, Cfg: cfg(), ReadyHints: true, ReadyQuick: true, AvailableDays: 30}
}

// HNT-01, HNT-02, REF-01: the hints and the states of the table.
func TestEvaluateHints(t *testing.T) {
	now := day("2026-03-02").Add(12 * time.Hour)
	cases := []struct {
		name  string
		in    HintInput
		state string
		hints []string
	}{
		{"HNT-01 no human calls is unused", base(now), StateUnused, []string{HintUnused}},
		// HNT-02: administrator calls do not count as use
		{"HNT-02 administrator only", func() HintInput {
			in := base(now)
			in.HumanCalls, in.HumanUsers = 0, 0
			return in
		}(), StateUnused, []string{HintUnused}},
		{"HNT-05 one person", func() HintInput {
			in := base(now)
			in.HumanCalls, in.HumanUsers = 5, 1
			return in
		}(), StateSingleUser, []string{HintSingleUser}},
		{"HNT-06 only errors", func() HintInput {
			in := base(now)
			in.HumanCalls, in.HumanUsers, in.ActualCalls, in.ActualOK = 25, 3, 25, 5
			return in
		}(), StateErrorsOnly, []string{HintErrorsOnly}},
		{"HNT-07 auth is outside the denominator", func() HintInput {
			in := base(now)
			in.HumanCalls, in.HumanUsers, in.ActualCalls, in.ActualOK, in.AuthCalls = 30, 3, 0, 0, 30
			return in
		}(), StateActive, []string{}},
		{"REF-01 dormant", func() HintInput {
			in := base(now)
			in.Calls90 = 12
			return in
		}(), StateDormant, []string{HintUnused, HintDormant}},
		{"CAL-01 service only", func() HintInput {
			in := base(now)
			in.ServiceCalls = 300
			return in
		}(), StateService, nil},
		{"GRP-02 not granted", func() HintInput {
			in := base(now)
			in.ExplicitGrant = false
			return in
		}(), StateNotGranted, []string{}},
		{"CUT-01 unpublished", func() HintInput {
			in := base(now)
			in.Published = false
			return in
		}(), StateUnpublish, []string{}},
		{"UNV-01 unavailable", func() HintInput {
			in := base(now)
			in.Status = "error"
			return in
		}(), StateUnavail, []string{}},
		{"PUB-03 young", func() HintInput {
			in := base(now)
			in.PublishedAt = ts(now.AddDate(0, 0, -10))
			return in
		}(), StateYoung, []string{}},
		{"PIN-01 pinned", func() HintInput {
			in := base(now)
			in.Pinned = true
			return in
		}(), StatePinned, []string{}},
		// PUB-01: removed for 60 days and returned 3 days ago — 3 days of availability
		{"PUB-01 returned recently", func() HintInput {
			in := base(now)
			in.PublishedAt = ts(now.AddDate(0, 0, -63))
			in.AvailableDays = 3
			return in
		}(), StateActive, []string{}},
		// RDY-01: the hints window is not collected yet
		{"RDY-01 not ready", func() HintInput {
			in := base(now)
			in.ReadyHints = false
			return in
		}(), StateActive, []string{}},
		// HNT-03, SKL-04: skills never get «unused»
		{"HNT-03 skill", func() HintInput {
			in := base(now)
			in.ItemType = "skill"
			return in
		}(), StateActive, []string{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state, hints := Evaluate(c.in)
			if state != c.state {
				t.Fatalf("state = %q, want %q", state, c.state)
			}
			if c.hints != nil && strings.Join(hints, ",") != strings.Join(c.hints, ",") {
				t.Fatalf("hints = %v, want %v", hints, c.hints)
			}
			if c.hints == nil && len(hints) != 0 {
				t.Fatalf("hints = %v, want none", hints)
			}
		})
	}
}

// HNT-04: the priorities of suppression of the note of the table.
func TestEvaluatePriority(t *testing.T) {
	now := day("2026-03-02").Add(12 * time.Hour)
	// not_granted wins over unpublished, unavailable, young and pinned
	in := base(now)
	in.ExplicitGrant, in.Published, in.Status, in.Pinned = false, false, "error", true
	in.PublishedAt = ts(now.AddDate(0, 0, -1))
	if state, _ := Evaluate(in); state != StateNotGranted {
		t.Fatalf("state = %q, want %q", state, StateNotGranted)
	}
	// unpublished wins over unavailable and young
	in = base(now)
	in.Published, in.Status = false, "error"
	in.PublishedAt = ts(now.AddDate(0, 0, -1))
	if state, _ := Evaluate(in); state != StateUnpublish {
		t.Fatalf("state = %q, want %q", state, StateUnpublish)
	}
	// unavailable wins over young and pinned
	in = base(now)
	in.Status, in.Pinned = "error", true
	in.PublishedAt = ts(now.AddDate(0, 0, -1))
	if state, _ := Evaluate(in); state != StateUnavail {
		t.Fatalf("state = %q, want %q", state, StateUnavail)
	}
	// young wins over pinned
	in = base(now)
	in.Pinned = true
	in.PublishedAt = ts(now.AddDate(0, 0, -1))
	if state, _ := Evaluate(in); state != StateYoung {
		t.Fatalf("state = %q, want %q", state, StateYoung)
	}
	// UNV-05: unconfigured is not an error
	in = base(now)
	in.Status = "unconfigured"
	in.HumanCalls, in.HumanUsers = 4, 3
	if state, _ := Evaluate(in); state != StateActive {
		t.Fatalf("state = %q, want %q", state, StateActive)
	}
}

// RDY-01…RDY-05: the readiness of the windows (R6).
func TestReadiness(t *testing.T) {
	now := day("2026-03-02")
	c := cfg()
	from := now.AddDate(0, 0, -9)
	r := Readiness(from, now, c)
	if r.ObservationDays != 10 {
		t.Fatalf("observation days = %d, want 10", r.ObservationDays)
	}
	if r.Hints {
		t.Fatal("hints must not be ready on day 10")
	}
	if r.Goal {
		t.Fatal("the goal must not be ready on day 10")
	}
	if r.ObservationLeft != 80 {
		t.Fatalf("observation left = %d, want 80", r.ObservationLeft)
	}
	// RDY-04: the quick hints work from 7 days
	r = Readiness(now.AddDate(0, 0, -7), now, c)
	if !r.Quick || r.Hints {
		t.Fatalf("quick = %v, hints = %v on day 8", r.Quick, r.Hints)
	}
	// RDY-02: hints from 30 days, the goal still a progress
	r = Readiness(now.AddDate(0, 0, -29), now, c)
	if !r.Hints || r.Goal {
		t.Fatalf("hints = %v, goal = %v on day 30", r.Hints, r.Goal)
	}
	// RDY-05: the goal from 90 days
	r = Readiness(now.AddDate(0, 0, -89), now, c)
	if !r.Goal || r.ObservationLeft != 0 {
		t.Fatalf("goal = %v, left = %d on day 90", r.Goal, r.ObservationLeft)
	}
}

// CFG-02: a wider hint window moves the readiness and the hint itself.
func TestHintsWindowConfig(t *testing.T) {
	now := day("2026-03-02").Add(12 * time.Hour)
	c := cfg()
	c.UnusedDays = 45
	in := base(now)
	in.Cfg = c
	in.AvailableDays = 40
	in.ReadyHints = false
	if state, _ := Evaluate(in); state == StateUnused {
		t.Fatal("unused with 40 available days of a 45-day window")
	}
	in.ReadyHints, in.AvailableDays = true, 45
	if state, _ := Evaluate(in); state != StateUnused {
		t.Fatalf("state = %q, want unused", state)
	}
}

// RDY-03, DRL-02: the periods and the windows are clamped.
func TestWindows(t *testing.T) {
	dataFrom := day("2026-02-01")
	from, to, clamped := ClampPeriod(day("2026-01-01"), day("2026-03-01"), &dataFrom)
	if !clamped || !from.Equal(dataFrom) || !to.Equal(day("2026-03-01")) {
		t.Fatalf("clamp: %s..%s clamped=%v", from, to, clamped)
	}
	if from, _, clamped := ClampPeriod(day("2026-02-10"), day("2026-03-01"), &dataFrom); clamped || !from.Equal(day("2026-02-10")) {
		t.Fatalf("clamp of a period inside the data: %s %v", from, clamped)
	}
	from, to, clamped = DrilldownWindow(day("2026-01-01"), day("2026-03-01"), 31)
	if !clamped {
		t.Fatal("the drill-down window must be clamped")
	}
	if days := PeriodDays(from, to); days != 31 {
		t.Fatalf("drill-down days = %d, want 31", days)
	}
	if _, _, clamped := DrilldownWindow(day("2026-02-01"), day("2026-02-10"), 31); clamped {
		t.Fatal("a window of 10 days must not be clamped")
	}
}

// PIN-04: the default date of the review of a pin.
func TestPinReviewAt(t *testing.T) {
	now := day("2026-03-02")
	if got := PinReviewAt(now, cfg()); !got.Equal(now.AddDate(0, 0, 90)) {
		t.Fatalf("review at = %s, want +90 days", got)
	}
	c := cfg()
	c.PinDays = 30
	if got := PinReviewAt(now, c); !got.Equal(now.AddDate(0, 0, 30)) {
		t.Fatalf("review at = %s, want +30 days", got)
	}
}

// AGG-07: the percentiles are recovered from the merged buckets.
func TestPercentiles(t *testing.T) {
	buckets := map[string]int64{"100": 10, "500": 20, "1000": 40, "10000": 30}
	p50, p95 := Percentiles(buckets)
	if p50 == nil || p95 == nil {
		t.Fatal("no percentiles")
	}
	if *p50 != 1000 {
		t.Fatalf("p50 = %d, want 1000", *p50)
	}
	if *p95 != 10000 {
		t.Fatalf("p95 = %d, want 10000", *p95)
	}
	// the merge of two days sums the buckets
	merged := map[string]int64{}
	MergeLatency(merged, []byte(`{"100": 10, "1000": 40}`))
	MergeLatency(merged, []byte(`{"100": 10, "500": 40}`))
	if merged["100"] != 20 || merged["500"] != 40 || merged["1000"] != 40 {
		t.Fatalf("merged = %v", merged)
	}
	if p50, _ := Percentiles(merged); p50 == nil || *p50 != 500 {
		t.Fatalf("merged p50 = %v, want 500", p50)
	}
	if p50, p95 := Percentiles(nil); p50 != nil || p95 != nil {
		t.Fatal("an empty histogram has no percentiles")
	}
}

// The bucket of a duration follows the boundaries of the proxy histogram.
func TestBucket(t *testing.T) {
	for _, c := range []struct{ ms, want int64 }{{50, 100}, {100, 100}, {101, 250}, {420, 500}, {2600, 5000}, {300000, 300000}, {400000, 600000}} {
		if got := Bucket(c.ms); got != c.want {
			t.Fatalf("bucket(%d) = %d, want %d", c.ms, got, c.want)
		}
	}
}

// CSV-02, TIL-04: the export repeats the columns of the screen, has the BOM,
// the comma and no personalities or money.
func TestCSV(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mode := "platform"
	p50, rejected := int64(420), int64(3)
	rows := []UsageRow{{ItemID: id, ItemName: "Jira", ItemType: "mcp", Mode: &mode, Published: true,
		ToolCount: 12, Calls: 310, AdminCalls: 12, ServiceCalls: 40, Users: 7, Sessions: 22, OK: 280,
		ErrorsByCode: map[string]int64{"tool_error": 18}, AuthCalls: 4, DeniedCalls: 2, P50Ms: &p50,
		AvailableDays: 30, Hints: []string{HintUnused}, State: StateDormant, ConfirmationsRejected: &rejected}}
	b, err := CSV(rows)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.HasPrefix(s, "\ufeff") {
		t.Fatal("no BOM")
	}
	for _, bad := range []string{"user_id", "userEmail", "email", "costUsd", "tokens", "userName"} {
		if strings.Contains(s, bad) {
			t.Fatalf("the export has %q", bad)
		}
	}
	recs, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(s, "\ufeff"))).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	if len(recs[0]) != len(recs[1]) {
		t.Fatalf("header %d fields, row %d fields", len(recs[0]), len(recs[1]))
	}
	if recs[1][1] != "Jira" || recs[1][8] != "12" || recs[1][23] != StateDormant {
		t.Fatalf("row = %v", recs[1])
	}
	// a missing number is empty, not zero (R17: «нет данных», not 0)
	rows[0].ConfirmationsRejected = nil
	b, _ = CSV(rows)
	recs, _ = csv.NewReader(strings.NewReader(strings.TrimPrefix(string(b), "\ufeff"))).ReadAll()
	if recs[1][len(recs[1])-1] != "" {
		t.Fatalf("confirmations = %q, want empty", recs[1][len(recs[1])-1])
	}
}

// R9, R19: the administrators are not people of the hints.
func TestHumanUsers(t *testing.T) {
	if got := humanUsers(5, 0); got != 5 {
		t.Fatalf("users without admins = %d, want 5", got)
	}
	if got := humanUsers(3, 4); got != 2 {
		t.Fatalf("users with an administrator = %d, want 2", got)
	}
	if got := humanUsers(1, 4); got != 0 {
		t.Fatalf("the only user is an administrator = %d, want 0", got)
	}
}

// TBL-05: the column «tools of the item» reads the allowed list, else the
// checked one.
func TestToolsOf(t *testing.T) {
	if n := ToolsOf([]byte(`["a","b","c"]`), []byte(`["a","b","c","d"]`)); n != 3 {
		t.Fatalf("tools = %d, want 3", n)
	}
	if n := ToolsOf(nil, []byte(`[{"name":"a"},{"name":"b"}]`)); n != 2 {
		t.Fatalf("tools = %d, want 2", n)
	}
	if n := ToolsOf(nil, nil); n != 0 {
		t.Fatalf("tools = %d, want 0", n)
	}
}

// GOL-01, R16: the success share keeps auth and denied outside the denominator.
func TestSuccessShare(t *testing.T) {
	if v := SuccessShare(91, 100, 5, 4); v == nil || *v < 0.9 || *v > 1 {
		t.Fatalf("share = %v", v)
	}
	if v := SuccessShare(0, 0, 0, 0); v != nil {
		t.Fatal("no actual calls must give a nil share")
	}
	if v := AuthShareOf(60, 100, 0); v != 0.6 {
		t.Fatalf("auth share = %v, want 0.6", v)
	}
}

// CFG-01: the documented defaults of the configuration.
func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	th := c.Thresholds()
	if th.UnusedDays != 30 || th.ReferenceDays != 90 || th.PinDays != 90 || th.SingleUserMax != 1 ||
		th.ErrorsShare != 0.7 || th.ErrorsMinCalls != 20 || th.AuthShare != 0.5 || th.AuthMinCalls != 20 || th.ToolsCandidate != 10 {
		t.Fatalf("thresholds = %+v", th)
	}
	w := c.Windows()
	if w.HintDays != 30 || w.GoalDays != 90 || w.DrilldownDays != 31 {
		t.Fatalf("windows = %+v", w)
	}
	if c.QuickDays != 7 || c.RetentionDays != 400 || c.MonthlyMonths != 36 || c.Tick != time.Hour || c.CloseAfter != "00:15" {
		t.Fatalf("config = %+v", c)
	}
}
