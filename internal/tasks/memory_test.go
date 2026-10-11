package tasks

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TASK-03: the runs of the collection are shifted by the user inside the
// interval; TASK-04: without round the clock they stay within 8:00–20:00.
func TestCollectCron(t *testing.T) {
	loc, _ := time.LoadLocation("Europe/Moscow")
	day := time.Date(2026, 10, 14, 0, 0, 0, 0, loc)
	minutes := map[int]bool{}
	for i := 0; i < 200; i++ {
		uid := uuid.New()
		for _, iv := range []struct {
			name string
			runs int
			gap  time.Duration
		}{{"15m", 48, 15 * time.Minute}, {"1h", 12, time.Hour}, {"4h", 3, 4 * time.Hour}, {"1d", 1, 0}} {
			expr, err := CollectCron(uid, iv.name, false)
			if err != nil {
				t.Fatal(err)
			}
			s, err := ParseCron(expr, loc)
			if err != nil {
				t.Fatalf("%s: %q: %v", iv.name, expr, err)
			}
			var runs []time.Time
			for n := s.Next(day.Add(-time.Second)); n.Before(day.AddDate(0, 0, 1)); n = s.Next(n) {
				runs = append(runs, n)
			}
			if len(runs) != iv.runs {
				t.Fatalf("%s %q: %d runs a day, want %d", iv.name, expr, len(runs), iv.runs)
			}
			for j, r := range runs {
				if r.Hour() < collectFromHour || r.Hour() >= collectToHour {
					t.Fatalf("%s %q: a run at %s outside the working hours", iv.name, expr, r.Format("15:04"))
				}
				if j > 0 && r.Sub(runs[j-1]) != iv.gap {
					t.Fatalf("%s %q: the gap is %s", iv.name, expr, r.Sub(runs[j-1]))
				}
			}
			if iv.name == "1h" {
				minutes[runs[0].Minute()] = true
			}
		}
		// round the clock
		for name, want := range map[string]int{"15m": 96, "1h": 24, "4h": 6, "1d": 1} {
			expr, _ := CollectCron(uid, name, true)
			s, err := ParseCron(expr, loc)
			if err != nil {
				t.Fatalf("%s: %q: %v", name, expr, err)
			}
			n := 0
			for x := s.Next(day.Add(-time.Second)); x.Before(day.AddDate(0, 0, 1)); x = s.Next(x) {
				n++
			}
			if n != want {
				t.Fatalf("%s round the clock %q: %d runs, want %d", name, expr, n, want)
			}
		}
	}
	if len(minutes) < 30 {
		t.Fatalf("200 users start in only %d different minutes of the hour", len(minutes))
	}
	if _, err := CollectCron(uuid.New(), "5m", false); err == nil {
		t.Fatal("an unknown interval is accepted")
	}
	// the same user always gets the same shift
	uid := uuid.New()
	a, _ := CollectCron(uid, "1h", false)
	b, _ := CollectCron(uid, "1h", false)
	if a != b || !strings.HasSuffix(a, " 8-19 * * *") {
		t.Fatalf("%q %q", a, b)
	}
}
