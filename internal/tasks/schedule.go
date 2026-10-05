// Package tasks implements scheduled tasks of personal agents
// (FTR.NAB.CMN-0001 R35–R38, arch §9a, tech §3.2a, §9a): the agent creates
// them with the task_* tools of the built-in MCP, the worker plans and starts
// the runs, the user sees and manages them in the Tasks section.
package tasks

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule is a one-off time or a 5-field cron in a time zone.
type Schedule struct {
	Kind     string     `json:"kind"` // once | cron
	At       *time.Time `json:"at,omitempty"`
	Cron     string     `json:"cron,omitempty"`
	Timezone string     `json:"timezone"`
	Human    string     `json:"human"`
}

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// Rules are the limits of tasks (R38).
type Rules struct {
	MinInterval time.Duration
	MaxActive   int
}

// ParseCron parses a 5-field cron expression in the location.
func ParseCron(expr string, loc *time.Location) (cron.Schedule, error) {
	if len(strings.Fields(expr)) != 5 {
		return nil, errors.New("the cron expression must have 5 fields: minute hour day-of-month month day-of-week")
	}
	s, err := parser.Parse("CRON_TZ=" + loc.String() + " " + expr)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}
	return s, nil
}

// ParseOnce reads a local time "2026-10-09T11:30" (or RFC 3339) in the location.
func ParseOnce(s string, loc *time.Location) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02 15:04"} {
		if layout == time.RFC3339 {
			if t, err := time.Parse(layout, s); err == nil {
				return t, nil
			}
			continue
		}
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time %q: use YYYY-MM-DDTHH:MM", s)
}

// Validate checks a cron schedule: the interval between the two nearest runs
// must be at least the minimum (TSK-03).
func (r Rules) ValidateCron(expr string, loc *time.Location, now time.Time) (time.Time, error) {
	s, err := ParseCron(expr, loc)
	if err != nil {
		return time.Time{}, err
	}
	first := s.Next(now)
	if first.IsZero() {
		return first, errors.New("the schedule never runs")
	}
	if second := s.Next(first); !second.IsZero() && second.Sub(first) < r.MinInterval {
		return first, fmt.Errorf("the interval between runs is %s, the minimum is %s", second.Sub(first), r.MinInterval)
	}
	return first, nil
}

// Next is the run after t (zero for a one-off schedule).
func Next(kind, expr, tz string, t time.Time) time.Time {
	if kind != "cron" {
		return time.Time{}
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	s, err := ParseCron(expr, loc)
	if err != nil {
		return time.Time{}
	}
	return s.Next(t)
}

// LatestBefore is the last run of a cron schedule at or before t, starting
// the search at from (zero when there is none).
func LatestBefore(expr, tz string, from, t time.Time) time.Time {
	var last time.Time
	for n := Next("cron", expr, tz, from.Add(-time.Second)); !n.IsZero() && !n.After(t); n = Next("cron", expr, tz, n) {
		last = n
	}
	return last
}

var weekdays = []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}

// Human describes a schedule for people; the interface localizes the
// structured fields and shows this text as the fallback.
func Human(kind, expr string, at *time.Time, tz string) string {
	if kind == "once" && at != nil {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			loc = time.UTC
		}
		return "Once on " + at.In(loc).Format("2006-01-02 at 15:04") + " (" + tz + ")"
	}
	f := strings.Fields(expr)
	if len(f) != 5 {
		return expr
	}
	min, hour, dom, mon, dow := f[0], f[1], f[2], f[3], f[4]
	isNum := func(s string) bool { return s != "" && strings.Trim(s, "0123456789") == "" }
	if strings.HasPrefix(min, "*/") && hour == "*" && dom == "*" && mon == "*" && dow == "*" {
		return "Every " + strings.TrimPrefix(min, "*/") + " minutes"
	}
	if isNum(min) && strings.HasPrefix(hour, "*/") && dom == "*" && mon == "*" && dow == "*" {
		return "Every " + strings.TrimPrefix(hour, "*/") + " hours"
	}
	if !isNum(min) || !isNum(hour) {
		return "Cron " + expr + " (" + tz + ")"
	}
	at2 := fmt.Sprintf("%02s:%02s", hour, min)
	switch {
	case dom == "*" && mon == "*" && dow == "*":
		return "Every day at " + at2
	case dom == "*" && mon == "*" && dow == "1-5":
		return "Every weekday at " + at2
	case dom == "*" && mon == "*" && isNum(dow):
		var d int
		_, _ = fmt.Sscan(dow, &d)
		return "Every " + weekdays[d%7] + " at " + at2
	case isNum(dom) && mon == "*" && dow == "*":
		return "Every month on day " + dom + " at " + at2
	}
	return "Cron " + expr + " (" + tz + ")"
}
