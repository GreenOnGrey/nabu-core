package tasks

import (
	"strings"
	"testing"
	"time"
)

// TSK-03: the minimum interval, invalid cron.
func TestValidateCron(t *testing.T) {
	r := Rules{MinInterval: 15 * time.Minute, MaxActive: 20}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	if _, err := r.ValidateCron("*/5 * * * *", time.UTC, now); err == nil || !strings.Contains(err.Error(), "minimum") {
		t.Fatalf("5 minutes accepted: %v", err)
	}
	if _, err := r.ValidateCron("*/15 * * * *", time.UTC, now); err != nil {
		t.Fatalf("15 minutes refused: %v", err)
	}
	if _, err := r.ValidateCron("0 10 * *", time.UTC, now); err == nil {
		t.Fatal("4 fields accepted")
	}
}

// TSK-09: runs at 10:00 in the user's time zone.
func TestTimezone(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Yekaterinburg")
	r := Rules{MinInterval: 15 * time.Minute}
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC) // Sunday
	first, err := r.ValidateCron("0 10 * * 1", loc, now)
	if err != nil {
		t.Fatal(err)
	}
	if l := first.In(loc); l.Hour() != 10 || l.Weekday() != time.Monday {
		t.Fatalf("first run %v", l)
	}
	if first.UTC().Hour() != 5 {
		t.Fatalf("UTC hour %d", first.UTC().Hour())
	}
}

// TSK-12: only the latest missed run counts.
func TestLatestBefore(t *testing.T) {
	from := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	now := from.Add(3 * time.Hour)
	last := LatestBefore("0 * * * *", "UTC", from, now)
	if !last.Equal(now) {
		t.Fatalf("latest %v", last)
	}
}

func TestHuman(t *testing.T) {
	if h := Human("cron", "0 10 * * 1", nil, "UTC"); h != "Every Monday at 10:00" {
		t.Fatal(h)
	}
	if h := Human("cron", "*/30 * * * *", nil, "UTC"); h != "Every 30 minutes" {
		t.Fatal(h)
	}
}

func TestParseOnce(t *testing.T) {
	loc, _ := time.LoadLocation("Europe/Moscow")
	tm, err := ParseOnce("2026-10-09T11:30", loc)
	if err != nil || tm.UTC().Hour() != 8 {
		t.Fatalf("%v %v", tm, err)
	}
}
