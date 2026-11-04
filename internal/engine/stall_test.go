package engine

import (
	"fmt"
	"testing"
)

func newDetector() *stallDetector { return &stallDetector{repeatLimit: 5, errorLimit: 5} }

// STL-01: five identical calls with the same result.
func TestSTL01RepeatedCall(t *testing.T) {
	d := newDetector()
	for i := 1; i <= 4; i++ {
		if r := d.observe("read", `{"path":"a"}`, "text", false); r != "" {
			t.Fatalf("fired on call %d: %s", i, r)
		}
	}
	if r := d.observe("read", `{"path":"a"}`, "text", false); r != stallRepeatedCall {
		t.Fatalf("the fifth call: %q", r)
	}
}

// STL-02: the same call with a changing result is legitimate polling.
func TestSTL02ChangingResult(t *testing.T) {
	d := newDetector()
	for i := range 10 {
		if r := d.observe("bash", `{"command":"status"}`, fmt.Sprintf("step %d", i), false); r != "" {
			t.Fatalf("fired: %s", r)
		}
	}
}

// STL-03: five failures of different tools in a row.
func TestSTL03ToolErrors(t *testing.T) {
	d := newDetector()
	for i := range 4 {
		if r := d.observe(fmt.Sprintf("tool%d", i), "{}", "boom", true); r != "" {
			t.Fatalf("fired: %s", r)
		}
	}
	if r := d.observe("tool4", "{}", "boom", true); r != stallToolErrors {
		t.Fatalf("the fifth error: %q", r)
	}
}

// STL-04: a failing test run and a successful edit in turn do not stall.
func TestSTL04ErrorsResetBySuccess(t *testing.T) {
	d := newDetector()
	for i := range 10 {
		if r := d.observe("bash", `{"command":"go test"}`, "FAIL", true); r != "" {
			t.Fatalf("fired on %d: %s", i, r)
		}
		if r := d.observe("edit", fmt.Sprintf(`{"n":%d}`, i), "ok", false); r != "" {
			t.Fatalf("fired on %d: %s", i, r)
		}
	}
}

func TestStallDetectorReset(t *testing.T) {
	d := newDetector()
	for range 4 {
		d.observe("read", "{}", "text", true)
	}
	d.reset()
	if r := d.observe("read", "{}", "text", true); r != "" {
		t.Fatalf("fired after a reset: %s", r)
	}
}
