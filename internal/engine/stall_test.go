package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
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

// STL-19: a turn that gets no events of the operator for TURN_SILENCE_TIMEOUT
// is stopped by the silence watchdog (FTR.NAB.CMN-0010 R10).
func TestSTL19SilenceWatchdog(t *testing.T) {
	e := &Engine{Cfg: Config{TurnSilenceTimeout: 80 * time.Millisecond}}
	fired := make(chan struct{}, 1)
	col := &collector{abort: func() { fired <- struct{}{} }}
	col.touch()
	end := e.watchSilence(context.Background(), col)
	defer end()
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("the watchdog did not fire on silence")
	}
	if !col.silent.Load() {
		t.Fatal("the watchdog did not mark the turn silent")
	}
}

// STL-20: a running tool is not silence: the turn waits for its answer, and
// the watchdog arms again once the tool has answered (FTR.NAB.CMN-0010 R10).
func TestSTL20SilenceWatchdogToolInFlight(t *testing.T) {
	e := &Engine{Cfg: Config{TurnSilenceTimeout: 40 * time.Millisecond}}
	fired := make(chan struct{}, 1)
	col := &collector{abort: func() { fired <- struct{}{} }}
	col.handle(agent.Event{Type: agent.EventToolCall, ID: "1", Name: "mcp__nabu__commitment_list"})
	if n := col.toolInFlight.Load(); n != 1 {
		t.Fatalf("calls in flight: %d", n)
	}
	end := e.watchSilence(context.Background(), col)
	defer end()
	select {
	case <-fired:
		t.Fatal("the watchdog fired while a tool ran")
	case <-time.After(300 * time.Millisecond): // many times the limit: not a race with one tick
	}
	col.toolInFlight.Store(0) // the tool answered
	col.touch()
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("the watchdog did not fire after the tool finished")
	}
}

// STL-21: TURN_SILENCE_TIMEOUT=0 switches the watchdog off (R8, R10).
func TestSTL21SilenceWatchdogOff(t *testing.T) {
	e := &Engine{}
	col := &collector{abort: func() { t.Error("the watchdog fired with the limit off") }}
	col.touch()
	end := e.watchSilence(context.Background(), col)
	time.Sleep(100 * time.Millisecond)
	end()
	if col.silent.Load() {
		t.Fatal("silent with the watchdog off")
	}
}

// STL-22: the row of a turn that did not end as a usual answer (R11): the
// reason, the result and when there is no row at all.
func TestSTL22TurnAudit(t *testing.T) {
	base := ledger.Entry{AgentKind: "personal", Agent: "Захар", Channel: "vkteams"}
	cases := []struct {
		name           string
		res            turnResult
		providerFailed bool
		row            bool
		result, reason string
	}{
		{"usual answer", turnResult{text: "готово"}, false, false, "", ""},
		{"stopped by the user", turnResult{stopped: true}, true, true, "error", "stopped"},
		{"silence of the model", turnResult{errClass: string(agent.ErrTimeout), stalled: stallNoOutput}, false, true, "error", "stalled:no_output"},
		{"repeated call", turnResult{stalled: stallRepeatedCall}, false, true, "ok", "stalled:repeated_call"},
		{"provider error", turnResult{errClass: string(agent.ErrBadRequest)}, true, false, "", ""},
		{"operator error", turnResult{errClass: "agent_busy"}, false, true, "error", "agent_busy"},
		{"the turn goes back to the queue", turnResult{podLost: true, errClass: string(agent.ErrAgentCrashed)}, false, false, "", ""},
	}
	for _, c := range cases {
		a, ok := turnAudit(base, c.res, c.providerFailed)
		if ok != c.row {
			t.Fatalf("%s: row written=%v, want %v", c.name, ok, c.row)
		}
		if !ok {
			continue
		}
		if a.Tool != "turn" || a.Result != c.result || a.Error == nil || *a.Error != c.reason {
			t.Fatalf("%s: tool=%q result=%q error=%v, want %q/%q/%q", c.name, a.Tool, a.Result, a.Error, "turn", c.result, c.reason)
		}
	}
}
