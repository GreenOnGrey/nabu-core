package pirpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// The test binary doubles as a fake Pi: with PIRPC_FAKE set it speaks the RPC
// protocol on stdio according to the mode.
func TestMain(m *testing.M) {
	if mode := os.Getenv("PIRPC_FAKE"); mode != "" {
		fakePi(mode)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func fakePi(mode string) {
	in := bufio.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	var mu sync.Mutex
	emit := func(line string) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = out.WriteString(line + "\n")
		_ = out.Flush()
	}
	respond := func(id, cmd string, data any) {
		b, _ := json.Marshal(map[string]any{"id": id, "type": "response", "command": cmd, "success": true, "data": data})
		emit(string(b))
	}
	var held *struct{ id, cmd string }
	for {
		line, err := readRecord(in)
		if err != nil && len(line) == 0 {
			return
		}
		var c struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}
		_ = json.Unmarshal(line, &c)
		switch {
		case mode == "reorder" && held == nil:
			held = &struct{ id, cmd string }{c.ID, c.Type} // answer after the next command
			continue
		case mode == "reorder":
			respond(c.ID, c.Type, map[string]string{"for": c.ID})
			respond(held.id, held.cmd, map[string]string{"for": held.id})
			held = nil
			continue
		}
		switch c.Type {
		case "prompt":
			respond(c.ID, "prompt", map[string]string{"disposition": "started"})
			run(mode, emit)
		case "get_session_stats":
			respond(c.ID, c.Type, map[string]any{"sessionId": "s1", "tokens": map[string]int{"input": 1200, "output": 30, "cacheRead": 800, "total": 2030}, "cost": 0.0019})
		case "set_model":
			respond(c.ID, c.Type, map[string]any{"id": "deepseek-v4-pro", "provider": "hmr-1", "reasoning": true, "cost": map[string]float64{"input": 1.74}})
		case "compact":
			respond(c.ID, c.Type, map[string]any{"summary": "s", "tokensBefore": 180000, "estimatedTokensAfter": 32000})
		case "switch_session":
			respond(c.ID, c.Type, map[string]bool{"cancelled": false})
		case "get_available_thinking_levels":
			respond(c.ID, c.Type, map[string][]string{"levels": {"off", "high", "xhigh"}})
		case "abort", "set_thinking_level", "set_auto_retry":
			emit(fmt.Sprintf(`{"id":%q,"type":"response","command":%q,"success":true}`, c.ID, c.Type))
		default:
			emit(fmt.Sprintf(`{"id":%q,"type":"response","command":%q,"success":false,"error":"Unknown command"}`, c.ID, c.Type))
		}
	}
}

func run(mode string, emit func(string)) {
	emit(`{"type":"agent_start"}`)
	switch mode {
	case "u2028":
		// A raw U+2028 inside a JSON string must not split the record.
		emit("{\"type\":\"message_update\",\"assistantMessageEvent\":{\"type\":\"text_delta\",\"contentIndex\":0,\"delta\":\"a b c\"}}")
	case "big":
		emit(`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"` + strings.Repeat("x", 1_500_000) + `"}}`)
	case "retry":
		emit(`{"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"error","errorMessage":"429: {\"message\":\"Rate limit\"}"}}`)
		emit(`{"type":"auto_retry_start","attempt":1,"maxAttempts":3,"delayMs":10,"errorMessage":"429"}`)
		emit(`{"type":"agent_end","messages":[],"willRetry":true}`)
		time.Sleep(50 * time.Millisecond)
		emit(`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"ok"}}`)
		emit(`{"type":"auto_retry_end","success":true,"attempt":2}`)
	case "unknown":
		emit(`{"type":"brand_new_event","x":1}`)
	case "crash":
		fmt.Fprintln(os.Stderr, "fatal: boom")
		os.Exit(3)
	default:
		emit(`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"Hel"}}`)
		emit(`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"lo"}}`)
	}
	emit(`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"done"}],"stopReason":"stop","usage":{"input":10,"output":2,"cost":{"total":0.01}}}}`)
	emit(`{"type":"agent_end","messages":[],"willRetry":false}`)
	emit(`{"type":"agent_settled"}`)
}

func start(t *testing.T, mode string) *Client {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	c, err := Start(context.Background(), Options{Binary: exe, Env: append(os.Environ(), "PIRPC_FAKE="+mode)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

// deltas runs a prompt and returns the reconstructed text and the event types.
func deltas(t *testing.T, c *Client) (string, []string) {
	t.Helper()
	var text strings.Builder
	var types []string
	err := c.PromptAndWait(ctx(t), PromptParams{Message: "hi"}, func(e Event) {
		types = append(types, e.Type)
		if e.Type == EventMessageUpdate {
			var u MessageUpdate
			if err := e.Decode(&u); err != nil {
				t.Errorf("decode: %v", err)
			}
			if u.AssistantMessageEvent.Type == "text_delta" {
				text.WriteString(u.AssistantMessageEvent.Delta)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return text.String(), types
}

// RPC-01: U+2028 and U+2029 inside JSON strings are not record separators.
func TestUnicodeSeparatorsDoNotSplitRecords(t *testing.T) {
	text, _ := deltas(t, start(t, "u2028"))
	if text != "a b c" {
		t.Fatalf("text = %q", text)
	}
}

// RPC-02: a record larger than 1 MB is read whole.
func TestLargeRecord(t *testing.T) {
	text, _ := deltas(t, start(t, "big"))
	if len(text) != 1_500_000 {
		t.Fatalf("len = %d", len(text))
	}
}

// RPC-03: responses are matched by id, not by order.
func TestResponsesCorrelatedByID(t *testing.T) {
	c := start(t, "reorder")
	var wg sync.WaitGroup
	got := map[string]string{}
	var mu sync.Mutex
	for _, typ := range []string{"get_state", "get_session_stats"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := c.Command(ctx(t), typ, nil)
			if err != nil {
				t.Error(err)
				return
			}
			var r struct {
				For string `json:"for"`
			}
			_ = json.Unmarshal(data, &r)
			mu.Lock()
			got[typ] = r.For
			mu.Unlock()
		}()
		time.Sleep(50 * time.Millisecond) // the first command is held until the second arrives
	}
	wg.Wait()
	if got["get_state"] != "c1" || got["get_session_stats"] != "c2" {
		t.Fatalf("got %v", got)
	}
}

// RPC-04: PromptAndWait returns after agent_settled, not after the first agent_end.
func TestPromptWaitsForSettled(t *testing.T) {
	text, types := deltas(t, start(t, "retry"))
	if text != "ok" {
		t.Fatalf("text = %q", text)
	}
	if types[len(types)-1] != EventAgentSettled {
		t.Fatalf("last event %s", types[len(types)-1])
	}
	ends := 0
	for _, ty := range types {
		if ty == EventAgentEnd {
			ends++
		}
	}
	if ends != 2 {
		t.Fatalf("agent_end seen %d times: %v", ends, types)
	}
}

// RPC-05: an unknown event type reaches the caller with its raw record.
func TestUnknownEventPassesThrough(t *testing.T) {
	_, types := deltas(t, start(t, "unknown"))
	found := false
	for _, ty := range types {
		found = found || ty == "brand_new_event"
	}
	if !found {
		t.Fatalf("events %v", types)
	}
}

// RPC-06: an exit in the middle of a run reports the exit code and the stderr tail.
func TestCrashReportsExitCodeAndStderr(t *testing.T) {
	c := start(t, "crash")
	err := c.PromptAndWait(ctx(t), PromptParams{Message: "hi"}, nil)
	var ee *ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("err = %v", err)
	}
	if ee.Code != 3 || !strings.Contains(ee.StderrTail, "boom") {
		t.Fatalf("exit %+v", ee)
	}
	if _, err := c.GetSessionStats(ctx(t)); err == nil {
		t.Fatal("command after exit succeeded")
	}
}

func TestTypedCommands(t *testing.T) {
	c := start(t, "basic")
	m, err := c.SetModel(ctx(t), "hmr-1", "deepseek-v4-pro")
	if err != nil || m.ID != "deepseek-v4-pro" || !m.Reasoning {
		t.Fatalf("model %+v %v", m, err)
	}
	levels, err := c.GetAvailableThinkingLevels(ctx(t))
	if err != nil || strings.Join(levels, ",") != "off,high,xhigh" {
		t.Fatalf("levels %v %v", levels, err)
	}
	st, err := c.GetSessionStats(ctx(t))
	if err != nil || st.Tokens.Input != 1200 || st.Tokens.CacheRead != 800 || st.Cost != 0.0019 {
		t.Fatalf("stats %+v %v", st, err)
	}
	cr, err := c.Compact(ctx(t), "")
	if err != nil || cr.TokensBefore != 180000 {
		t.Fatalf("compact %+v %v", cr, err)
	}
	if err := c.SwitchSession(ctx(t), "/work/s.jsonl"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetAutoRetry(ctx(t), true); err != nil {
		t.Fatal(err)
	}
	_, err = c.Command(ctx(t), "bogus", nil)
	var ce *CommandError
	if !errors.As(err, &ce) || ce.Message != "Unknown command" {
		t.Fatalf("err = %v", err)
	}
}

func TestMessageEndDecoding(t *testing.T) {
	var got Message
	c := start(t, "basic")
	err := c.PromptAndWait(ctx(t), PromptParams{Message: "hi"}, func(e Event) {
		if e.Type == EventMessageEnd {
			var me MessageEnd
			_ = e.Decode(&me)
			got = me.Message
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Text() != "done" || got.StopReason != "stop" || got.Usage == nil || got.Usage.Cost.Total != 0.01 {
		t.Fatalf("message %+v", got)
	}
}

// RPC-08: the package does not depend on Hammurapi's internal packages.
func TestNoInternalImports(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Skipf("go list: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		if strings.Contains(dep, "/hammurapi-core/internal/") {
			t.Fatalf("pkg/pirpc depends on %s", dep)
		}
	}
}
