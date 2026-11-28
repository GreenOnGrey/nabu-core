package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// AUD-07 (FTR.NAB.CMN-0001 R28): a failed tool of the collector names the class of the failure — the text of the
// source stays out (R30), but an empty error says nothing.
func TestAuditToolError(t *testing.T) {
	if got := auditToolError(agent.ErrUnavailable); got == nil || *got != string(agent.ErrUnavailable) {
		t.Fatalf("class of the failure: %v", got)
	}
	if got := auditToolError(""); got == nil || *got != "tool_error" {
		t.Fatalf("a failure without a class: %v", got)
	}
}

// AUD-07 (FTR.NAB.CMN-0001 R28): a refusal of a tool of the collector goes to the audit with its stable code; the
// class of a failure follows, then the kind of a failure that never reached a server, then tool_error.
func TestToolResultError(t *testing.T) {
	cases := []struct {
		ev   agent.Event
		want string
	}{
		{agent.Event{ErrorClass: agent.ErrTimeout, Summary: "422 invalid_same_as: x"}, "invalid_same_as"},
		{agent.Event{ErrorClass: agent.ErrTimeout, Summary: "the tool failed"}, "timeout"},
		{agent.Event{Summary: "422 invalid_same_as: x"}, "invalid_same_as"},
		{agent.Event{Summary: "commitment_basis_not_in_batch: the message 1 is not in the batch"}, "commitment_basis_not_in_batch"},
		{agent.Event{Summary: "Connection closed"}, "tool_transport"},
		{agent.Event{Summary: "MCP error -32000: Connection closed"}, "mcp_error_-32000"},
		{agent.Event{Summary: "context deadline exceeded"}, "tool_timeout"},
		{agent.Event{Summary: "context canceled"}, "tool_canceled"},
		{agent.Event{Summary: ""}, "tool_transport"},
		{agent.Event{Summary: "the tool failed"}, "tool_error"},
	}
	for _, tc := range cases {
		got := toolResultError(tc.ev)
		if got == nil || *got != tc.want {
			t.Fatalf("%q: %v, want %q", tc.ev.Summary, got, tc.want)
		}
	}
}

// AUD-08 (FTR.NAB.CMN-0003 R30): the audit of a call of the collector keeps the identifiers of the object and
// drops the arguments that carry text.
func TestCollectorArgSummary(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`{"id":"t1","limit":100}`, `{"id":"t1","limit":"100"}`},                                                                  // a VK mail thread
		{`{"threadId":"t1","messageFormat":"FULL_CONTENT"}`, `{"messageFormat":"<12 chars>","threadId":"t1"}`},                    // a Gmail thread
		{`{"chat_sn":"c1","limit":20,"cursor":"eyJmcm9tX21zZ19pZCI6MX0"}`, `{"chat_sn":"c1","cursor":"<23 chars>","limit":"20"}`}, // a VK chat
		{`{"query":"Qwerty!2345","from":"a@x.org"}`, `{"from":"<7 chars>","query":"<11 chars>"}`},                                 // a search: no text
		{`{"title":"Перенос","summary":"Согласовали"}`, `{"summary":"<11 chars>","title":"<7 chars>"}`},                           // a note: no text
		{`{"threadKey":"gmail:t1"}`, `{"threadKey":"gmail:t1"}`},
		{`{"id":""}`, ``},
		{`{"id":"a\nb"}`, ``},
		{`{"id":"` + strings.Repeat("a", 201) + `"}`, ``},
		{`{"chatId":"c1"}`, `{"chatId":"c1"}`},
		{`{"threadID":"t1"}`, `{"threadID":"<2 chars>"}`},                                              // not the suffix the decision names
		{`{"id":"` + strings.Repeat("a", 200) + `","chat_sn":"` + strings.Repeat("b", 200) + `"}`, ``}, // above collectorArgsMax
		{`not json`, ``},
		// AUD-09: the arguments of a call that came wrapped in a JSON string: an object in a string named no
		// identifiers, and the row of a read call stayed empty although the server gave the id of the thread.
		{`"{\"id\":\"t1\",\"limit\":100}"`, `{"id":"t1","limit":"100"}`},
		{`"{\"summary\":\"Согласовали\"}"`, `{"summary":"<11 chars>"}`},
		{`""`, ``},
		{`"not json"`, ``},
	} {
		if got := collectorArgSummary(json.RawMessage(tc.in)); got != tc.want {
			t.Fatalf("%s: got %q, want %q", tc.in, got, tc.want)
		}
	}
}

// AUD-09 (FTR.NAB.CMN-0003 R30): the identifiers of a call reach the row of its result. Pi names a call with an
// id; a stream without ids is paired by the name of the tool while exactly one call is outstanding, and several
// id-less calls of one tool keep no identifiers at all — the id of another chat is worse than none.
func TestCollectorCallArgs(t *testing.T) {
	call := func(id, name, args string) agent.Event {
		return agent.Event{Type: agent.EventToolCall, ID: id, Name: name, Args: json.RawMessage(args)}
	}
	result := func(id, name string) agent.Event {
		return agent.Event{Type: agent.EventToolResult, ID: id, Name: name}
	}

	var a callArgs
	a.add(call("c1", "mcp__vk__mail_read_thread", `{"id":"t1","limit":100}`))
	if got := a.take(result("c1", "mcp__vk__mail_read_thread")); got != `{"id":"t1","limit":"100"}` {
		t.Fatalf("by id: %q", got)
	}
	if got := a.take(result("c1", "mcp__vk__mail_read_thread")); got != "" {
		t.Fatalf("the id is used once: %q", got)
	}

	// a stream without ids: one call of the tool is outstanding — the id is known
	a.add(call("", "mcp__vk__mail_read_thread", `{"id":"t2"}`))
	if got := a.take(result("", "mcp__vk__mail_read_thread")); got != `{"id":"t2"}` {
		t.Fatalf("id-less: %q", got)
	}

	// several id-less calls of one tool: the pairing is unknown
	a.add(call("", "mcp__vk__messenger_list_messages", `{"chat_sn":"c1"}`))
	a.add(call("", "mcp__vk__messenger_list_messages", `{"chat_sn":"c2"}`))
	if got := a.take(result("", "mcp__vk__messenger_list_messages")); got != "" {
		t.Fatalf("ambiguous: %q", got)
	}
	if got := a.take(result("", "mcp__vk__messenger_list_messages")); got != "" {
		t.Fatalf("ambiguous, second: %q", got)
	}

	// a call without identifiers (a note, a search) and a call of another tool are not mixed up
	a.add(call("", "mcp__nabu__comm_note_save", `{"summary":"Согласовали"}`))
	a.add(call("", "mcp__vk__mail_read_thread", `{"id":"t3"}`))
	if got := a.take(result("", "mcp__nabu__comm_note_save")); got != `{"summary":"<11 chars>"}` {
		t.Fatalf("a note: %q", got)
	}
	if got := a.take(result("", "mcp__vk__mail_read_thread")); got != `{"id":"t3"}` {
		t.Fatalf("other tool: %q", got)
	}
}

// RATE-03 (FTR.NAB.ADM-0001 tech §4.3): the tick waits for a source that named the wait of its limit, at most
// twice in one call and only while the run has more time than the wait and the margin for its work.
func TestRateLimitWait(t *testing.T) {
	e := &Engine{}
	limited := &catalog.RateLimited{After: 4 * time.Minute}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if d, ok := e.rateLimitWait(ctx, limited, 0); !ok || d != 4*time.Minute {
		t.Fatalf("the wait of the server: %v %v", d, ok)
	}
	if d, ok := e.rateLimitWait(ctx, limited, maxRateLimitWaits); ok {
		t.Fatalf("more waits than allowed: %v %v", d, ok)
	}

	// the run has less time than the wait and the margin: the next tick repeats it
	tight, cancel := context.WithTimeout(context.Background(), 4*time.Minute+30*time.Second)
	defer cancel()
	if _, ok := e.rateLimitWait(tight, limited, 0); ok {
		t.Fatal("a wait that leaves no time for the work")
	}

	// a source that named no wait is not waited for
	if _, ok := e.rateLimitWait(ctx, catalog.ErrSourceRateLimited, 0); ok {
		t.Fatal("a limit without a wait")
	}
	// the wait is capped: a server that asks for an hour gets the next tick
	if d, ok := e.rateLimitWait(ctx, &catalog.RateLimited{After: time.Hour}, 0); !ok || d != maxRateLimitWait {
		t.Fatalf("capped: %v %v", d, ok)
	}
}
