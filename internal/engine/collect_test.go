package engine

import (
	"encoding/json"
	"strings"
	"testing"

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

// AUD-08 (FTR.NAB.CMN-0003 R30): the audit of a call of the collector keeps the identifiers of the object and
// drops the arguments that carry text.
func TestCollectorArgSummary(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`{"id":"t1","limit":100}`, `{"id":"t1"}`},                                             // a VK mail thread
		{`{"threadId":"t1","messageFormat":"FULL_CONTENT"}`, `{"threadId":"t1"}`},              // a Gmail thread
		{`{"chat_sn":"c1","limit":20,"cursor":"eyJmcm9tX21zZ19pZCI6MX0"}`, `{"chat_sn":"c1"}`}, // a VK chat
		{`{"query":"Qwerty!2345","from":"a@x.org"}`, ``},                                       // a search: the text of a user
		{`{"title":"Перенос","summary":"Согласовали"}`, ``},                                    // a note: the text of messages
		{`{"id":""}`, ``},
		{`{"id":"a\nb"}`, ``},
		{`{"id":"` + strings.Repeat("a", 201) + `"}`, ``},
		{`{"chatId":"c1"}`, `{"chatId":"c1"}`},
		{`{"threadID":"t1"}`, ``}, // not the suffix the decision names
		{`{"id":"` + strings.Repeat("a", 200) + `","chat_sn":"` + strings.Repeat("b", 200) + `"}`, ``}, // above collectorArgsMax
		{`not json`, ``},
	} {
		if got := collectorArgSummary(json.RawMessage(tc.in)); got != tc.want {
			t.Fatalf("%s: got %q, want %q", tc.in, got, tc.want)
		}
	}
}
