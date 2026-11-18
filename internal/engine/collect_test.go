package engine

import (
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
