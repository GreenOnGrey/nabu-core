package ledger

import "testing"

// ATR-02, MIG-01: the result codes of the audit are the values of the
// constraint audit_result_code_chk; 401/403 are «auth», not «transport_error».
func TestResultCodes(t *testing.T) {
	want := map[string]bool{"ok": true, "tool_error": true, "transport_error": true, "denied": true,
		"file_rejected": true, "file_too_large": true, "auth": true}
	if len(ResultCodes) != len(want) {
		t.Fatalf("codes = %v", ResultCodes)
	}
	for _, c := range ResultCodes {
		if !want[c] {
			t.Fatalf("unexpected code %q", c)
		}
	}
	if ResultAuth != "auth" {
		t.Fatalf("auth = %q", ResultAuth)
	}
}
