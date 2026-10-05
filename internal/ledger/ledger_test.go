package ledger

import "testing"

// AUD-02: secrets of the session are masked in the audit.
func TestMask(t *testing.T) {
	got := Mask(`{"query":"select 1","token":"sk-very-secret-123"}`, []string{"sk-very-secret-123", "abc"}, 500)
	if got != `{"query":"select 1","token":"***"}` {
		t.Fatal(got)
	}
	long := Mask(string(make([]byte, 600)), nil, 500)
	if len([]rune(long)) != 501 {
		t.Fatalf("clip: %d", len([]rune(long)))
	}
}

func TestSplitTool(t *testing.T) {
	if s, n := SplitTool("mcp__jira__create_issue"); s != "jira" || n != "create_issue" {
		t.Fatal(s, n)
	}
	if s, n := SplitTool("bash"); s != "workspace" || n != "bash" {
		t.Fatal(s, n)
	}
}
