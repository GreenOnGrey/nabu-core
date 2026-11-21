package chat

import "testing"

// STP-14: a NUL character of a tool result never reaches the database: text,
// steps and the error are cleaned before the write (FTR.NAB.CHAT-0004 R13).
func TestWithoutNUL(t *testing.T) {
	if got := withoutNUL("a\x00b"); got != "ab" {
		t.Fatalf("clean text = %q", got)
	}
	steps := []ToolStep{
		{ID: "1", Server: "mcp\x00", Tool: "read", Summary: "bin\x00ary", Status: "done",
			Subagents: []SubagentTask{{Task: 1, Description: "d\x00", State: "done", Error: "e\x00", Result: "r\x00"}}},
	}
	out := stepsWithoutNUL(steps)
	if out[0].Server != "mcp" || out[0].Summary != "binary" || out[0].Subagents[0].Result != "r" {
		t.Fatalf("cleanSteps = %+v", out)
	}
	if steps[0].Server != "mcp\x00" {
		t.Fatal("stepsWithoutNUL changed the source steps")
	}
	if got := stepsWithoutNUL(nil); got != nil {
		t.Fatal("nil steps stay nil")
	}
}
