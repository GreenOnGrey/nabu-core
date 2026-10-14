package agent

import (
	"encoding/json"
	"strings"
	"testing"
)

// FTR.NAB.CMN-0006 CAT-15: only the properties of type string.
func TestStringArgs(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{
		"source":{"type":"string"},"pages":{"type":"integer"},"opt":{"type":["string","null"]},
		"list":{"type":"array","items":{"type":"string"}},"any":{}}}`)
	if got := strings.Join(StringArgs(schema), ","); got != "opt,source" {
		t.Fatalf("got %q", got)
	}
	if StringArgs(nil) != nil || StringArgs(json.RawMessage(`{}`)) != nil {
		t.Fatal("no schema — no args")
	}
}
