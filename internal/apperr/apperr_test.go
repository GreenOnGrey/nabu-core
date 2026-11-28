package apperr

import (
	"errors"
	"fmt"
	"testing"
)

// AUD-07, AUD-10 (FTR.NAB.CMN-0001 R28): a refusal that reaches an agent as
// text keeps a machine-readable code for the audit; the message follows it.
func TestCodeIn(t *testing.T) {
	cases := map[string]string{
		"422 invalid_same_as: sameAs is not open":                          "invalid_same_as",
		"404 note_missing: save the note first":                            "note_missing",
		"commitment_basis_not_in_batch: the message 1 is not in the batch": "commitment_basis_not_in_batch",
		"invalid arguments":                           "",
		"open /work/a.txt: no such file or directory": "",
		"http: server closed":                         "",
		"":                                            "",
		"500: no code before the message":             "",
		"422 InvalidCode: capital letters":            "",
	}
	for text, want := range cases {
		if got := CodeIn(text); got != want {
			t.Errorf("CodeIn(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestCodeOf(t *testing.T) {
	wrapped := fmt.Errorf("tool: %w", Unprocessable("catalog_tool_unknown", "no such tool"))
	if got := CodeOf(wrapped); got != "catalog_tool_unknown" {
		t.Fatalf("CodeOf(*Error) = %q", got)
	}
	if got := CodeOf(errors.New("422 name_taken: an item with this name exists")); got != "name_taken" {
		t.Fatalf("CodeOf(text) = %q", got)
	}
	if got := CodeOf(nil); got != "" {
		t.Fatalf("CodeOf(nil) = %q", got)
	}
}
