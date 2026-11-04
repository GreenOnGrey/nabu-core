package services

import (
	"errors"
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
)

func checkCode(t *testing.T, err error) (string, string) {
	t.Helper()
	var ae *apperr.Error
	if !errors.As(err, &ae) {
		t.Fatalf("not an apperr: %v", err)
	}
	field, _ := ae.Details["field"].(string)
	return ae.Code, field
}

// CHK-10: a run without a check block is valid for any agent.
func TestCHK10NoCheck(t *testing.T) {
	if err := validateCheck(nil, WorkspaceNone); err != nil {
		t.Fatal(err)
	}
}

// CHK-27: a check needs a sandbox of Nabu.
func TestCHK27NoWorkspace(t *testing.T) {
	code, _ := checkCode(t, validateCheck(&RunCheck{Command: "make"}, WorkspaceNone))
	if code != "check_requires_workspace" {
		t.Fatal(code)
	}
}

func TestValidateCheck(t *testing.T) {
	for _, c := range []struct {
		in    RunCheck
		field string
	}{
		{RunCheck{Command: " "}, "check.command"},
		{RunCheck{Command: strings.Repeat("x", 2001)}, "check.command"},
		{RunCheck{Command: "make", TimeoutSec: 5}, "check.timeoutSec"},
		{RunCheck{Command: "make", TimeoutSec: 1801}, "check.timeoutSec"},
		{RunCheck{Command: "make", MaxRepairs: 6}, "check.maxRepairs"},
		{RunCheck{Command: "make", MaxRepairs: -1}, "check.maxRepairs"},
	} {
		code, field := checkCode(t, validateCheck(&c.in, WorkspaceNabu))
		if code != "invalid_check" || field != c.field {
			t.Fatalf("%+v: %s %s", c.in, code, field)
		}
	}
	if err := validateCheck(&RunCheck{Command: "make", TimeoutSec: 600, MaxRepairs: 5}, WorkspaceNabu); err != nil {
		t.Fatal(err)
	}
	if (RunCheck{}).Timeout() != checkDefaultTimeout {
		t.Fatal("the default timeout")
	}
}
