package config

import (
	"strings"
	"testing"
)

// SVC-13 (FTR.NAB.CMN-0001 R17a): the first service clients are one JSON array; an error names the variable.
// The rules of a client are checked by clients.ValidateBootstrap.
func TestBootstrapClients(t *testing.T) {
	t.Setenv("BOOTSTRAP_CLIENTS", `[{"name":"hammurapi","secretSha256":"ab","url":"https://hammurapi.example.com","agents":["issue-analysis"]}]`)
	p := &parser{}
	got := p.bootstrapClients("BOOTSTRAP_CLIENTS")
	if len(p.errs) != 0 || len(got) != 1 || got[0].Name != "hammurapi" || got[0].Agents[0] != "issue-analysis" {
		t.Fatalf("%+v %v", got, p.errs)
	}
	for name, v := range map[string]string{
		"not an array": `{"name":"hammurapi"}`,
		"unknown key":  `[{"name":"hammurapi","secret":"x"}]`,
		"text after":   `[{"name":"a"}] [{"name":"b"}]`,
		"broken":       `[{"name":"a"}`,
	} {
		t.Setenv("BOOTSTRAP_CLIENTS", v)
		p := &parser{}
		if p.bootstrapClients("BOOTSTRAP_CLIENTS"); len(p.errs) == 0 || !strings.Contains(p.errs[0].Error(), "BOOTSTRAP_CLIENTS") {
			t.Fatalf("%s: %v", name, p.errs)
		}
	}
	t.Setenv("BOOTSTRAP_CLIENTS", "")
	if got := (&parser{}).bootstrapClients("BOOTSTRAP_CLIENTS"); got != nil {
		t.Fatalf("empty: %+v", got)
	}
}
