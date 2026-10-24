package config

import (
	"strings"
	"testing"
)

// FTR.NAB.CMN-0008 tech §3: profiles are name=image pairs; defaults name
// declared profiles; custom images need prefixes ending with a slash.
func TestSandboxProfilesConfig(t *testing.T) {
	t.Setenv("SANDBOX_PROFILES", "base=ghcr.io/x/nabu-sandbox-base:v1,documents=ghcr.io/x/nabu-sandbox-documents:v1")
	t.Setenv("SANDBOX_DEFAULTS", "personal=documents,group=documents")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.SandboxProfiles["documents"] != "ghcr.io/x/nabu-sandbox-documents:v1" || c.SandboxDefaults["service"] != "base" {
		t.Fatalf("%v %v", c.SandboxProfiles, c.SandboxDefaults)
	}
	if len(c.SandboxPullSecrets) != 1 || c.SandboxPullSecrets[0] != "ghcr-pull" {
		t.Fatalf("pull secrets %v", c.SandboxPullSecrets)
	}
	if errs := c.validateSandboxProfiles(); len(errs) != 0 {
		t.Fatal(errs)
	}
	c.SandboxDefaults["group"] = "missing"
	c.SandboxCustomImages = true
	c.SandboxImagePrefixes = []string{"registry.local/nabu"}
	var msgs []string
	for _, e := range c.validateSandboxProfiles() {
		msgs = append(msgs, e.Error())
	}
	all := strings.Join(msgs, "\n")
	if !strings.Contains(all, "group=missing") || !strings.Contains(all, "must end with /") {
		t.Fatalf("%s", all)
	}
}

func TestPairsErrors(t *testing.T) {
	t.Setenv("X_PAIRS", "a=1,b,a=2")
	p := &parser{}
	m := p.pairs("X_PAIRS")
	if m["a"] != "1" || len(p.errs) != 2 {
		t.Fatalf("%v %v", m, p.errs)
	}
}
