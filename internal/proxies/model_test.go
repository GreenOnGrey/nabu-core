package proxies

import (
	"errors"
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
)

func code(err error) string {
	var e *apperr.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// FTR.HMR.CMN-0007 PX-03, PX-04: masks and addresses inside the network.
func TestValidHost(t *testing.T) {
	for _, c := range []struct {
		h    Host
		want string
	}{
		{Host{Host: "*"}, "wide_mask"},
		{Host{Host: "*.com"}, "wide_mask"},
		{Host{Host: "a.*.com"}, "wide_mask"},
		{Host{Host: "*.*.company.local", Internal: true}, "wide_mask"},
		{Host{Host: "*.company.local", Internal: true}, ""},
		{Host{Host: "*.company.local"}, "private_address"},
		{Host{Host: "10.0.0.5"}, "private_address"},
		{Host{Host: "10.0.0.5", Internal: true}, ""},
		{Host{Host: "nexus.company.local"}, "private_address"},
		{Host{Host: "registry.npmjs.org"}, ""},
		{Host{Host: "registry.npmjs.org", Port: 70000}, "invalid_port"},
		{Host{Host: "bad_host.org"}, "invalid_host"},
	} {
		h := NormalizeHost(c.h)
		if c.h.Port != 0 {
			h.Port = c.h.Port
		}
		if got := code(ValidHost(h, "allow[0]")); got != c.want {
			t.Errorf("%+v: %q, want %q", c.h, got, c.want)
		}
	}
}

// PX-05: the variables of Nabu are reserved in any case.
func TestValidEnv(t *testing.T) {
	for _, n := range []string{"HTTPS_PROXY", "NABU_X", "LD_PRELOAD", "PATH"} {
		if got := code(ValidEnv([]Env{{Name: n}})); got != "reserved_env" {
			t.Errorf("%s: %q", n, got)
		}
	}
	if !Reserved("https_proxy") {
		t.Error("https_proxy is reserved")
	}
	if got := code(ValidEnv([]Env{{Name: "bad-name"}})); got != "invalid_env" {
		t.Errorf("bad-name: %q", got)
	}
	if err := ValidEnv([]Env{{Name: "GOPROXY"}, {Name: "FOO_1"}}); err != nil {
		t.Error(err)
	}
}

// PX-01, PX-02, PX-07: the template of "packages" and private registries.
func TestBuild(t *testing.T) {
	allow, env, err := Build(Spec{Ecosystems: []string{EcoGo, EcoNPM, EcoPyPI}})
	if err != nil {
		t.Fatal(err)
	}
	if len(allow) != 5 || allow[0].String() != "proxy.golang.org:443" {
		t.Fatalf("allow %v", allow)
	}
	if e, _ := findEnv(env, "GOPROXY"); e.Value != "https://proxy.golang.org" {
		t.Fatalf("env %v", env)
	}
	allow, env, err = Build(Spec{Ecosystems: []string{EcoNPM}, Registries: []Registry{
		{Ecosystem: EcoNPM, URL: "https://npm.company.local:8443/repo", Internal: true, Token: "t0k"},
		{Ecosystem: EcoPyPI, URL: "https://pypi.company.local/simple", Internal: true, Token: "u:p"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(allow) != 3 || allow[1].String() != "npm.company.local:8443" || !allow[1].Internal {
		t.Fatalf("allow %v", allow)
	}
	if e, ok := findEnv(env, "NPM_TOKEN"); !ok || !e.Secret || e.Value != "t0k" {
		t.Fatalf("npm token %v", env)
	}
	if e, _ := findEnv(env, "PIP_EXTRA_INDEX_URL"); !e.Secret || e.Value != "https://u:p@pypi.company.local/simple" {
		t.Fatalf("pypi %v", e)
	}
	if _, _, err := Build(Spec{Registries: []Registry{{Ecosystem: EcoGo, URL: "https://go.company.local", Internal: true, Token: "x"}}}); code(err) != "token_unsupported" {
		t.Fatalf("go token: %v", err)
	}
	if _, _, err := Build(Spec{Registries: []Registry{{Ecosystem: EcoNPM, URL: "https://npm.company.local"}}}); code(err) != "private_address" {
		t.Fatalf("internal: %v", err)
	}
}

// EG-03, EG-05: a mask is any subdomain, never the domain itself; ports match.
func TestMatch(t *testing.T) {
	allow := []Host{{Host: "*.company.local", Port: 443}, {Host: "pypi.org", Port: 443}}
	for host, want := range map[string]bool{"a.company.local": true, "company.local": false, "a.company.local.evil.com": false,
		"PyPI.org.": true, "evilpypi.org": false} {
		if _, ok := Match(allow, host, 443); ok != want {
			t.Errorf("%s: %v", host, ok)
		}
	}
	if _, ok := Match(allow, "pypi.org", 80); ok {
		t.Error("the port must match")
	}
}

// PX-20: the proxy earlier by name wins a clash of variables.
func TestMerge(t *testing.T) {
	env, clashes := Merge([]Resolved{{Name: "b", Env: map[string]string{"X": "b"}}, {Name: "a", Env: map[string]string{"X": "a", "Y": "1"}}})
	if env["X"] != "a" || env["Y"] != "1" || len(clashes) != 1 || !strings.Contains(clashes[0], "a over b") {
		t.Fatalf("%v %v", env, clashes)
	}
}
