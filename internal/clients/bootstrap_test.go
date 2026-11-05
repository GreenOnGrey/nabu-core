package clients

import (
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/config"
)

const testHash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

// SVC-13 (FTR.NAB.CMN-0001 R17a): a wrong client of BOOTSTRAP_CLIENTS stops api with the variable named.
func TestValidateBootstrap(t *testing.T) {
	good := config.BootstrapClient{Name: "hammurapi", SecretSHA256: testHash, URL: "https://hammurapi.example.com", Agents: []string{"issue-analysis"}}
	if err := ValidateBootstrap([]config.BootstrapClient{good}); err != nil {
		t.Fatal(err)
	}
	with := func(f func(*config.BootstrapClient)) []config.BootstrapClient {
		c := good
		f(&c)
		return []config.BootstrapClient{c}
	}
	for name, list := range map[string][]config.BootstrapClient{
		"upper case":  with(func(c *config.BootstrapClient) { c.Name = "Hammurapi" }),
		"short hash":  with(func(c *config.BootstrapClient) { c.SecretSHA256 = "9f86d08188" }),
		"upper hex":   with(func(c *config.BootstrapClient) { c.SecretSHA256 = strings.ToUpper(testHash) }),
		"no scheme":   with(func(c *config.BootstrapClient) { c.URL = "hammurapi.example.com" }),
		"empty agent": with(func(c *config.BootstrapClient) { c.Agents = []string{""} }),
		"agent twice": with(func(c *config.BootstrapClient) { c.Agents = []string{"a", "a"} }),
		"name twice":  {good, good},
	} {
		if err := ValidateBootstrap(list); err == nil || !strings.Contains(err.Error(), "BOOTSTRAP_CLIENTS") {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
