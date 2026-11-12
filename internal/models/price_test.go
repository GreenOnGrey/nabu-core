package models

import (
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// FTR.HMR.DEV-0004 qa CKN-01: a price from the default is not a price.
func TestPriceKnown(t *testing.T) {
	cases := []struct {
		name string
		d    *agent.ModelDef
		want bool
	}{
		{"unknown model", nil, true},
		{"default source", &agent.ModelDef{Sources: map[string]string{FCost: SrcDefault}}, false},
		{"catalog source", &agent.ModelDef{Sources: map[string]string{FCost: SrcCatalog}, Cost: agent.Cost{Input: 3}}, true},
		{"manual zero", &agent.ModelDef{Sources: map[string]string{FCost: SrcManual}}, true},
		{"no sources, zero", &agent.ModelDef{}, false},
		{"no sources, a price", &agent.ModelDef{Cost: agent.Cost{Output: 1}}, true},
	}
	for _, c := range cases {
		if got := priceKnown(c.d); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}
