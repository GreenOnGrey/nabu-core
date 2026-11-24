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

// FTR.NAB.CHAT-0001 R16: the user sees a price only when Nabu knows it.
func TestPriceOf(t *testing.T) {
	unknown := priceOf(agent.ModelDef{Sources: map[string]string{FCost: SrcDefault}})
	if unknown != nil {
		t.Fatalf("a default price is not a price: %+v", unknown)
	}
	p := priceOf(agent.ModelDef{Cost: agent.Cost{Input: 3, Output: 6, CacheRead: 0.3, CacheWrite: 3.75},
		Sources: map[string]string{FCost: SrcProvider}})
	if p == nil || p.CacheRead != 0.3 || p.CacheWrite != 3.75 {
		t.Fatalf("the price of a model: %+v", p)
	}
}
