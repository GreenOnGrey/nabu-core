package agent

import "testing"

// FTR.HMR.DEV-0004 qa CKN-02: an unknown price of one turn makes the total unknown; without the field it stays absent.
func TestUsageCostKnown(t *testing.T) {
	f := false
	var u Usage
	u.Add(Usage{TokensIn: 1, CostUSD: 0.5})
	if u.CostKnown != nil {
		t.Fatalf("known turns: %v", *u.CostKnown)
	}
	u.Add(Usage{TokensIn: 2, CostKnown: &f})
	u.Add(Usage{TokensIn: 3, CostUSD: 1})
	if u.CostKnown == nil || *u.CostKnown || u.TokensIn != 6 || u.CostUSD != 1.5 {
		t.Fatalf("total: %+v", u)
	}
}
