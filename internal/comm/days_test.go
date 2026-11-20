package comm

import (
	"testing"

	"github.com/google/uuid"
)

// FTR.NAB.MEM-0001 MEM-GR-07: of the links between the chosen nodes stay the strongest of each node, then the
// strongest in all; a node with weak links keeps its strongest one.
func TestCapEdges(t *testing.T) {
	hub := uuid.New()
	var edges []Edge
	leaves := make([]uuid.UUID, 12)
	for i := range leaves {
		leaves[i] = uuid.New()
		edges = append(edges, Edge{A: hub, B: leaves[i], Days: 20 - i})
	}
	weak := uuid.New()
	edges = append(edges, Edge{A: leaves[11], B: weak, Days: 1})

	got := capEdges(edges, 8, 0)
	has := func(a, b uuid.UUID) bool {
		for _, e := range got {
			if (e.A == a && e.B == b) || (e.A == b && e.B == a) {
				return true
			}
		}
		return false
	}
	// the hub keeps its 8 strongest; a leaf's only link stays, since it is among the strongest of the leaf
	if !has(hub, leaves[0]) || !has(hub, leaves[11]) || !has(leaves[11], weak) {
		t.Fatalf("%+v", got)
	}
	if n := len(capEdges(edges, 8, 5)); n != 5 {
		t.Fatalf("the total cap: %d", n)
	}
	if n := len(capEdges(edges, 0, 0)); n != len(edges) {
		t.Fatalf("no caps: %d", n)
	}
}
