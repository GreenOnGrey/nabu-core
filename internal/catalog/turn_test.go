package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
)

// FTR.NAB.CHAT-0002 NX-04, NX-05: the JWT of a delegated call names the topic
// and the channel of the turn the server put into the context, and nothing
// outside a turn.
func TestDelegationTurnClaims(t *testing.T) {
	signer := jwt.NewSigner("https://nabu.test", []byte("0123456789abcdef0123456789abcdef"))
	s := &Service{Signer: signer}
	it := &Item{Name: "hammurapi", PersonalAuth: &PersonalAuth{Kind: "delegation"}}
	claims := func(ctx context.Context) jwt.Claims {
		h := s.delegationHeaders(ctx, it, "anna@x.org")
		c, err := signer.Verify(strings.TrimPrefix(h["Authorization"], "Bearer "), "hammurapi")
		if err != nil {
			t.Fatal(err)
		}
		if h[DelegationHeader] != "anna@x.org" || c.Email != "anna@x.org" {
			t.Fatalf("delegation: %v %+v", h, c)
		}
		return c
	}
	c := claims(withTurn(context.Background(), "1c9e0000-0000-0000-0000-000000000001", "client:hammurapi"))
	if c.Conversation != "1c9e0000-0000-0000-0000-000000000001" || c.Channel != "client:hammurapi" {
		t.Fatalf("NX-04: %+v", c)
	}
	if c := claims(context.Background()); c.Conversation != "" || c.Channel != "" {
		t.Fatalf("NX-05: %+v", c)
	}
}
