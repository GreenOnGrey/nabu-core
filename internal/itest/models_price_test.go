//go:build integration

package itest

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
)

// SEL-06 (FTR.NAB.CHAT-0001 R16): the user sees the price of a million tokens
// of a model, the read cache included; a price Nabu does not know is not sent,
// so a zero is never read as free.
func TestModelPriceForUser(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			_, _ = io.WriteString(w, `{"data":[{"id":"priced"},{"id":"unpriced"}]}`)
		case "/v1/model/info":
			_, _ = io.WriteString(w, `{"data":[{"model_name":"priced","model_info":{"mode":"chat","max_input_tokens":200000,"max_output_tokens":8000,
				"input_cost_per_token":0.000001,"output_cost_per_token":0.000002,"cache_read_input_token_cost":0.0000001,
				"cache_creation_input_token_cost":0.0000015}}]}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	box, _ := crypto.NewBox(key)
	ms := &models.Service{Pool: pool, Box: box}
	c, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Price " + uuid.NewString()[:6],
		BaseURL: srv.URL + "/v1", APIKey: "sk-price-0001", Models: []models.ModelRef{{ID: "priced"}, {ID: "unpriced"}}})
	if err != nil {
		t.Fatal(err)
	}
	// R7: the prices come with the description of the provider, not with the form
	if c, err = ms.Refresh(ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.PutPersonal(ctx, models.PersonalModels{Default: &models.Choice{ConnectionID: c.ID, Model: "priced"},
		Available: []models.Choice{{ConnectionID: c.ID, Model: "unpriced"}}}); err != nil {
		t.Fatal(err)
	}
	avail, err := ms.AvailableForUsers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]models.AvailableModel{}
	for _, m := range avail {
		if m.ConnectionID == c.ID {
			by[m.Model] = m
		}
	}
	p := by["priced"]
	// the provider gives a price per token; the tail of the float is not a defect
	if p.Price == nil || !near(p.Price.Input, 1) || !near(p.Price.Output, 2) || !near(p.Price.CacheRead, 0.1) || !near(p.Price.CacheWrite, 1.5) {
		t.Fatalf("SEL-06: the price of the model: %+v", p.Price)
	}
	if u := by["unpriced"]; u.Price != nil {
		t.Fatalf("SEL-06: an unknown price is not a price: %+v", u.Price)
	}
}

// near compares the prices the provider gave per token, multiplied by 1e6.
func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
