package models

import (
	"context"
	"errors"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// A model of embeddings fails the chat check and passes by /embeddings; a
// model that fails both stays failed.
func TestCheckEmbeddings(t *testing.T) {
	s := &Service{
		Check: func(context.Context, agent.LLMCheckRequest) (*agent.LLMCheckResponse, error) {
			return &agent.LLMCheckResponse{Results: []agent.LLMCheckResult{
				{Model: "chat", OK: true, LatencyMs: 5},
				{Model: "embed", ErrorClass: "auth", HTTPStatus: 403},
				{Model: "broken", ErrorClass: "auth", HTTPStatus: 401},
			}}, nil
		},
		Embed: func(_ context.Context, baseURL, key, model string) error {
			if baseURL != "https://llm.test/v1" || key != "k" {
				t.Errorf("embed called with %q %q", baseURL, key)
			}
			if model == "chat" {
				t.Error("a model that chats must not be asked for a vector")
			}
			if model == "embed" {
				return nil
			}
			return errors.New("embeddings: HTTP 401")
		},
	}
	res, err := s.runCheck(context.Background(), agent.ModelSpec{BaseURL: "https://llm.test/v1"}, "k")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]agent.LLMCheckResult{}
	for _, r := range res.Results {
		got[r.Model] = r
	}
	if r := got["chat"]; !r.OK || r.Embeddings {
		t.Errorf("chat: %+v", r)
	}
	if r := got["embed"]; !r.OK || !r.Embeddings || r.ErrorClass != "" || r.HTTPStatus != 0 {
		t.Errorf("embed: %+v", r)
	}
	if r := got["broken"]; r.OK || r.Embeddings || r.ErrorClass != "auth" {
		t.Errorf("broken: %+v", r)
	}
}
