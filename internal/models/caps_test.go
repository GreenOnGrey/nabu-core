package models

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// MDL-15: the directory is consistent.
func TestDirectory(t *testing.T) {
	ids := map[string]bool{}
	for _, k := range directory {
		key := k.typ + "/" + strings.ToLower(k.def.ID)
		if ids[key] {
			t.Errorf("duplicate %s", key)
		}
		ids[key] = true
		if k.def.ContextWindow <= 0 || k.def.Name == "" {
			t.Errorf("%s: no window or name", k.def.ID)
		}
		for _, a := range k.def.Aliases {
			if strings.EqualFold(a, k.def.ID) {
				t.Errorf("%s: alias equals id", k.def.ID)
			}
		}
		if k.def.Embeddings && k.def.Tools {
			t.Errorf("%s: a model of embeddings with tools", k.def.ID)
		}
	}
}

// MDL-01, R12: DeepSeek V4.1 Flash sees images, also under its old name.
func TestBuildFromDirectory(t *testing.T) {
	for _, id := range []string{"deepseek-v4.1-flash", "deepseek-v4-flash"} {
		d := build(TypeDeepSeek, id, nil, nil)
		if d.ID != id || !d.HasInput("image") || !d.Tools || d.Sources[FImage] != SrcCatalog || d.Compat["thinkingFormat"] != "deepseek" {
			t.Fatalf("%s: %+v", id, d)
		}
		if !d.SupportsThinking("high") {
			t.Fatalf("%s: no reasoning levels", id)
		}
	}
	pro := build(TypeDeepSeek, "deepseek-v4-pro", nil, nil)
	if pro.HasInput("image") {
		t.Fatal("V4 Pro does not see images")
	}
	unknown := build(TypeOpenAICompatible, "acme-7b", nil, nil)
	if unknown.Sources[FImage] != SrcDefault || unknown.HasInput("image") || !unknown.Tools || unknown.ContextWindow != 128_000 {
		t.Fatalf("defaults: %+v", unknown)
	}
	// a LiteLLM name with a provider prefix finds the directory
	if d := build(TypeOpenAICompatible, "deepseek/deepseek-v4-flash", nil, nil); !d.HasInput("image") {
		t.Fatal("deepseek/deepseek-v4-flash")
	}
	if e := build(TypeGigaChat, "EmbeddingsGigaR", nil, nil); Usable(e) || !e.Embeddings {
		t.Fatalf("embeddings: %+v", e)
	}
	if g := build(TypeGigaChat, "GigaChat-2-Max", nil, nil); g.Reasoning || g.Compat["maxTokensField"] != "max_tokens" {
		t.Fatalf("gigachat: %+v", g)
	}
	if z := build(TypeZAI, "glm-4.6v", nil, nil); !z.HasInput("image") || z.Compat["thinkingFormat"] != "zai" {
		t.Fatalf("zai: %+v", z)
	}
}

// R7: provider over directory, manual over provider; MDL-05: a refresh keeps
// the manual fields; reset returns a field to its source.
func TestSourcesAndEdits(t *testing.T) {
	no, yes := false, true
	info := &providerInfo{Image: &no, ContextWindow: 64_000}
	d := build(TypeDeepSeek, "deepseek-v4.1-flash", info, nil)
	if d.HasInput("image") || d.Sources[FImage] != SrcProvider || d.ContextWindow != 64_000 || d.Sources[FMaxTokens] != SrcCatalog {
		t.Fatalf("provider over directory: %+v", d)
	}
	var mt int64 = 1000
	d = edit(TypeDeepSeek, d, &CapsPatch{Image: &yes, MaxTokens: &mt})
	if !d.HasInput("image") || d.Sources[FImage] != SrcManual || d.MaxTokens != 1000 || len(d.Overrides) != 2 {
		t.Fatalf("manual: %+v", d)
	}
	// a refresh with the provider saying the same as before
	r := build(TypeDeepSeek, d.ID, info, &d)
	if !r.HasInput("image") || r.MaxTokens != 1000 || r.Sources[FImage] != SrcManual || r.ContextWindow != 64_000 {
		t.Fatalf("refresh keeps manual: %+v", r)
	}
	// without a fresh info the provider values stay
	k := build(TypeDeepSeek, d.ID, nil, &d)
	if k.ContextWindow != 64_000 || k.Sources[FContextWindow] != SrcProvider {
		t.Fatalf("stored provider values: %+v", k)
	}
	back := edit(TypeDeepSeek, r, &CapsPatch{Reset: []string{FMaxTokens}})
	if back.MaxTokens != 384_000 || back.Sources[FMaxTokens] != SrcCatalog || !back.HasInput("image") {
		t.Fatalf("reset: %+v", back)
	}
	// a probe applies only over the defaults
	p := build(TypeOpenAICompatible, "acme", nil, nil)
	p.Sources[FTools] = SrcProbe
	if b := build(TypeOpenAICompatible, "acme", nil, &p); b.Sources[FTools] != SrcProbe {
		t.Fatalf("probe kept: %+v", b.Sources)
	}
}

func TestModelRefJSON(t *testing.T) {
	var in Input
	if err := jsonUnmarshal(`{"models":["a",{"id":"b","overrides":{"image":true}}]}`, &in); err != nil {
		t.Fatal(err)
	}
	if len(in.Models) != 2 || in.Models[0].ID != "a" || in.Models[1].Overrides == nil || !*in.Models[1].Overrides.Image {
		t.Fatalf("%+v", in.Models)
	}
}

// MDL-02, MDL-03, MDL-04: listing a provider.
func TestDiscover(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/litellm/v1/models", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk-good-key" {
			w.WriteHeader(401)
			_, _ = io.WriteString(w, `{"error":{"message":"invalid key"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"vision-x"},{"id":"embed-x"},{"id":"plain"}]}`)
	})
	mux.HandleFunc("/litellm/model/info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"model_name":"vision-x","model_info":{"mode":"chat","max_input_tokens":200000,"max_output_tokens":8000,
			"input_cost_per_token":0.000001,"output_cost_per_token":0.000002,"supports_vision":true,"supports_function_calling":true}},
			{"model_name":"embed-x","model_info":{"mode":"embedding"}}]}`)
	})
	mux.HandleFunc("/zai/models", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	s := &Service{}
	ctx := context.Background()
	d, err := s.discover(ctx, &Connection{ID: uuid.New(), Type: TypeOpenAICompatible, BaseURL: srv.URL + "/litellm/v1"}, "sk-good-key", nil)
	if err != nil || !d.Listed || len(d.Models) != 3 {
		t.Fatalf("%+v %v", d, err)
	}
	by := map[string]agent.ModelDef{}
	for _, m := range d.Models {
		by[m.ID] = m
	}
	v := by["vision-x"]
	if !v.HasInput("image") || v.ContextWindow != 200_000 || v.Cost.Input != 1 || v.Sources[FImage] != SrcProvider {
		t.Fatalf("MDL-02: %+v", v)
	}
	if Usable(by["embed-x"]) || !Usable(by["plain"]) {
		t.Fatal("embeddings are not usable by an agent")
	}
	// MDL-03: no list — the directory of the type
	z, err := s.discover(ctx, &Connection{ID: uuid.New(), Type: TypeZAI, BaseURL: srv.URL + "/zai"}, "k", nil)
	if err != nil || z.Listed || len(z.Models) != len(ofType(TypeZAI)) {
		t.Fatalf("MDL-03: %+v %v", z, err)
	}
	// MDL-04
	_, err = s.discover(ctx, &Connection{ID: uuid.New(), Type: TypeOpenAICompatible, BaseURL: srv.URL + "/litellm/v1"}, "sk-bad", nil)
	var ae *apperr.Error
	if !errors.As(err, &ae) || ae.Code != "invalid_key" {
		t.Fatalf("MDL-04: %v", err)
	}
}

func jsonUnmarshal(s string, v any) error { return json.Unmarshal([]byte(s), v) }
