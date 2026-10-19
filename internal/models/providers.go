package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/models/gigachat"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// listed is a model the provider lists, with what it says of it.
type listed struct {
	ID   string
	Info *providerInfo
}

// errNoList: the provider does not list its models (a 404 on /models).
var errNoList = errors.New("the provider does not list models")

// listError is a refusal of the provider (a wrong key, a wrong address).
type listError struct {
	Status int
	Text   string
}

func (e *listError) Error() string { return fmt.Sprintf("%d: %s", e.Status, e.Text) }

func (s *Service) httpClient() *http.Client {
	if s.HTTP != nil {
		return s.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func (s *Service) getJSON(ctx context.Context, url, key string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Accept", "application/json")
	resp, err := s.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode == http.StatusNotFound {
		return errNoList
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &e)
		msg := e.Error.Message
		if msg == "" {
			msg = e.Message
		}
		if msg == "" {
			msg = strings.TrimSpace(string(raw))
		}
		return &listError{Status: resp.StatusCode, Text: clip(msg, 300)}
	}
	return json.Unmarshal(raw, out)
}

func clip(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

// list asks the provider for its models (tech §5).
func (s *Service) list(ctx context.Context, conn *Connection, key string) ([]listed, error) {
	if conn.Type == TypeGigaChat {
		if s.GigaChat == nil {
			return nil, apperr.Unavailable("gigachat_unavailable", "GigaChat is not available in this deployment")
		}
		ms, err := s.GigaChat.Models(ctx, gigachatConn(conn, key))
		if err != nil {
			var ge *gigachat.Error
			if errors.As(err, &ge) {
				return nil, &listError{Status: ge.Status, Text: ge.Message}
			}
			return nil, err
		}
		out := make([]listed, 0, len(ms))
		for _, m := range ms {
			out = append(out, listed{ID: m.ID, Info: &providerInfo{Embeddings: m.Type == "embedder"}})
		}
		return out, nil
	}
	var models struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := s.getJSON(ctx, conn.BaseURL+"/models", key, &models); err != nil {
		return nil, err
	}
	out := make([]listed, 0, len(models.Data))
	for _, m := range models.Data {
		if id := normalizeID(m.ID); id != "" {
			out = append(out, listed{ID: id})
		}
	}
	if conn.Type == TypeOpenAICompatible {
		if info := s.liteLLMInfo(ctx, conn.BaseURL, key); len(info) > 0 {
			for i := range out {
				out[i].Info = info[out[i].ID]
			}
		}
	}
	return out, nil
}

// liteLLMInfo reads /model/info of LiteLLM: the capabilities and prices of its
// models (MDL-02). Another endpoint answers 404 and gives nothing.
func (s *Service) liteLLMInfo(ctx context.Context, base, key string) map[string]*providerInfo {
	var res struct {
		Data []struct {
			ModelName string `json:"model_name"`
			ModelInfo struct {
				Mode                    string   `json:"mode"`
				MaxInputTokens          int64    `json:"max_input_tokens"`
				MaxOutputTokens         int64    `json:"max_output_tokens"`
				InputCostPerToken       *float64 `json:"input_cost_per_token"`
				OutputCostPerToken      *float64 `json:"output_cost_per_token"`
				CacheReadInputTokenCost *float64 `json:"cache_read_input_token_cost"`
				SupportsVision          *bool    `json:"supports_vision"`
				SupportsFunctionCalling *bool    `json:"supports_function_calling"`
				SupportsReasoning       *bool    `json:"supports_reasoning"`
			} `json:"model_info"`
		} `json:"data"`
	}
	urls := []string{base + "/model/info"}
	if root := strings.TrimSuffix(base, "/v1"); root != base {
		urls = append(urls, root+"/model/info")
	}
	var err error
	for _, u := range urls {
		if err = s.getJSON(ctx, u, key, &res); err == nil {
			break
		}
	}
	if err != nil {
		return nil
	}
	out := map[string]*providerInfo{}
	for _, d := range res.Data {
		mi := d.ModelInfo
		p := &providerInfo{Embeddings: mi.Mode == "embedding", Image: mi.SupportsVision, Tools: mi.SupportsFunctionCalling,
			Reasoning: mi.SupportsReasoning, ContextWindow: mi.MaxInputTokens, MaxTokens: mi.MaxOutputTokens}
		if mi.InputCostPerToken != nil || mi.OutputCostPerToken != nil {
			c := agent.Cost{}
			if mi.InputCostPerToken != nil {
				c.Input = *mi.InputCostPerToken * 1e6
			}
			if mi.OutputCostPerToken != nil {
				c.Output = *mi.OutputCostPerToken * 1e6
			}
			if mi.CacheReadInputTokenCost != nil {
				c.CacheRead = *mi.CacheReadInputTokenCost * 1e6
			}
			p.Cost = &c
		}
		out[d.ModelName] = p
	}
	return out
}

// Discovered is the answer of a discovery: the models the provider lists, or
// the directory of the type when it lists none.
type Discovered struct {
	Listed bool             `json:"listed"`
	Models []agent.ModelDef `json:"models"`
}

// listErr maps a failure of the list to an error of the API (MDL-04).
func listErr(err error) error {
	var le *listError
	if errors.As(err, &le) {
		if le.Status == http.StatusUnauthorized || le.Status == http.StatusForbidden {
			return apperr.Unprocessable("invalid_key", "the provider rejected the key: "+le.Text).With("field", "apiKey")
		}
		return apperr.Unprocessable("models_list_failed", le.Error()).With("reason", le.Error())
	}
	if _, ok := apperr.As(err); ok {
		return err
	}
	return apperr.Unprocessable("models_list_failed", err.Error()).With("reason", err.Error())
}

// discover lists the models of a connection with their capabilities
// (FTR.NAB.CHAT-0001 R7). old are the stored models: their edits and probes stay.
func (s *Service) discover(ctx context.Context, conn *Connection, key string, old []agent.ModelDef) (Discovered, error) {
	byID := map[string]*agent.ModelDef{}
	for i := range old {
		byID[old[i].ID] = &old[i]
	}
	ls, err := s.list(ctx, conn, key)
	if errors.Is(err, errNoList) {
		out := Discovered{Models: []agent.ModelDef{}}
		for _, k := range ofType(conn.Type) {
			out.Models = append(out.Models, build(conn.Type, k.def.ID, nil, byID[k.def.ID]))
		}
		return out, nil
	}
	if err != nil {
		return Discovered{}, listErr(err)
	}
	out := Discovered{Listed: true, Models: make([]agent.ModelDef, 0, len(ls))}
	for _, l := range ls {
		out.Models = append(out.Models, build(conn.Type, l.ID, l.Info, byID[l.ID]))
	}
	sort.SliceStable(out.Models, func(i, j int) bool {
		a, b := out.Models[i], out.Models[j]
		if Usable(a) != Usable(b) {
			return Usable(a)
		}
		return a.Name < b.Name
	})
	return out, nil
}

func gigachatConn(c *Connection, key string) gigachat.Conn {
	return gigachat.Conn{ID: c.ID, AuthKey: key, Scope: c.Options["scope"], AuthURL: c.Options["authUrl"], APIURL: c.BaseURL}
}
