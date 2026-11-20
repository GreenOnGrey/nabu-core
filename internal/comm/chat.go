package comm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ChatModel is a chat model of a connection that the worker calls directly for the compaction (FTR.NAB.MEM-0001
// arch A1): one request with the instructions and the input, no tools. The connection speaks the OpenAI chat
// completions protocol, as the connections of Nabu do by default.
type ChatModel struct {
	BaseURL string
	Key     string
	Model   string
	Client  *http.Client
}

// ChatUsage are the tokens of a call.
type ChatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// Complete sends one request and returns the text of the answer. It asks for a JSON object; a server that does not
// know response_format (400) is asked once more without it.
func (m *ChatModel) Complete(ctx context.Context, system, user string) (string, ChatUsage, error) {
	text, usage, status, err := m.call(ctx, system, user, true)
	if status == http.StatusBadRequest && err != nil && strings.Contains(strings.ToLower(err.Error()), "response_format") {
		var again ChatUsage
		text, again, _, err = m.call(ctx, system, user, false)
		usage.PromptTokens += again.PromptTokens
		usage.CompletionTokens += again.CompletionTokens
	}
	return text, usage, err
}

func (m *ChatModel) call(ctx context.Context, system, user string, jsonMode bool) (string, ChatUsage, int, error) {
	req := map[string]any{
		"model":       m.Model,
		"temperature": 0.2,
		"messages":    []map[string]string{{"role": "system", "content": system}, {"role": "user", "content": user}},
	}
	if jsonMode {
		req["response_format"] = map[string]string{"type": "json_object"}
	}
	body, _ := json.Marshal(req)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(m.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", ChatUsage{}, 0, err
	}
	r.Header.Set("Content-Type", "application/json")
	if m.Key != "" {
		r.Header.Set("Authorization", "Bearer "+m.Key)
	}
	client := m.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(r)
	if err != nil {
		return "", ChatUsage{}, 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", ChatUsage{}, resp.StatusCode, err
	}
	if resp.StatusCode/100 != 2 {
		// the start of the body names the cause (a parameter the server does not know, the length of the context)
		cause := strings.TrimSpace(string(raw))
		if r := []rune(cause); len(r) > 200 {
			cause = string(r[:200])
		}
		return "", ChatUsage{}, resp.StatusCode, fmt.Errorf("the model answered HTTP %d: %s", resp.StatusCode, cause)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage ChatUsage `json:"usage"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", ChatUsage{}, resp.StatusCode, fmt.Errorf("the answer of the model is not JSON: %v", err)
	}
	if len(out.Choices) == 0 {
		return "", out.Usage, resp.StatusCode, fmt.Errorf("the model gave no answer")
	}
	return out.Choices[0].Message.Content, out.Usage, resp.StatusCode, nil
}
