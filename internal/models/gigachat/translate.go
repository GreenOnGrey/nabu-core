package gigachat

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// The request of OpenAI Chat Completions as Pi sends it.
type oaRequest struct {
	Model               string          `json:"model"`
	Messages            []oaMessage     `json:"messages"`
	Tools               []oaTool        `json:"tools,omitempty"`
	ToolChoice          json.RawMessage `json:"tool_choice,omitempty"`
	Stream              bool            `json:"stream,omitempty"`
	MaxTokens           *int            `json:"max_tokens,omitempty"`
	MaxCompletionTokens *int            `json:"max_completion_tokens,omitempty"`
	Temperature         *float64        `json:"temperature,omitempty"`
	TopP                *float64        `json:"top_p,omitempty"`
}

type oaMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content,omitempty"`
	ToolCalls  []oaToolCall    `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	Name       string          `json:"name,omitempty"`
}

type oaToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type oaTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters,omitempty"`
	} `json:"function"`
}

// The request of GigaChat.
type gcRequest struct {
	Model        string      `json:"model"`
	Messages     []gcMessage `json:"messages"`
	Functions    []gcFunc    `json:"functions,omitempty"`
	FunctionCall any         `json:"function_call,omitempty"`
	Stream       bool        `json:"stream,omitempty"`
	MaxTokens    *int        `json:"max_tokens,omitempty"`
	Temperature  *float64    `json:"temperature,omitempty"`
	TopP         *float64    `json:"top_p,omitempty"`
}

type gcMessage struct {
	Role         string          `json:"role"`
	Content      string          `json:"content"`
	FunctionCall *gcFunctionCall `json:"function_call,omitempty"`
	Name         string          `json:"name,omitempty"`
	Attachments  []string        `json:"attachments,omitempty"`
}

type gcFunctionCall struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"` // an object, not a string as in OpenAI
}

type gcFunc struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

// image is an image of a user message to upload before the request.
type image struct {
	mime string
	data []byte
	msg  int // the index of the GigaChat message it belongs to
}

// contentParts reads the content of an OpenAI message: a string or parts.
func contentParts(raw json.RawMessage) (string, []image, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil, nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil, nil
	}
	var parts []struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		ImageURL struct {
			URL string `json:"url"`
		} `json:"image_url"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", nil, fmt.Errorf("content: %w", err)
	}
	var texts []string
	var imgs []image
	for _, p := range parts {
		switch p.Type {
		case "text":
			texts = append(texts, p.Text)
		case "image_url":
			mime, data, err := dataURL(p.ImageURL.URL)
			if err != nil {
				return "", nil, err
			}
			imgs = append(imgs, image{mime: mime, data: data})
		}
	}
	return strings.Join(texts, "\n"), imgs, nil
}

// dataURL decodes data:<mime>;base64,<data>; Pi sends images inline.
func dataURL(u string) (string, []byte, error) {
	rest, ok := strings.CutPrefix(u, "data:")
	if !ok {
		return "", nil, errors.New("only inline images are supported")
	}
	meta, data, ok := strings.Cut(rest, ",")
	if !ok || !strings.HasSuffix(meta, ";base64") {
		return "", nil, errors.New("an image must be base64")
	}
	b, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return "", nil, fmt.Errorf("image: %w", err)
	}
	return strings.TrimSuffix(meta, ";base64"), b, nil
}

// functionArgs turns the arguments of an OpenAI call (a JSON string) into the
// object GigaChat expects.
func functionArgs(s string) json.RawMessage {
	s = strings.TrimSpace(s)
	if s == "" {
		return json.RawMessage(`{}`)
	}
	var v map[string]any
	if json.Unmarshal([]byte(s), &v) != nil {
		b, _ := json.Marshal(map[string]string{"input": s})
		return b
	}
	return json.RawMessage(s)
}

// functionResult is the content of a function message: GigaChat expects JSON.
func functionResult(s string) string {
	t := strings.TrimSpace(s)
	if json.Valid([]byte(t)) && (strings.HasPrefix(t, "{") || strings.HasPrefix(t, "[")) {
		return t
	}
	b, _ := json.Marshal(map[string]string{"result": s})
	return string(b)
}

// translateRequest turns an OpenAI request into GigaChat's (GC-04). GigaChat
// knows one function call per answer: an answer with several calls becomes a
// chain of a call and its result each.
func translateRequest(in oaRequest) (gcRequest, []image, error) {
	out := gcRequest{Model: in.Model, Stream: in.Stream, MaxTokens: in.MaxTokens, Temperature: in.Temperature, TopP: in.TopP}
	if out.MaxTokens == nil {
		out.MaxTokens = in.MaxCompletionTokens
	}
	var images []image
	var system []string
	calls := map[string]oaToolCall{} // call id → call
	emitted := map[string]bool{}     // calls already written as function_call
	for _, m := range in.Messages {
		text, imgs, err := contentParts(m.Content)
		if err != nil {
			return out, nil, err
		}
		switch m.Role {
		case "system", "developer":
			if strings.TrimSpace(text) != "" {
				system = append(system, text)
			}
		case "user":
			out.Messages = append(out.Messages, gcMessage{Role: "user", Content: text})
			for _, im := range imgs {
				im.msg = len(out.Messages) - 1
				images = append(images, im)
			}
		case "assistant":
			gm := gcMessage{Role: "assistant", Content: text}
			for i, tc := range m.ToolCalls {
				calls[tc.ID] = tc
				if i == 0 {
					gm.FunctionCall = &gcFunctionCall{Name: tc.Function.Name, Arguments: functionArgs(tc.Function.Arguments)}
					emitted[tc.ID] = true
				}
			}
			out.Messages = append(out.Messages, gm)
		case "tool", "function":
			tc, known := calls[m.ToolCallID]
			name := m.Name
			if known {
				name = tc.Function.Name
				if !emitted[tc.ID] {
					out.Messages = append(out.Messages, gcMessage{Role: "assistant",
						FunctionCall: &gcFunctionCall{Name: tc.Function.Name, Arguments: functionArgs(tc.Function.Arguments)}})
					emitted[tc.ID] = true
				}
			}
			out.Messages = append(out.Messages, gcMessage{Role: "function", Name: name, Content: functionResult(text)})
		}
	}
	if len(system) > 0 {
		out.Messages = append([]gcMessage{{Role: "system", Content: strings.Join(system, "\n\n")}}, out.Messages...)
		for i := range images {
			images[i].msg++
		}
	}
	for _, t := range in.Tools {
		params := t.Function.Parameters
		if len(params) == 0 || string(params) == "null" {
			params = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		out.Functions = append(out.Functions, gcFunc{Name: t.Function.Name, Description: t.Function.Description, Parameters: params})
	}
	if len(in.ToolChoice) > 0 && len(out.Functions) > 0 {
		var s string
		var obj struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		}
		switch {
		case json.Unmarshal(in.ToolChoice, &s) == nil:
			if s == "none" || s == "auto" {
				out.FunctionCall = s
			}
		case json.Unmarshal(in.ToolChoice, &obj) == nil && obj.Function.Name != "":
			out.FunctionCall = map[string]string{"name": obj.Function.Name}
		}
	}
	return out, images, nil
}

// The answer of GigaChat.
type gcResponse struct {
	Choices []struct {
		Index        int       `json:"index"`
		Message      gcMessage `json:"message"`
		Delta        gcMessage `json:"delta"`
		FinishReason string    `json:"finish_reason"`
	} `json:"choices"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Usage   *gcUsage `json:"usage,omitempty"`
}

type gcUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	Precached        int64 `json:"precached_prompt_tokens"`
}

func (u *gcUsage) openAI() map[string]any {
	if u == nil {
		return nil
	}
	return map[string]any{"prompt_tokens": u.PromptTokens, "completion_tokens": u.CompletionTokens, "total_tokens": u.TotalTokens,
		"prompt_tokens_details": map[string]int64{"cached_tokens": u.Precached}}
}

func finish(r string) string {
	switch r {
	case "function_call":
		return "tool_calls"
	case "", "stop", "length":
		return r
	case "blacklist":
		return "content_filter"
	}
	return "stop"
}

func callID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "call_" + hex.EncodeToString(b)
}

func toolCall(fc *gcFunctionCall, index bool) map[string]any {
	args := string(fc.Arguments)
	if args == "" || args == "null" {
		args = "{}"
	}
	var s string
	if json.Unmarshal(fc.Arguments, &s) == nil {
		args = s // some answers carry the arguments as a string already
	}
	tc := map[string]any{"id": callID(), "type": "function", "function": map[string]string{"name": fc.Name, "arguments": args}}
	if index {
		tc["index"] = 0
	}
	return tc
}

// translateResponse turns an answer of GigaChat into OpenAI's.
func translateResponse(raw []byte, model string) ([]byte, error) {
	var r gcResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("gigachat answer: %w", err)
	}
	choices := []map[string]any{}
	for _, c := range r.Choices {
		msg := map[string]any{"role": "assistant", "content": c.Message.Content}
		if fc := c.Message.FunctionCall; fc != nil {
			msg["tool_calls"] = []map[string]any{toolCall(fc, false)}
		}
		choices = append(choices, map[string]any{"index": c.Index, "message": msg, "finish_reason": finish(c.FinishReason)})
	}
	if r.Model != "" {
		model = r.Model
	}
	out := map[string]any{"id": "chatcmpl-" + callID()[5:], "object": "chat.completion", "created": created(r.Created), "model": model, "choices": choices}
	if u := r.Usage.openAI(); u != nil {
		out["usage"] = u
	}
	return json.Marshal(out)
}

func created(t int64) int64 {
	if t == 0 {
		return time.Now().Unix()
	}
	return t
}

// translateStream copies the SSE stream of GigaChat as OpenAI chunks (GC-05).
// A function call comes whole in one delta and leaves as one tool call.
func translateStream(r io.Reader, w io.Writer, flush func(), model string) error {
	id := "chatcmpl-" + callID()[5:]
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 8<<20)
	write := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return err
		}
		flush()
		return nil
	}
	var usage map[string]any
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			break
		}
		var r gcResponse
		if err := json.Unmarshal([]byte(data), &r); err != nil {
			continue
		}
		if r.Model != "" {
			model = r.Model
		}
		if u := r.Usage.openAI(); u != nil {
			usage = u
		}
		for _, c := range r.Choices {
			delta := map[string]any{}
			if c.Delta.Role != "" {
				delta["role"] = "assistant"
			}
			if c.Delta.Content != "" {
				delta["content"] = c.Delta.Content
			}
			if fc := c.Delta.FunctionCall; fc != nil {
				delta["tool_calls"] = []map[string]any{toolCall(fc, true)}
			}
			ch := map[string]any{"index": c.Index, "delta": delta, "finish_reason": nil}
			if c.FinishReason != "" {
				ch["finish_reason"] = finish(c.FinishReason)
			}
			if err := write(map[string]any{"id": id, "object": "chat.completion.chunk", "created": created(r.Created), "model": model,
				"choices": []any{ch}}); err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if usage != nil {
		if err := write(map[string]any{"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
			"choices": []any{}, "usage": usage}); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "data: [DONE]\n\n")
	flush()
	return err
}
