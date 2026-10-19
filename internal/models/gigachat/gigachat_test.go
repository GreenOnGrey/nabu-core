package gigachat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// GC-04: tools become functions, a tool result a function message, several
// calls of one answer a chain, images attachments of their message.
func TestTranslateRequest(t *testing.T) {
	var in oaRequest
	raw := `{"model":"GigaChat-2-Max","stream":true,"max_completion_tokens":100,"messages":[
	 {"role":"system","content":"be brief"},
	 {"role":"developer","content":"use tools"},
	 {"role":"user","content":[{"type":"text","text":"what is here?"},{"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}}]},
	 {"role":"assistant","content":"","tool_calls":[
	   {"id":"a","type":"function","function":{"name":"mail_search","arguments":"{\"q\":\"invoice\"}"}},
	   {"id":"b","type":"function","function":{"name":"memory_save","arguments":"{\"text\":\"x\"}"}}]},
	 {"role":"tool","tool_call_id":"a","content":"3 letters"},
	 {"role":"tool","tool_call_id":"b","content":"{\"ok\":true}"}],
	 "tools":[{"type":"function","function":{"name":"mail_search","description":"search","parameters":{"type":"object"}}},
	          {"type":"function","function":{"name":"memory_save"}}],
	 "tool_choice":"auto"}`
	if err := json.Unmarshal([]byte(raw), &in); err != nil {
		t.Fatal(err)
	}
	out, imgs, err := translateRequest(in)
	if err != nil {
		t.Fatal(err)
	}
	roles := []string{}
	for _, m := range out.Messages {
		roles = append(roles, m.Role)
	}
	if got := strings.Join(roles, ","); got != "system,user,assistant,function,assistant,function" {
		t.Fatalf("roles %s", got)
	}
	if out.Messages[0].Content != "be brief\n\nuse tools" {
		t.Fatalf("system %q", out.Messages[0].Content)
	}
	if fc := out.Messages[2].FunctionCall; fc == nil || fc.Name != "mail_search" || string(fc.Arguments) != `{"q":"invoice"}` {
		t.Fatalf("first call %+v", out.Messages[2])
	}
	if out.Messages[3].Name != "mail_search" || out.Messages[3].Content != `{"result":"3 letters"}` {
		t.Fatalf("result %+v", out.Messages[3])
	}
	if fc := out.Messages[4].FunctionCall; fc == nil || fc.Name != "memory_save" {
		t.Fatalf("second call %+v", out.Messages[4])
	}
	if out.Messages[5].Content != `{"ok":true}` {
		t.Fatalf("json result %q", out.Messages[5].Content)
	}
	if len(imgs) != 1 || imgs[0].msg != 1 || string(imgs[0].data) != "hello" || imgs[0].mime != "image/png" {
		t.Fatalf("images %+v", imgs)
	}
	if len(out.Functions) != 2 || string(out.Functions[1].Parameters) != `{"type":"object","properties":{}}` || out.FunctionCall != "auto" {
		t.Fatalf("functions %+v %v", out.Functions, out.FunctionCall)
	}
	if out.MaxTokens == nil || *out.MaxTokens != 100 || !out.Stream {
		t.Fatalf("options %+v", out)
	}
}

func TestTranslateResponse(t *testing.T) {
	raw := `{"choices":[{"index":0,"message":{"role":"assistant","content":"","function_call":{"name":"mail_search","arguments":{"q":"x"}}},"finish_reason":"function_call"}],
	 "usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13},"model":"GigaChat-2-Max"}`
	b, err := translateResponse([]byte(raw), "m")
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					ID       string
					Function struct{ Name, Arguments string }
				} `json:"tool_calls"`
			}
			FinishReason string `json:"finish_reason"`
		}
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
		}
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	tc := out.Choices[0].Message.ToolCalls
	if len(tc) != 1 || tc[0].Function.Name != "mail_search" || tc[0].Function.Arguments != `{"q":"x"}` || tc[0].ID == "" ||
		out.Choices[0].FinishReason != "tool_calls" || out.Usage.PromptTokens != 10 {
		t.Fatalf("%s", b)
	}
}

// GC-05: the stream keeps the text, turns the function call into one tool
// call and ends with the usage and [DONE].
func TestTranslateStream(t *testing.T) {
	in := "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Ищу\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\",\"function_call\":{\"name\":\"mail_search\",\"arguments\":{\"q\":\"x\"}}},\"finish_reason\":\"function_call\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\n" +
		"data: [DONE]\n\n"
	var w strings.Builder
	if err := translateStream(strings.NewReader(in), &w, func() {}, "m"); err != nil {
		t.Fatal(err)
	}
	out := w.String()
	for _, want := range []string{`"content":"Ищу"`, `"name":"mail_search"`, `"arguments":"{\"q\":\"x\"}"`, `"finish_reason":"tool_calls"`, `"prompt_tokens":5`, "data: [DONE]"} {
		if !strings.Contains(out, want) {
			t.Fatalf("no %s in\n%s", want, out)
		}
	}
	if strings.Count(out, `"tool_calls":[`) != 1 {
		t.Fatalf("one tool call expected:\n%s", out)
	}
}

// fakeGigaChat is the OAuth service and the API of GigaChat.
type fakeGigaChat struct {
	mu        sync.Mutex
	tokens    int
	reject    int // the next chat requests answered 401
	status    int // the chat requests answered with this status
	uploads   []string
	deletes   []string
	chatBody  string
	expiresIn time.Duration
}

func (f *fakeGigaChat) handler(t *testing.T) http.Handler {
	r := chi.NewRouter()
	r.Post("/oauth", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Header.Get("Authorization") != "Basic KEY" || r.Header.Get("RqUID") == "" || r.Form.Get("scope") != "GIGACHAT_API_CORP" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"message":"bad key"}`)
			return
		}
		f.mu.Lock()
		f.tokens++
		n := f.tokens
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok" + string(rune('0'+n)), "expires_at": time.Now().Add(f.expiresIn).UnixMilli()})
	})
	r.Get("/api/models", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"id":"GigaChat-2-Max","type":"chat"},{"id":"EmbeddingsGigaR","type":"embedder"}]}`)
	})
	r.Post("/api/files", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.uploads = append(f.uploads, r.Header.Get("Authorization"))
		f.mu.Unlock()
		_, _ = io.WriteString(w, `{"id":"file-1"}`)
	})
	r.Post("/api/files/{id}/delete", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.deletes = append(f.deletes, chi.URLParam(r, "id"))
		f.mu.Unlock()
		_, _ = io.WriteString(w, `{"deleted":true}`)
	})
	r.Post("/api/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.chatBody = string(b)
		reject := f.reject > 0
		if reject {
			f.reject--
		}
		status := f.status
		f.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"message":"refused"}`)
			return
		}
		if reject {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"message":"token expired"}`)
			return
		}
		_, _ = io.WriteString(w, `{"choices":[{"index":0,"message":{"role":"assistant","content":"красный"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	})
	return r
}

// GC-01, GC-02, GC-03, GC-06, GC-07, GC-08 through the bridge.
func TestBridge(t *testing.T) {
	f := &fakeGigaChat{expiresIn: 30 * time.Minute}
	up := httptest.NewServer(f.handler(t))
	defer up.Close()
	id := uuid.New()
	conn := &Conn{ID: id, AuthKey: "KEY", Scope: "GIGACHAT_API_CORP", AuthURL: up.URL + "/oauth", APIURL: up.URL + "/api"}
	b := &Bridge{Client: &Client{HTTP: up.Client()},
		Lookup: func(_ context.Context, got uuid.UUID) (*Conn, error) {
			if got != id {
				return nil, nil
			}
			return conn, nil
		},
		Verify: func(tok string, c uuid.UUID) bool { return tok == "bridge-"+c.String() },
	}
	r := chi.NewRouter()
	b.Routes(r)
	srv := httptest.NewServer(r)
	defer srv.Close()
	post := func(tok, body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+Prefix+"/"+id.String()+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	msg := `{"model":"GigaChat-2-Max","messages":[{"role":"user","content":[{"type":"text","text":"colour?"},{"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}}]}]}`
	// GC-07
	if code, body := post("wrong", msg); code != http.StatusUnauthorized || !strings.Contains(body, "authentication_error") {
		t.Fatalf("GC-07: %d %s", code, body)
	}
	// GC-01, GC-06
	code, body := post("bridge-"+id.String(), msg)
	if code != 200 || !strings.Contains(body, "красный") {
		t.Fatalf("answer %d %s", code, body)
	}
	if !strings.Contains(f.chatBody, `"attachments":["file-1"]`) || len(f.deletes) != 1 || f.tokens != 1 {
		t.Fatalf("GC-06: body %s deletes %v tokens %d", f.chatBody, f.deletes, f.tokens)
	}
	// GC-02: the cached token is used
	post("bridge-"+id.String(), `{"model":"GigaChat-2-Max","messages":[{"role":"user","content":"hi"}]}`)
	if f.tokens != 1 {
		t.Fatalf("GC-02: tokens %d", f.tokens)
	}
	// GC-03: a 401 gets one retry with a new token
	f.reject = 1
	if code, _ := post("bridge-"+id.String(), `{"model":"GigaChat-2-Max","messages":[{"role":"user","content":"hi"}]}`); code != 200 || f.tokens != 2 {
		t.Fatalf("GC-03: %d tokens %d", code, f.tokens)
	}
	f.reject = 2
	if code, body := post("bridge-"+id.String(), `{"model":"GigaChat-2-Max","messages":[{"role":"user","content":"hi"}]}`); code != 401 || !strings.Contains(body, "401") {
		t.Fatalf("GC-03: a second 401 is an error: %d %s", code, body)
	}
	// a token close to its end is renewed
	cl := &Client{HTTP: up.Client(), now: func() time.Time { return time.Now().Add(29*time.Minute + 30*time.Second) }}
	before := f.tokens
	if _, err := cl.Token(context.Background(), *conn); err != nil || f.tokens != before+1 {
		t.Fatalf("a fresh client takes a token: %v", err)
	}
	if _, err := cl.Token(context.Background(), *conn); err != nil || f.tokens != before+2 {
		t.Fatalf("GC-02: a token expiring within a minute is renewed (%d)", f.tokens-before)
	}
	// GC-08: 402 and 429 keep their status and are classified as the balance and the limits
	for status, class := range map[int]agent.ErrorClass{402: agent.ErrInsufficientBalance, 429: agent.ErrRateLimit} {
		f.mu.Lock()
		f.status = status
		f.mu.Unlock()
		code, body := post("bridge-"+id.String(), `{"model":"GigaChat-2-Max","messages":[{"role":"user","content":"hi"}]}`)
		if c := agent.Classify(strconv.Itoa(code) + " " + body); code != status || c.Class != class {
			t.Fatalf("GC-08 %d: %d %s → %s", status, code, body, c.Class)
		}
	}
	f.mu.Lock()
	f.status = 0
	f.mu.Unlock()
	// the service does not answer: 503, and the text says 503 too
	down := &Bridge{Client: &Client{HTTP: up.Client()}, Verify: b.Verify,
		Lookup: func(context.Context, uuid.UUID) (*Conn, error) {
			c := *conn
			c.APIURL = "http://127.0.0.1:1"
			return &c, nil
		}}
	rd := chi.NewRouter()
	down.Routes(rd)
	sd := httptest.NewServer(rd)
	defer sd.Close()
	req, _ := http.NewRequest(http.MethodPost, sd.URL+Prefix+"/"+id.String()+"/v1/chat/completions", strings.NewReader(`{"model":"GigaChat-2-Max","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer bridge-"+id.String())
	if resp, err := http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	} else {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(raw), `"message":"503`) {
			t.Fatalf("unavailable: %d %s", resp.StatusCode, raw)
		}
	}
	// models
	list, err := b.Client.Models(context.Background(), *conn)
	if err != nil || len(list) != 2 || list[1].Type != "embedder" {
		t.Fatalf("models %+v %v", list, err)
	}
}
