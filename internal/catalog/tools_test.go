package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

func doclingItem() *Item {
	return &Item{Name: "docling", Tools: []agent.MCPTool{
		{Name: "convert", StringArgs: []string{"source"}},
		{Name: "export", ReadOnly: true, StringArgs: []string{"document_key"}},
		{Name: "list_cached", ReadOnly: true},
		{Name: "save", StringArgs: []string{"path"}},
	}, FileMaxBytes: DefaultFileMaxBytes}
}

func code(err error) string {
	if e, ok := apperr.As(err); ok {
		return e.Code
	}
	return ""
}

// CAT-01, CAT-02, CAT-04, CAT-07
func TestAllowed(t *testing.T) {
	it := doclingItem()
	it.AllowedTools = []string{"convert", "export", "renamed"}
	for name, want := range map[string]bool{"convert": true, "export": true, "list_cached": false, "renamed": false, "other": false} {
		if got := it.allowed(name); got != want {
			t.Errorf("allowed(%s) = %v", name, got)
		}
	}
	legacy := doclingItem()
	legacy.ReadOnly = true
	if legacy.allowed("convert") || !legacy.allowed("export") {
		t.Error("an item without a list keeps the rule of read-only")
	}
	legacy.ReadOnly = false
	if !legacy.allowed("anything") {
		t.Error("an item without a list and not read-only allows everything")
	}
}

// CAT-08, CAT-09, CAT-10, FIL-12, FIL-13
func TestApplyTools(t *testing.T) {
	it := doclingItem()
	list := []string{"convert", "export"}
	if err := applyTools(it, Input{AllowedTools: &list}); err != nil || it.ReadOnly {
		t.Fatalf("a changing tool makes the item not read-only: %v %v", err, it.ReadOnly)
	}
	ro := []string{"export"}
	if err := applyTools(it, Input{AllowedTools: &ro}); err != nil || !it.ReadOnly {
		t.Fatalf("only reading tools: read-only: %v %v", err, it.ReadOnly)
	}
	bad := []string{"nope"}
	if err := applyTools(it, Input{AllowedTools: &bad}); code(err) != "catalog_tool_unknown" {
		t.Fatalf("unknown tool: %v", err)
	}
	on := true
	it = doclingItem()
	if err := applyTools(it, Input{ReadOnly: &on}); err != nil || strings.Join(it.AllowedTools, ",") != "export,list_cached" {
		t.Fatalf("readOnly without a list chooses the reading tools: %v %v", err, it.AllowedTools)
	}
	list = []string{"convert", "export"}
	fa := []FileArg{{Tool: "convert", Arg: "source"}}
	if err := applyTools(it, Input{AllowedTools: &list, FileArgs: &fa}); err != nil || len(it.FileArgs) != 1 {
		t.Fatalf("file arg: %v %v", err, it.FileArgs)
	}
	for _, f := range [][]FileArg{{{Tool: "save", Arg: "path"}}, {{Tool: "convert", Arg: "nope"}}, {{Tool: "list_cached", Arg: "x"}}} {
		if err := applyTools(it, Input{FileArgs: &f}); code(err) != "catalog_file_arg_invalid" {
			t.Errorf("%v: %v", f, err)
		}
	}
	for _, n := range []int64{0, 1023, 100<<20 + 1} {
		if err := applyTools(it, Input{FileMaxBytes: &n}); code(err) != "catalog_file_max_invalid" {
			t.Errorf("%d: %v", n, err)
		}
	}
	// the file args of a tool no longer allowed are dropped
	only := []string{"export"}
	if err := applyTools(it, Input{AllowedTools: &only}); err != nil || len(it.FileArgs) != 0 {
		t.Fatalf("file args of a removed tool: %v %v", err, it.FileArgs)
	}
}

func toolsAnswer() []byte {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 2, "result": map[string]any{"tools": []any{
		map[string]any{"name": "convert", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{
			"source": map[string]any{"type": "string", "description": "URL or local path", "format": "uri"},
			"other":  map[string]any{"type": "string", "description": "keep"}}}},
		map[string]any{"name": "export", "annotations": map[string]any{"readOnlyHint": true}},
		map[string]any{"name": "list_cached", "annotations": map[string]any{"readOnlyHint": true}},
	}}})
	return b
}

func toolNames(t *testing.T, raw []byte) (names []string, schema map[string]map[string]any) {
	t.Helper()
	var msg struct {
		Result struct {
			Tools []struct {
				Name        string `json:"name"`
				InputSchema struct {
					Properties map[string]map[string]any `json:"properties"`
				} `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatal(err)
	}
	for _, tl := range msg.Result.Tools {
		names = append(names, tl.Name)
		if tl.Name == "convert" {
			schema = tl.InputSchema.Properties
		}
	}
	return names, schema
}

// CAT-01, CAT-12, FIL-09
func TestFilterToolsList(t *testing.T) {
	it := doclingItem()
	it.AllowedTools = []string{"convert", "export"}
	it.FileArgs = []FileArg{{Tool: "convert", Arg: "source"}}
	names, schema := toolNames(t, filterToolsList(toolsAnswer(), it, nil))
	if strings.Join(names, ",") != "convert,export" {
		t.Fatalf("tools: %v", names)
	}
	if schema["source"]["description"] != fileArgDescription || schema["source"]["format"] != nil || schema["other"]["description"] != "keep" {
		t.Fatalf("schema: %v", schema)
	}
	names, _ = toolNames(t, filterToolsList(toolsAnswer(), it, []string{"export", "list_cached"}))
	if strings.Join(names, ",") != "export" {
		t.Fatalf("collector run sees the intersection: %v", names)
	}
	legacy := doclingItem()
	legacy.ReadOnly = true
	names, _ = toolNames(t, filterToolsList(toolsAnswer(), legacy, nil))
	if strings.Join(names, ",") != "export,list_cached" {
		t.Fatalf("an item without a list filters by annotation: %v", names)
	}
}

// FIL-04, FIL-05, FIL-11, FIL-14
func TestRewriteFileArgs(t *testing.T) {
	uid := uuid.New()
	it := doclingItem()
	it.AllowedTools = []string{"convert"}
	it.FileArgs = []FileArg{{Tool: "convert", Arg: "source"}}
	var asked []string
	s := &Service{Files: func(_ context.Context, u uuid.UUID, p string, max int64) (string, int64, error) {
		asked = append(asked, p)
		switch p {
		case "big.pdf":
			return "", 0, ErrFileTooLarge
		case "missing.pdf":
			return "", 0, ErrFileNotFound
		}
		return "http://s3/mcp-files/x/" + p + "?sig", 1, nil
	}}
	args, fr := s.rewriteFileArgs(context.Background(), it, uid, "convert", json.RawMessage(`{"source":"docs/a.pdf","other":"x"}`))
	if fr != nil || !strings.Contains(string(args), `"source":"http://s3/mcp-files/x/docs/a.pdf?sig#nabu-path=docs/a.pdf"`) || !strings.Contains(string(args), `"other":"x"`) {
		t.Fatalf("rewrite: %s %v", args, fr)
	}
	for _, v := range []string{"https://x/a.pdf", "s3://b/k", "file:///etc/passwd", "/etc/passwd", ""} {
		asked = nil
		b, _ := json.Marshal(map[string]string{"source": v})
		if _, fr := s.rewriteFileArgs(context.Background(), it, uid, "convert", b); fr == nil || fr.result != "file_rejected" || len(asked) != 0 {
			t.Errorf("%q: %v %v", v, fr, asked)
		}
	}
	if _, fr := s.rewriteFileArgs(context.Background(), it, uid, "convert", json.RawMessage(`{"source":"big.pdf"}`)); fr == nil || fr.result != "file_too_large" || !strings.Contains(fr.text, "20 MB") {
		t.Errorf("too large: %v", fr)
	}
	if _, fr := s.rewriteFileArgs(context.Background(), it, uid, "convert", json.RawMessage(`{"source":"missing.pdf"}`)); fr == nil || fr.result != "file_rejected" {
		t.Errorf("missing: %v", fr)
	}
	same := json.RawMessage(`{"document_key":"k"}`)
	if args, fr := s.rewriteFileArgs(context.Background(), it, uid, "export", same); fr != nil || string(args) != string(same) {
		t.Errorf("a tool without file args is untouched: %s %v", args, fr)
	}
	if _, fr := s.rewriteFileArgs(context.Background(), it, uuid.Nil, "convert", json.RawMessage(`{"source":"a.pdf"}`)); fr == nil {
		t.Error("no user — no space")
	}
}

// OBS-02
func TestMetricTool(t *testing.T) {
	it := doclingItem()
	if it.metricTool("convert") != "convert" || it.metricTool("made-up") != "other" {
		t.Fatal("tool label")
	}
}

// OBS-01
func TestResultSniffer(t *testing.T) {
	for in, want := range map[string]bool{
		`{"jsonrpc":"2.0","id":3,"result":{"content":[],"isError":true}}`:                        true,
		`{"jsonrpc":"2.0","id":3,"result":{"content":[]}}`:                                       false,
		"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{\"isError\":true}}\n\n": true,
		`{"jsonrpc":"2.0","id":3,"error":{"code":-32602,"message":"bad"}}`:                       true,
	} {
		var s resultSniffer
		s.write([]byte(in))
		if s.isError() != want {
			t.Errorf("%s: %v", in, !want)
		}
	}
}

func TestWithArguments(t *testing.T) {
	out := withArguments([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"convert","arguments":{"source":"a"}}}`), json.RawMessage(`{"source":"b"}`))
	if !strings.Contains(string(out), `"arguments":{"source":"b"}`) || !strings.Contains(string(out), `"name":"convert"`) {
		t.Fatal(string(out))
	}
}

func TestFileErrorsAreDistinct(t *testing.T) {
	if errors.Is(ErrFileTooLarge, ErrFileNotFound) {
		t.Fatal()
	}
}

// OBS-01: a server that refused the credentials is "auth", not a transport error
func TestCallResult(t *testing.T) {
	cases := []struct {
		status int
		isErr  bool
		err    error
		want   string
	}{
		{200, false, nil, "ok"},
		{200, true, nil, "tool_error"},
		{401, false, nil, "auth"},
		{403, false, nil, "auth"},
		{502, false, nil, "transport_error"},
		{statusOf(errors.New("mcp tools/call: HTTP 401")), false, errors.New("mcp tools/call: HTTP 401"), "auth"},
		{statusOf(errors.New("dial tcp: refused")), false, errors.New("dial tcp: refused"), "transport_error"},
	}
	for _, c := range cases {
		if got := callResult(c.status, c.isErr, c.err); got != c.want {
			t.Errorf("%d %v %v: %s", c.status, c.isErr, c.err, got)
		}
	}
}
