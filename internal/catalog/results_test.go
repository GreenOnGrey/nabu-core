package catalog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// fakeResults is a space with files and uploads of servers.
type fakeResults struct {
	files     map[string]bool
	uploaded  map[string]int64
	finished  map[string]string
	discarded []string
	quotaFull bool
}

func (f *fakeResults) Exists(_ context.Context, _ uuid.UUID, p string) (bool, error) {
	return f.files[p], nil
}
func (f *fakeResults) Upload(_ context.Context, name string) (string, string, error) {
	key := "mcp-files/out/k/" + name
	return key, "http://s3/put/" + key, nil
}
func (f *fakeResults) Finish(_ context.Context, _ uuid.UUID, p, key string, max int64) (int64, error) {
	size, ok := f.uploaded[key]
	switch {
	case !ok:
		return 0, ErrResultMissing
	case size > max:
		return 0, ErrResultTooLarge
	case f.quotaFull:
		return 0, ErrResultQuota
	}
	f.finished[p] = key
	return size, nil
}
func (f *fakeResults) Discard(_ context.Context, key string) { f.discarded = append(f.discarded, key) }

func docgenItem() *Item {
	return &Item{Name: "docgen", Tools: []agent.MCPTool{
		{Name: "render_document", StringArgs: []string{"dest", "markdown", "reference"}, StringListArgs: []string{"images"}},
		{Name: "convert", StringArgs: []string{"dest", "source", "to"}},
	}, AllowedTools: []string{"render_document", "convert"}, FileMaxBytes: DefaultFileMaxBytes, ResultMaxBytes: DefaultResultMaxBytes,
		FileArgs:       []FileArg{{Tool: "convert", Arg: "source"}, {Tool: "render_document", Arg: "images"}},
		ResultArgs:     []FileArg{{Tool: "render_document", Arg: "dest"}, {Tool: "convert", Arg: "dest"}},
		SpaceOnlyTools: []string{"render_document", "convert"}}
}

// FTR.NAB.CHAT-0003 RES-03, RES-04, RES-05: the path of a result and the
// overwrite of an existing file are checked before the server is called.
func TestRewriteResultArgs(t *testing.T) {
	uid := uuid.New()
	it := docgenItem()
	fr := &fakeResults{files: map[string]bool{"reports/q3.pdf": true}, uploaded: map[string]int64{}, finished: map[string]string{}}
	s := &Service{Results: fr}
	for _, bad := range []string{"../x.pdf", "/etc/x", "https://x/y.pdf", "reports/", "a/../../b"} {
		b, _ := json.Marshal(map[string]string{"dest": bad})
		if _, _, r := s.rewriteResultArgs(context.Background(), it, uid, "render_document", b); r == nil || r.result != "result_rejected" {
			t.Errorf("RES-03 %q: %+v", bad, r)
		}
	}
	if _, _, r := s.rewriteResultArgs(context.Background(), it, uid, "render_document", json.RawMessage(`{"dest":"reports/q3.pdf"}`)); r == nil || r.result != "result_exists" {
		t.Fatalf("RES-04: %+v", r)
	}
	args, plans, r := s.rewriteResultArgs(context.Background(), it, uid, "render_document", json.RawMessage(`{"dest":"reports/q3.pdf","overwrite":true,"markdown":"# x"}`))
	if r != nil || len(plans) != 1 || plans[0].Path != "reports/q3.pdf" {
		t.Fatalf("RES-05: %+v %+v", plans, r)
	}
	if !strings.Contains(string(args), `"dest":"http://s3/put/mcp-files/out/k/q3.pdf"`) || strings.Contains(string(args), "overwrite") {
		t.Fatalf("RES-05: overwrite is the proxy's, the server gets a link: %s", args)
	}
}

// RES-06, RES-07, RES-08: the result is moved after a successful call only.
func TestFinishResults(t *testing.T) {
	uid := uuid.New()
	fr := &fakeResults{files: map[string]bool{}, uploaded: map[string]int64{"k1": 412 << 10, "big": 60 << 20}, finished: map[string]string{}}
	s := &Service{Results: fr}
	note, r := s.finishResults(context.Background(), uid, []resultPlan{{Path: "reports/q3.pdf", Key: "k1", Max: DefaultResultMaxBytes}}, false)
	if r != nil || note != "Saved to reports/q3.pdf (412 KB)." || fr.finished["reports/q3.pdf"] != "k1" {
		t.Fatalf("RES-01: %q %+v", note, r)
	}
	if _, r := s.finishResults(context.Background(), uid, []resultPlan{{Path: "a.pdf", Key: "big", Max: DefaultResultMaxBytes}}, false); r == nil || r.result != "result_too_large" {
		t.Fatalf("RES-06: %+v", r)
	}
	if _, r := s.finishResults(context.Background(), uid, []resultPlan{{Path: "a.pdf", Key: "none", Max: DefaultResultMaxBytes}}, false); r == nil || r.result != "result_missing" {
		t.Fatalf("RES-08: %+v", r)
	}
	fr.quotaFull = true
	if _, r := s.finishResults(context.Background(), uid, []resultPlan{{Path: "a.pdf", Key: "k1", Max: DefaultResultMaxBytes}}, false); r == nil || r.result != "quota_exceeded" {
		t.Fatalf("RES-07: %+v", r)
	}
	fr.discarded = nil
	if note, r := s.finishResults(context.Background(), uid, []resultPlan{{Path: "a.pdf", Key: "k1", Max: 1}}, true); note != "" || r != nil || len(fr.discarded) != 1 {
		t.Fatalf("an error of the tool discards the upload: %q %+v %v", note, r, fr.discarded)
	}
}

// RES-01: the agent learns where the file is; a refusal replaces the result.
func TestWithResultNote(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"format\":\"pdf\"}"}],"isError":false}}`)
	out := string(withResultNote(raw, "Saved to reports/q3.pdf (412 KB).", nil))
	if !strings.Contains(out, `"format`) || !strings.Contains(out, "Saved to reports/q3.pdf") {
		t.Fatalf("note: %s", out)
	}
	// a JSON answer with "data:" in a text is not a stream
	uri := []byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"<img src=\"data:image/png;base64,AA\">"}]}}`)
	if out := string(withResultNote(uri, "Saved to a.pdf (1 KB).", nil)); !strings.Contains(out, "Saved to a.pdf") || !strings.Contains(out, "data:image/png") {
		t.Fatalf("R10: a data URI in JSON: %s", out)
	}
	sse := []byte("event: message\ndata: " + string(raw) + "\n\n")
	out = string(withResultNote(sse, "", &fileRefusal{"Not enough space left to save a.pdf.", "quota_exceeded"}))
	if !strings.Contains(out, `"isError":true`) || !strings.Contains(out, "Not enough space") || strings.Contains(out, `format`) {
		t.Fatalf("refusal in a stream: %s", out)
	}
}

// CNF-04, RES-10: result arguments and «writes only to the space» are checked
// on saving; tools/list describes the result and adds overwrite.
func TestApplyResultArgs(t *testing.T) {
	it := docgenItem()
	if err := applyTools(it, Input{ResultArgs: &[]FileArg{{Tool: "convert", Arg: "source"}}}); code(err) != "catalog_result_arg_invalid" {
		t.Fatalf("a file argument cannot be a result: %v", err)
	}
	it = docgenItem()
	it.ResultArgs = nil
	if err := applyTools(it, Input{SpaceOnlyTools: &[]string{"convert"}}); code(err) != "catalog_space_only_invalid" {
		t.Fatalf("CNF-04: %v", err)
	}
	big := int64(60 << 20)
	if err := applyTools(docgenItem(), Input{ResultMaxBytes: &big}); code(err) != "catalog_result_max_invalid" {
		t.Fatalf("result max: %v", err)
	}
	if err := applyTools(docgenItem(), Input{FileArgs: &[]FileArg{{Tool: "render_document", Arg: "images"}}}); err != nil {
		t.Fatalf("a list of strings can be a file argument: %v", err)
	}
	answer := []byte(`{"result":{"tools":[{"name":"render_document","inputSchema":{"type":"object","properties":{"dest":{"type":"string","format":"uri"},"markdown":{"type":"string"}}}}]}}`)
	out := string(filterToolsList(answer, docgenItem(), nil))
	if !strings.Contains(out, "result file is saved") || !strings.Contains(out, `"overwrite"`) || strings.Contains(out, `"format":"uri"`) {
		t.Fatalf("RES-10: %s", out)
	}
}

// CNF-01, CNF-02: a new file of the space needs no confirmation, an overwrite does.
func TestSpaceOnly(t *testing.T) {
	it := docgenItem()
	if !it.spaceOnly("render_document") || overwriteRequested(json.RawMessage(`{"dest":"a.pdf"}`)) {
		t.Fatal("CNF-01")
	}
	if !overwriteRequested(json.RawMessage(`{"dest":"a.pdf","overwrite":true}`)) {
		t.Fatal("CNF-02")
	}
	it.ResultArgs = nil
	if it.spaceOnly("render_document") {
		t.Fatal("no result argument — no exemption")
	}
}

// R11: a list of files, each with its path in the fragment; one bad path refuses the call.
func TestRewriteFileList(t *testing.T) {
	it := docgenItem()
	s := &Service{Files: func(_ context.Context, _ uuid.UUID, p string, max int64) (string, int64, error) {
		return "http://s3/c/" + p + "?sig", max * 3, nil
	}}
	args, fr := s.rewriteFileArgs(context.Background(), it, uuid.New(), "render_document", json.RawMessage(`{"images":["charts/q3.png","логотип.png"]}`))
	if fr != nil || !strings.Contains(string(args), "#nabu-path=charts/q3.png") || !strings.Contains(string(args), "#nabu-path=%D0%BB") {
		t.Fatalf("R11: %s %+v", args, fr)
	}
	if _, fr := s.rewriteFileArgs(context.Background(), it, uuid.New(), "render_document", json.RawMessage(`{"images":["a.png","/etc/x"]}`)); fr == nil || !strings.HasPrefix(fr.text, "images[1]: ") {
		t.Fatalf("CNV-09: one bad path refuses the call with its index: %+v", fr)
	}
	// tech §4.3: the files of a list together — at most 10 × FileMaxBytes (each file here is 3 × the limit of a file)
	if _, fr := s.rewriteFileArgs(context.Background(), it, uuid.New(), "render_document", json.RawMessage(`{"images":["a.png","b.png","c.png"]}`)); fr != nil {
		t.Fatalf("30 of 100: %+v", fr)
	}
	if _, fr := s.rewriteFileArgs(context.Background(), it, uuid.New(), "render_document", json.RawMessage(`{"images":["a.png","b.png","c.png","d.png"]}`)); fr == nil || fr.result != "file_too_large" {
		t.Fatalf("over the sum: %+v", fr)
	}
}
