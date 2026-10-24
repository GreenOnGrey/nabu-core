package catalog

// Result files of catalog MCP tools (FTR.NAB.CHAT-0003 R10, arch §5.1, tech
// §4.4): the agent passes a path of its space, the server gets a link to
// upload a temporary object, and after the answer the proxy moves the file to
// the space — into the live sandbox when it runs.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/google/uuid"
)

// ResultStore is what the proxy needs of the space and the storage.
type ResultStore interface {
	// Exists reports whether a file is at the path of the space (the live
	// sandbox first).
	Exists(ctx context.Context, uid uuid.UUID, p string) (bool, error)
	// Upload reserves a temporary object for a result and returns its key and
	// a link to PUT it.
	Upload(ctx context.Context, name string) (key, putURL string, err error)
	// Finish moves the uploaded object to the path of the space and deletes
	// it; it returns the size.
	Finish(ctx context.Context, uid uuid.UUID, p, key string, max int64) (int64, error)
	// Discard deletes the temporary object.
	Discard(ctx context.Context, key string)
}

// Errors of ResultStore.Finish.
var (
	ErrResultMissing  = errors.New("the server did not upload the result")
	ErrResultTooLarge = errors.New("the result is too large")
	ErrResultQuota    = errors.New("the space is full")
	ErrResultPath     = errors.New("not a path in the space")
)

// resultPlan is a result argument of a call being made.
type resultPlan struct {
	Path, Key string
	Max       int64
}

// overwriteRequested reports overwrite: true in the arguments.
func overwriteRequested(args json.RawMessage) bool {
	var m struct {
		Overwrite bool `json:"overwrite"`
	}
	_ = json.Unmarshal(args, &m)
	return m.Overwrite
}

// rewriteResultArgs checks the result path of a call and replaces it with a
// link to upload the result; overwrite is read and taken out of the
// arguments. A bad path or an existing file without overwrite refuse the
// call before the server is called.
func (s *Service) rewriteResultArgs(ctx context.Context, it *Item, uid uuid.UUID, tool string, args json.RawMessage) (json.RawMessage, []resultPlan, *fileRefusal) {
	names := it.resultArgsOf(tool)
	if len(names) == 0 || len(args) == 0 {
		return args, nil, nil
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(args, &m) != nil {
		return args, nil, nil
	}
	overwrite := overwriteRequested(args)
	delete(m, "overwrite")
	var plans []resultPlan
	for _, name := range names {
		raw, ok := m[name]
		if !ok {
			continue
		}
		var p string
		if json.Unmarshal(raw, &p) != nil {
			s.discard(ctx, plans)
			return nil, nil, &fileRefusal{"The argument " + name + " is a path in your space where the file is saved, e.g. reports/q3.pdf.", "result_rejected"}
		}
		p = strings.TrimSpace(p)
		if p == "" || schemeRe.MatchString(p) || strings.HasPrefix(p, "/") || strings.HasPrefix(p, "\\") || strings.HasSuffix(p, "/") ||
			uid == uuid.Nil || s.Results == nil || path.Clean(p) != p || strings.HasPrefix(p, "../") || p == ".." {
			s.discard(ctx, plans)
			return nil, nil, &fileRefusal{"Pass a path in your space where the file is saved, e.g. reports/q3.pdf; URLs, absolute paths and .. are not accepted.", "result_rejected"}
		}
		exists, err := s.Results.Exists(ctx, uid, p)
		if errors.Is(err, ErrResultPath) {
			s.discard(ctx, plans)
			return nil, nil, &fileRefusal{"Pass a path in your space where the file is saved, e.g. reports/q3.pdf.", "result_rejected"}
		}
		if err != nil {
			s.discard(ctx, plans)
			return nil, nil, &fileRefusal{"The space is not available: " + err.Error(), "result_rejected"}
		}
		if exists && !overwrite {
			s.discard(ctx, plans)
			return nil, nil, &fileRefusal{"The file " + p + " exists; pass overwrite: true to replace it, or choose another path.", "result_exists"}
		}
		key, u, err := s.Results.Upload(ctx, path.Base(p))
		if err != nil {
			s.discard(ctx, plans)
			return nil, nil, &fileRefusal{"The result cannot be prepared: " + err.Error(), "result_rejected"}
		}
		m[name], _ = json.Marshal(u)
		plans = append(plans, resultPlan{Path: p, Key: key, Max: it.ResultMaxBytes})
	}
	out, _ := json.Marshal(m)
	return out, plans, nil
}

func (s *Service) discard(ctx context.Context, plans []resultPlan) {
	for _, p := range plans {
		s.Results.Discard(context.WithoutCancel(ctx), p.Key)
	}
}

// finishResults moves the results of a successful call to the space. It
// returns the note for the agent, or the refusal when a file is missing,
// too large or the space is full; the temporary objects are deleted either way.
func (s *Service) finishResults(ctx context.Context, uid uuid.UUID, plans []resultPlan, isErr bool) (string, *fileRefusal) {
	ctx = context.WithoutCancel(ctx)
	if isErr {
		s.discard(ctx, plans)
		return "", nil
	}
	var notes []string
	for i, p := range plans {
		size, err := s.Results.Finish(ctx, uid, p.Path, p.Key, p.Max)
		var fr *fileRefusal
		switch {
		case errors.Is(err, ErrResultMissing):
			fr = &fileRefusal{"The server did not save the file " + p.Path + ".", "result_missing"}
		case errors.Is(err, ErrResultTooLarge):
			fr = &fileRefusal{"The file " + p.Path + " is larger than the limit of this server, " + mb(p.Max) + " MB; it is not saved.", "result_too_large"}
		case errors.Is(err, ErrResultQuota):
			fr = &fileRefusal{"Not enough space left to save " + p.Path + ".", "quota_exceeded"}
		case err != nil:
			fr = &fileRefusal{"The file " + p.Path + " could not be saved: " + err.Error(), "result_missing"}
		}
		if fr != nil {
			s.discard(ctx, plans[i+1:])
			return "", fr
		}
		notes = append(notes, fmt.Sprintf("Saved to %s (%s).", p.Path, humanSize(size)))
	}
	return strings.Join(notes, " "), nil
}

func humanSize(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%d KB", n/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// withResultNote rewrites the answer of a call (JSON or the last event of a
// stream): a refusal replaces the result with an error, a note is added as
// the last text of the content.
func withResultNote(raw []byte, note string, fr *fileRefusal) []byte {
	edit := func(msg []byte) []byte {
		var m map[string]json.RawMessage
		if json.Unmarshal(msg, &m) != nil {
			return msg
		}
		var res map[string]json.RawMessage
		if json.Unmarshal(m["result"], &res) != nil {
			return msg
		}
		var content []json.RawMessage
		_ = json.Unmarshal(res["content"], &content)
		text := note
		if fr != nil {
			content = nil
			text = fr.text
			res["isError"] = json.RawMessage("true")
		}
		item, _ := json.Marshal(map[string]string{"type": "text", "text": text})
		content = append(content, item)
		res["content"], _ = json.Marshal(content)
		m["result"], _ = json.Marshal(res)
		out, _ := json.Marshal(m)
		return out
	}
	// a JSON answer may well carry "data:" (a data URI in a text): only an
	// answer that is not one JSON document is a stream
	if json.Valid(raw) {
		return edit(raw)
	}
	// a stream: the result is the last data event with a result
	var lines []string
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(nil, 16<<20)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	for i := len(lines) - 1; i >= 0; i-- {
		if data, ok := strings.CutPrefix(lines[i], "data:"); ok && strings.Contains(data, `"result"`) {
			lines[i] = "data: " + string(edit([]byte(strings.TrimSpace(data))))
			break
		}
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}
