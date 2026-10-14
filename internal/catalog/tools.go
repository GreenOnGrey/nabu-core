package catalog

// The tools of an MCP item an agent may use and the arguments that are files
// of the user's space (FTR.NAB.CMN-0006 R13, R14; arch §4; tech §3.2–3.5).
// Nabu knows nothing of particular servers: the administrator chooses both in
// the item.

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// FileArg marks a string argument of a tool as a path in the user's space.
type FileArg struct {
	Tool string `json:"tool"`
	Arg  string `json:"arg"`
}

const (
	// DefaultFileMaxBytes is the default limit of a file argument (20 MB).
	DefaultFileMaxBytes = 20 << 20
	minFileMaxBytes     = 1 << 10
	maxFileMaxBytes     = 100 << 20
)

// fileArgDescription replaces the description of a file argument in
// tools/list (FIL-09): the agent passes a path, never a URL.
const fileArgDescription = "Path to a file in the user's Nabu space, relative to its root, e.g. docs/report.pdf. URLs and absolute paths are rejected."

// FileSnapshot copies the current version of a file of the user's space and
// returns a URL valid for a few minutes (space.Service.Snapshot).
type FileSnapshot func(ctx context.Context, uid uuid.UUID, path string, max int64) (string, error)

// Errors of FileSnapshot the proxy turns into answers to the agent.
var (
	ErrFileNotFound = errors.New("file not found in the space")
	ErrFileTooLarge = errors.New("file too large")
	ErrFileInvalid  = errors.New("not a path in the space")
)

func (it *Item) tool(name string) (agent.MCPTool, bool) {
	for _, t := range it.Tools {
		if t.Name == name {
			return t, true
		}
	}
	return agent.MCPTool{}, false
}

// allowed reports whether the agent may call the tool. With a list of
// allowed tools only those in both the list and the stored tools of the last
// check pass (CAT-04); without one (an item never saved with a list) the old
// rule applies: a read-only item passes only its read-only tools.
func (it *Item) allowed(name string) bool {
	if it.AllowedTools != nil {
		_, stored := it.tool(name)
		return stored && slices.Contains(it.AllowedTools, name)
	}
	if it.ReadOnly {
		t, ok := it.tool(name)
		return ok && t.ReadOnly
	}
	return true
}

// presetTools are the tools a list starts with: the read-only ones, or all.
func presetTools(tools []agent.MCPTool, readOnly bool) []string {
	out := []string{}
	for _, t := range tools {
		if !readOnly || t.ReadOnly {
			out = append(out, t.Name)
		}
	}
	return out
}

// allReadOnly reports whether every allowed tool only reads: the computed
// readOnly of an item with a list (arch §4.1).
func allReadOnly(tools []agent.MCPTool, allowed []string) bool {
	for _, t := range tools {
		if slices.Contains(allowed, t.Name) && !t.ReadOnly {
			return false
		}
	}
	return true
}

// newTools are the tools the agent may not use yet that the last check
// brought: the ones already marked and still not allowed, and the ones the
// server did not have before.
func newTools(before, after []agent.MCPTool, allowed, marked []string) []string {
	out := []string{}
	for _, t := range after {
		if slices.Contains(allowed, t.Name) {
			continue
		}
		known := slices.ContainsFunc(before, func(b agent.MCPTool) bool { return b.Name == t.Name })
		if !known || slices.Contains(marked, t.Name) {
			out = append(out, t.Name)
		}
	}
	return out
}

// keepFileArgs drops the file arguments of tools that are no longer allowed
// or no longer have the argument.
func keepFileArgs(it *Item) {
	kept := []FileArg{}
	for _, fa := range it.FileArgs {
		if validFileArg(it, fa) {
			kept = append(kept, fa)
		}
	}
	it.FileArgs = kept
}

func validFileArg(it *Item, fa FileArg) bool {
	t, ok := it.tool(fa.Tool)
	return ok && it.allowed(fa.Tool) && slices.Contains(t.StringArgs, fa.Arg)
}

// applyTools applies allowedTools, readOnly, fileArgs and fileMaxBytes of an
// input to an item (tech §3.3).
func applyTools(it *Item, in Input) error {
	switch {
	case in.AllowedTools != nil:
		for _, name := range *in.AllowedTools {
			if _, ok := it.tool(name); !ok {
				return apperr.Unprocessable("catalog_tool_unknown", "the tool "+name+" is not in the last check of the item; check the item again").
					With("field", "allowedTools").With("tool", name)
			}
		}
		it.AllowedTools = slices.Clone(*in.AllowedTools)
		it.ReadOnly = allReadOnly(it.Tools, it.AllowedTools)
	case in.ReadOnly != nil && len(it.Tools) > 0:
		// an old client of the API (CAT-10): the flag chooses the tools
		it.AllowedTools = presetTools(it.Tools, *in.ReadOnly)
		it.ReadOnly = *in.ReadOnly
	case in.ReadOnly != nil:
		it.ReadOnly = *in.ReadOnly
	}
	if in.FileArgs != nil {
		for _, fa := range *in.FileArgs {
			if !validFileArg(it, fa) {
				return apperr.Unprocessable("catalog_file_arg_invalid", "only a string argument of an allowed tool can be a space file").
					With("field", "fileArgs").With("tool", fa.Tool).With("arg", fa.Arg)
			}
		}
		it.FileArgs = slices.Clone(*in.FileArgs)
	} else {
		keepFileArgs(it)
	}
	if in.FileMaxBytes != nil {
		if *in.FileMaxBytes < minFileMaxBytes || *in.FileMaxBytes > maxFileMaxBytes {
			return apperr.Unprocessable("catalog_file_max_invalid", "the file limit is 1 KB to 100 MB").With("field", "fileMaxBytes")
		}
		it.FileMaxBytes = *in.FileMaxBytes
	}
	return nil
}

func (it *Item) fileArgsOf(tool string) []string {
	var out []string
	for _, fa := range it.FileArgs {
		if fa.Tool == tool {
			out = append(out, fa.Arg)
		}
	}
	return out
}

var schemeRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+.-]*:`)

// fileRefusal is the answer to an agent whose file argument was refused, and
// the result label of the metric.
type fileRefusal struct {
	text, result string
}

// rewriteFileArgs replaces the paths in the file arguments of a call with
// URLs of copies of the files (FIL-01…FIL-16). It returns the new arguments,
// or why the call is refused before the server is called.
func (s *Service) rewriteFileArgs(ctx context.Context, it *Item, uid uuid.UUID, tool string, args json.RawMessage) (json.RawMessage, *fileRefusal) {
	names := it.fileArgsOf(tool)
	if len(names) == 0 || len(args) == 0 {
		return args, nil
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(args, &m) != nil {
		return args, nil
	}
	changed := false
	for _, name := range names {
		raw, ok := m[name]
		if !ok {
			continue
		}
		var p string
		if json.Unmarshal(raw, &p) != nil {
			return nil, &fileRefusal{"The argument " + name + " is a path to a file in your space, e.g. docs/report.pdf.", "file_rejected"}
		}
		p = strings.TrimSpace(p)
		if p == "" || schemeRe.MatchString(p) || strings.HasPrefix(p, "/") || strings.HasPrefix(p, "\\") || uid == uuid.Nil || s.Files == nil {
			return nil, &fileRefusal{"Pass a path to a file in your space, e.g. docs/report.pdf; URLs and absolute paths are not accepted.", "file_rejected"}
		}
		u, err := s.Files(ctx, uid, p, it.FileMaxBytes)
		switch {
		case errors.Is(err, ErrFileTooLarge):
			return nil, &fileRefusal{"The file " + p + " is larger than the limit of this server, " + mb(it.FileMaxBytes) + " MB.", "file_too_large"}
		case errors.Is(err, ErrFileNotFound):
			return nil, &fileRefusal{"The file " + p + " is not found in your space.", "file_rejected"}
		case errors.Is(err, ErrFileInvalid):
			return nil, &fileRefusal{"Pass a path to a file in your space, e.g. docs/report.pdf; URLs and absolute paths are not accepted.", "file_rejected"}
		case err != nil:
			return nil, &fileRefusal{"The file " + p + " could not be read: " + err.Error(), "file_rejected"}
		}
		m[name], _ = json.Marshal(u)
		changed = true
	}
	if !changed {
		return args, nil
	}
	out, _ := json.Marshal(m)
	return out, nil
}

func mb(n int64) string {
	b, _ := json.Marshal(float64(n) / (1 << 20))
	return strings.TrimSuffix(strings.TrimSuffix(string(b), ".0"), ".00")
}

// filterToolsList keeps the tools of a tools/list answer the agent may see
// and replaces the descriptions of file arguments. only — the tools of a run
// of the collector (FTR.NAB.CMN-0003), intersected with the allowed ones
// (CAT-12).
func filterToolsList(raw []byte, it *Item, only []string) []byte {
	var msg map[string]json.RawMessage
	if json.Unmarshal(raw, &msg) != nil {
		return raw
	}
	res, ok := msg["result"]
	if !ok {
		return raw
	}
	var r map[string]json.RawMessage
	if json.Unmarshal(res, &r) != nil {
		return raw
	}
	var tools []map[string]json.RawMessage
	if json.Unmarshal(r["tools"], &tools) != nil {
		return raw
	}
	kept := []map[string]json.RawMessage{}
	for _, t := range tools {
		var name string
		_ = json.Unmarshal(t["name"], &name)
		var ann struct {
			ReadOnlyHint *bool `json:"readOnlyHint"`
		}
		_ = json.Unmarshal(t["annotations"], &ann)
		switch {
		case only != nil && !slices.Contains(only, name):
			continue
		case it.AllowedTools != nil && !it.allowed(name):
			continue
		case it.AllowedTools == nil && it.ReadOnly && only == nil && (ann.ReadOnlyHint == nil || !*ann.ReadOnlyHint):
			continue // CAT-07: an item never checked keeps the filter by annotation
		}
		if args := it.fileArgsOf(name); len(args) > 0 {
			t["inputSchema"] = describeFileArgs(t["inputSchema"], args)
		}
		kept = append(kept, t)
	}
	r["tools"], _ = json.Marshal(kept)
	msg["result"], _ = json.Marshal(r)
	out, _ := json.Marshal(msg)
	return out
}

// describeFileArgs replaces the description of file arguments and drops a
// URI format (FIL-09); the rest of the schema stays.
func describeFileArgs(schema json.RawMessage, args []string) json.RawMessage {
	var s map[string]json.RawMessage
	if json.Unmarshal(schema, &s) != nil {
		return schema
	}
	var props map[string]map[string]json.RawMessage
	if json.Unmarshal(s["properties"], &props) != nil {
		return schema
	}
	for _, a := range args {
		p, ok := props[a]
		if !ok {
			continue
		}
		p["description"], _ = json.Marshal(fileArgDescription)
		delete(p, "format")
	}
	s["properties"], _ = json.Marshal(props)
	out, _ := json.Marshal(s)
	return out
}

// metricTool is the tool label of the metrics (OBS-02).
func (it *Item) metricTool(name string) string {
	if _, ok := it.tool(name); ok {
		return name
	}
	return "other"
}

func deniedRefusal(tool string) string {
	return "The tool " + tool + " is not allowed in Nabu for this server."
}
