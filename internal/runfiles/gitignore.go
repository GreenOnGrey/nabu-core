package runfiles

import (
	"bufio"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// The rules of .gitignore files the changes of a run skip (tech §2.5): a
// subset of git that covers what repositories use — comments, negation,
// directory-only patterns, anchored patterns and "**".

type rule struct {
	base    string // the directory of the .gitignore, "" — the root
	pattern string
	neg     bool
	dirOnly bool
	// anchored patterns match the path from base; others match the name.
	anchored bool
}

// Ignore holds the rules of the .gitignore files of a tree.
type Ignore struct {
	root  string
	rules map[string][]rule // by directory, loaded once
}

// NewIgnore reads .gitignore files under root as directories are visited.
func NewIgnore(root string) *Ignore { return &Ignore{root: root, rules: map[string][]rule{}} }

func (g *Ignore) load(dir string) []rule {
	if rs, ok := g.rules[dir]; ok {
		return rs
	}
	var rs []rule
	f, err := os.Open(filepath.Join(g.root, filepath.FromSlash(dir), ".gitignore"))
	if err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimRight(sc.Text(), " \t\r")
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			r := rule{base: dir}
			if strings.HasPrefix(line, "!") {
				r.neg, line = true, line[1:]
			}
			line = strings.TrimPrefix(line, `\`)
			if strings.HasSuffix(line, "/") {
				r.dirOnly, line = true, strings.TrimRight(line, "/")
			}
			if strings.Contains(line, "/") {
				r.anchored, line = true, strings.TrimPrefix(line, "/")
			}
			if line != "" {
				r.pattern = line
				rs = append(rs, r)
			}
		}
		_ = f.Close()
	}
	g.rules[dir] = rs
	return rs
}

// Ignored reports whether rel (slash-separated, from the root) is ignored:
// the rules of the root and of every parent directory, the last match wins.
func (g *Ignore) Ignored(rel string, isDir bool) bool {
	dirs := []string{""}
	parts := strings.Split(rel, "/")
	for i := 1; i < len(parts); i++ {
		dirs = append(dirs, strings.Join(parts[:i], "/"))
	}
	ignored := false
	for _, d := range dirs {
		for _, r := range g.load(d) {
			if r.dirOnly && !isDir {
				continue
			}
			sub := rel
			if d != "" {
				sub = strings.TrimPrefix(rel, d+"/")
			}
			if r.match(sub) {
				ignored = !r.neg
			}
		}
	}
	return ignored
}

func (r rule) match(sub string) bool {
	if !r.anchored {
		ok, _ := path.Match(r.pattern, path.Base(sub))
		return ok
	}
	return globSegments(strings.Split(r.pattern, "/"), strings.Split(sub, "/"))
}

// globSegments matches path segments; "**" matches any number of them.
func globSegments(pat, segs []string) bool {
	for len(pat) > 0 {
		if pat[0] == "**" {
			if len(pat) == 1 {
				return true
			}
			for i := 0; i <= len(segs); i++ {
				if globSegments(pat[1:], segs[i:]) {
					return true
				}
			}
			return false
		}
		if len(segs) == 0 {
			return false
		}
		if ok, _ := path.Match(pat[0], segs[0]); !ok {
			return false
		}
		pat, segs = pat[1:], segs[1:]
	}
	return len(segs) == 0
}
