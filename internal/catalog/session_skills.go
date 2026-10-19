package catalog

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Skill is a skill a user may call explicitly (FTR.NAB.CHAT-0001 R4, R5,
// tech §2.1): a skill of a published skill item the user connected.
type Skill struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	ItemID      uuid.UUID `json:"itemId"`
	ItemTitle   string    `json:"itemTitle"`
}

// skillsCache keeps what is read from snapshots: the descriptions of their
// skills and the merged snapshots of sessions.
type skillsCache struct {
	mu     sync.Mutex
	descr  map[string]map[string]string // snapshot hash → skill → description
	merged map[string]string            // the key of the parts → hash of the merged snapshot
}

var cache = &skillsCache{descr: map[string]map[string]string{}, merged: map[string]string{}}

// SkillsFor lists the skills of the user's sessions, by name; a name met in
// two items is taken from the first one by the item's name.
func (s *Service) SkillsFor(ctx context.Context, uid uuid.UUID) ([]Skill, error) {
	return s.skillsOf(ctx, uid, true, nil)
}

// GroupSkills lists the skills of a group agent: those the owner chose, within
// the owner's access (FTR.NAB.CMN-0007 R12, FTR.NAB.CHAT-0001 R6); uuid.Nil —
// an agent without an owner.
func (s *Service) GroupSkills(ctx context.Context, owner uuid.UUID, chosen []string) ([]Skill, error) {
	want := map[string]bool{}
	for _, n := range chosen {
		want[n] = true
	}
	return s.skillsOf(ctx, owner, false, want)
}

// skillsOf lists the skills of the items granted to uid: the connected ones,
// or all of them with only the names in want.
func (s *Service) skillsOf(ctx context.Context, uid uuid.UUID, connected bool, want map[string]bool) ([]Skill, error) {
	rows, err := s.Pool.Query(ctx, `SELECT c.id, COALESCE(NULLIF(c.title,''), c.name), c.skills, c.skills_snapshot
		FROM catalog_items c
		WHERE c.type = 'skill' AND c.published AND NOT c.in_development AND c.skills_snapshot IS NOT NULL
		  AND (NOT $2 OR EXISTS (SELECT 1 FROM user_connections u WHERE u.item_id = c.id AND u.user_id = $1)) ORDER BY c.name`, uid, connected)
	if err != nil {
		return nil, err
	}
	type item struct {
		id    uuid.UUID
		title string
		names []string
		snap  string
	}
	var items []item
	for rows.Next() {
		var it item
		var raw []byte
		if err := rows.Scan(&it.id, &it.title, &raw, &it.snap); err != nil {
			rows.Close()
			return nil, err
		}
		_ = json.Unmarshal(raw, &it.names)
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := []Skill{}
	seen := map[string]bool{}
	g, err := s.granted(ctx, uid)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		if g != nil && !g[it.id] { // FTR.NAB.CMN-0007 R11
			continue
		}
		descr := s.skillDescriptions(ctx, it.snap)
		for _, n := range it.names {
			if seen[n] || (want != nil && !want[n]) {
				continue
			}
			seen[n] = true
			out = append(out, Skill{Name: n, Description: descr[n], ItemID: it.id, ItemTitle: it.title})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// skillDescriptions reads the description of every skill of a snapshot from
// the frontmatter of its SKILL.md; cached by the hash.
func (s *Service) skillDescriptions(ctx context.Context, hash string) map[string]string {
	cache.mu.Lock()
	d, ok := cache.descr[hash]
	cache.mu.Unlock()
	if ok {
		return d
	}
	d = map[string]string{}
	b, err := s.Bundle(ctx, hash)
	if err != nil {
		return d
	}
	_ = walkTar(b, func(name string, data []byte) {
		dir, file := path.Split(name)
		if file == "SKILL.md" && strings.Count(strings.Trim(dir, "/"), "/") == 0 {
			d[strings.Trim(dir, "/")] = frontmatter(string(data), "description")
		}
	})
	cache.mu.Lock()
	cache.descr[hash] = d
	cache.mu.Unlock()
	return d
}

// frontmatter returns a field of the YAML frontmatter of a Markdown file
// (one line; quotes removed).
func frontmatter(md, field string) string {
	md = strings.TrimPrefix(md, "\ufeff")
	if !strings.HasPrefix(md, "---") {
		return ""
	}
	end := strings.Index(md[3:], "\n---")
	if end < 0 {
		return ""
	}
	for _, line := range strings.Split(md[3:3+end], "\n") {
		k, v, ok := strings.Cut(line, ":")
		if ok && strings.TrimSpace(k) == field {
			v = strings.TrimSpace(v)
			return strings.Trim(v, `"'`)
		}
	}
	return ""
}

func walkTar(bundle []byte, fn func(name string, data []byte)) error {
	gz, err := gzip.NewReader(bytes.NewReader(bundle))
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if h.Typeflag != tar.TypeReg || h.Size > 16<<20 {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return err
		}
		fn(h.Name, data)
	}
}

// SessionSkills returns the snapshot of a session with skills from several
// items: one snapshot as is, several merged into one stored in S3 under its
// own hash, so the operator receives one bundle (FTR.NAB.CHAT-0001 R5). The
// names are deduplicated.
func (s *Service) SessionSkills(ctx context.Context, m *SessionMCP) (string, []string, error) {
	parts := map[string][]string{} // snapshot → names
	var names []string
	seen := map[string]bool{}
	for _, n := range m.Skills {
		h := m.SkillSnapshots[n]
		if h == "" || seen[n] {
			continue
		}
		seen[n] = true
		names = append(names, n)
		parts[h] = append(parts[h], n)
	}
	if len(parts) == 0 {
		return "", nil, nil
	}
	sort.Strings(names)
	if len(parts) == 1 {
		for h := range parts {
			return h, names, nil
		}
	}
	hashes := make([]string, 0, len(parts))
	for h := range parts {
		sort.Strings(parts[h])
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	var key strings.Builder
	for _, h := range hashes {
		key.WriteString(h + "=" + strings.Join(parts[h], ",") + ";")
	}
	cache.mu.Lock()
	if h, ok := cache.merged[key.String()]; ok {
		cache.mu.Unlock()
		return h, names, nil
	}
	cache.mu.Unlock()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.ModTime = time.Unix(0, 0)
	tw := tar.NewWriter(gz)
	type file struct {
		name string
		data []byte
	}
	var files []file
	for _, h := range hashes {
		b, err := s.Bundle(ctx, h)
		if err != nil {
			return "", nil, err
		}
		want := map[string]bool{}
		for _, n := range parts[h] {
			want[n] = true
		}
		if err := walkTar(b, func(name string, data []byte) {
			if top, _, _ := strings.Cut(name, "/"); want[top] {
				files = append(files, file{name, data})
			}
		}); err != nil {
			return "", nil, err
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	for _, f := range files {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.data)), ModTime: time.Unix(0, 0), Typeflag: tar.TypeReg}); err != nil {
			return "", nil, err
		}
		if _, err := tw.Write(f.data); err != nil {
			return "", nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return "", nil, err
	}
	if err := gz.Close(); err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(buf.Bytes())
	hash := "sha256:" + hex.EncodeToString(sum[:])
	if err := s.S3.Put(ctx, "skills/"+hex.EncodeToString(sum[:])+".tar.gz", bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/gzip"); err != nil {
		return "", nil, err
	}
	cache.mu.Lock()
	cache.merged[key.String()] = hash
	cache.mu.Unlock()
	return hash, names, nil
}
