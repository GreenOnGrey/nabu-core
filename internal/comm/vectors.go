package comm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Embedder turns texts into vectors with the model the administrator chose
// for the search by meaning (R20, arch §7.3): an OpenAI-compatible
// /embeddings of a model connection.
type Embedder struct {
	BaseURL string
	Key     string
	Model   string
	Client  *http.Client
}

// Embed returns a vector per text.
func (e *Embedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	body, _ := json.Marshal(map[string]any{"model": e.Model, "input": texts})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.BaseURL, "/")+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.Key != "" {
		req.Header.Set("Authorization", "Bearer "+e.Key)
	}
	c := e.Client
	if c == nil {
		c = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil, fmt.Errorf("embeddings: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Data) != len(texts) {
		return nil, fmt.Errorf("embeddings: %d vectors for %d texts", len(out.Data), len(texts))
	}
	vecs := make([][]float32, len(texts))
	for _, d := range out.Data {
		if d.Index < 0 || d.Index >= len(vecs) || len(d.Embedding) == 0 {
			return nil, fmt.Errorf("embeddings: unexpected answer")
		}
		vecs[d.Index] = d.Embedding
	}
	return vecs, nil
}

func vectorLiteral(v []float32) string {
	var b strings.Builder
	b.Grow(len(v) * 10)
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', 7, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// vectorDDL creates the table of the vectors of notes (tech §14, decision 13):
// separate from the notes, so the memory of communications works without the
// vector extension and the migration does not need it. The vectors have half
// precision and no fixed dimension — the model can change; there is no index
// by vector: the search reads the vectors of one user only.
const vectorDDL = `
CREATE TABLE IF NOT EXISTS comm_note_vectors (
  user_id     UUID NOT NULL,
  note_id     UUID NOT NULL,
  model       TEXT NOT NULL,
  embedding   halfvec NOT NULL,
  embedded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, note_id),
  FOREIGN KEY (user_id, note_id) REFERENCES comm_notes (user_id, id) ON DELETE CASCADE
) PARTITION BY HASH (user_id);
DO $$
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format('CREATE TABLE IF NOT EXISTS comm_note_vectors_p%s PARTITION OF comm_note_vectors FOR VALUES WITH (MODULUS 32, REMAINDER %s)', lpad(i::text, 2, '0'), i);
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'comm_note_vectors' AND policyname = 'comm_owner') THEN
    ALTER TABLE comm_note_vectors ENABLE ROW LEVEL SECURITY;
    ALTER TABLE comm_note_vectors FORCE ROW LEVEL SECURITY;
    CREATE POLICY comm_owner ON comm_note_vectors USING (
      user_id = nullif(current_setting('nabu.user_id', true), '')::uuid
      OR current_setting('nabu.maintenance', true) = 'on');
  END IF;
END $$;`

// EnsureVectors prepares the store of vectors when the database has the
// vector extension: it tries to create the extension (the owner of a managed
// database may not be allowed to — then the infrastructure does it) and the
// table. It reports whether the search by meaning can work. Call it at the
// start of api and worker; it is idempotent.
func (s *Service) EnsureVectors(ctx context.Context) bool {
	var installed, ready bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector'), to_regclass('comm_note_vectors') IS NOT NULL`).Scan(&installed, &ready); err != nil {
		return false
	}
	if installed && ready {
		s.vectors.Store(true)
		return true
	}
	if !installed {
		if _, err := s.Pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
			slog.InfoContext(ctx, "the vector extension is not available: the communications are searched by words only", "err", err)
			s.vectors.Store(false)
			return false
		}
	}
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('nabu:comm:vectors'))`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, vectorDDL)
		return err
	})
	if err != nil {
		slog.WarnContext(ctx, "the table of vectors could not be created", "err", err)
	}
	s.vectors.Store(err == nil)
	return err == nil
}

// Semantic reports whether the search by meaning works now: the vectors can
// be stored and a model of embeddings is chosen and reachable (tech §3.1).
func (s *Service) Semantic(ctx context.Context) bool {
	return s.vectors.Load() && s.embedder(ctx) != nil
}

func (s *Service) embedder(ctx context.Context) *Embedder {
	if s.Embedder == nil || !s.vectors.Load() {
		return nil
	}
	return s.Embedder(ctx)
}

// noteText is what a note is embedded by: the title, the summary, the people
// and the topics.
const noteTextSQL = `n.title || E'\n' || n.summary || E'\n' ||
	COALESCE((SELECT string_agg(COALESCE(NULLIF(p.name,''), p.email, tp.name, ''), ', ')
		FROM comm_note_tags t LEFT JOIN comm_people p ON t.kind = 'person' AND p.id = t.tag_id
		LEFT JOIN comm_topics tp ON t.kind = 'topic' AND tp.id = t.tag_id
		WHERE t.user_id = n.user_id AND t.note_id = n.id), '')`

// EmbedPending computes the vectors of the notes that have none, were changed
// since, or were embedded by another model — so a change of the model
// recounts everything in the background (tech §10). It returns how many
// notes it embedded; 0 — nothing is pending or the search by meaning is off.
// It is maintenance: it reads the notes of every user and never returns them.
func (s *Service) EmbedPending(ctx context.Context, limit int) (int, error) {
	e := s.embedder(ctx)
	if e == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = 64
	}
	type row struct {
		uid, id uuid.UUID
		text    string
	}
	var rows []row
	err := postgres.AsMaintenance(ctx, s.Pool, func(tx pgx.Tx) error {
		r, err := tx.Query(ctx, `SELECT n.user_id, n.id, `+noteTextSQL+` FROM comm_notes n
			LEFT JOIN comm_note_vectors v ON v.user_id = n.user_id AND v.note_id = n.id
			WHERE v.note_id IS NULL OR v.model <> $1 OR v.embedded_at < n.updated_at
			ORDER BY n.updated_at LIMIT $2`, e.Model, limit)
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			var x row
			if err := r.Scan(&x.uid, &x.id, &x.text); err != nil {
				return err
			}
			rows = append(rows, x)
		}
		return r.Err()
	})
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	texts := make([]string, len(rows))
	for i, x := range rows {
		texts[i] = clipRunes(x.text, 6000)
	}
	vecs, err := e.Embed(ctx, texts)
	if err != nil {
		return 0, err
	}
	err = postgres.AsMaintenance(ctx, s.Pool, func(tx pgx.Tx) error {
		for i, x := range rows {
			// the note may be gone by now: the insert selects from it
			if _, err := tx.Exec(ctx, `INSERT INTO comm_note_vectors (user_id, note_id, model, embedding, embedded_at)
				SELECT n.user_id, n.id, $3, $4::halfvec, now() FROM comm_notes n WHERE n.user_id = $1 AND n.id = $2
				ON CONFLICT (user_id, note_id) DO UPDATE SET model = EXCLUDED.model, embedding = EXCLUDED.embedding, embedded_at = now()`,
				x.uid, x.id, e.Model, vectorLiteral(vecs[i])); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// PendingVectors counts the notes waiting for a vector (a metric).
func (s *Service) PendingVectors(ctx context.Context) (int, error) {
	e := s.embedder(ctx)
	if e == nil {
		return 0, nil
	}
	var n int
	err := postgres.AsMaintenance(ctx, s.Pool, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM comm_notes n LEFT JOIN comm_note_vectors v ON v.user_id = n.user_id AND v.note_id = n.id
			WHERE v.note_id IS NULL OR v.model <> $1 OR v.embedded_at < n.updated_at`, e.Model).Scan(&n)
	})
	return n, err
}

// searchHybrid ranks the notes of the user by the closeness of vectors and by
// words and merges the two ranks (tech §8). The vectors of one user are read
// exactly: there are at most thousands of them.
func (s *Service) searchHybrid(ctx context.Context, tx pgx.Tx, uid uuid.UUID, f Filter, q string, vec []float32, model string, limit int) ([]uuid.UUID, error) {
	args := []any{uid}
	cond := f.where(&args)
	args = append(args, vectorLiteral(vec), model, q, limit)
	vi, mi, qi, li := len(args)-3, len(args)-2, len(args)-1, len(args)
	rows, err := tx.Query(ctx, fmt.Sprintf(`WITH v AS (
			SELECT n.id, row_number() OVER (ORDER BY x.embedding <=> $%[1]d::halfvec) AS r
			FROM comm_note_vectors x JOIN comm_notes n ON n.user_id = x.user_id AND n.id = x.note_id
			WHERE x.user_id = $1 AND x.model = $%[2]d%[5]s
			ORDER BY x.embedding <=> $%[1]d::halfvec LIMIT 20),
		w AS (
			SELECT n.id, row_number() OVER (ORDER BY ts_rank(n.search_vector, tq) DESC, n.last_at DESC) AS r
			FROM comm_notes n, plainto_tsquery('simple', $%[3]d) tq
			WHERE n.user_id = $1%[5]s AND (n.search_vector @@ tq OR n.title ILIKE '%%' || $%[3]d || '%%' OR n.summary ILIKE '%%' || $%[3]d || '%%')
			LIMIT 50)
		SELECT id FROM (SELECT * FROM v UNION ALL SELECT * FROM w) u
		GROUP BY id ORDER BY sum(1.0 / (60 + r)) DESC, id LIMIT $%[4]d`, vi, mi, qi, li, cond), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
