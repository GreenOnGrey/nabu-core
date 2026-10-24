// Package preview turns office files into previews for the site
// (FTR.NAB.CHAT-0003 R13–R16, arch §6, tech §6): documents and presentations
// become PDF through LibreOffice of doc-convert, spreadsheets become sheets
// of cells through docgen. A preview is made on the first open and kept by
// the hash of the content.
package preview

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
	"github.com/GreenOnGrey/nabu-core/internal/space"
)

// Config is where the converters are (PREVIEW_*).
type Config struct {
	ConvertURL, ConvertUser, ConvertPassword string // Gotenberg of doc-convert
	LegacyURL                                string // the legacy sidecar: DOC/XLS/PPT → OOXML
	DocgenURL, DocgenKey                     string // docgen: sheets of a workbook
	MaxBytes                                 int64  // 25 MB
	Parallel                                 int    // conversions per api pod
}

// Enabled reports that the service of documents is on the stand (R22).
func (c Config) Enabled() bool { return c.ConvertURL != "" && c.DocgenURL != "" }

// Bounds of a preview (R13–R15).
const (
	pagesShown  = 50
	taskTimeout = 30 * time.Second
	errorKeep   = time.Hour
	pendingFor  = 40 * time.Second
	keepUnused  = 30 * 24 * time.Hour
	sheetRows   = 500
	sheetCols   = 50
	cachePrefix = "previews/"
)

var (
	docExt   = map[string]bool{"docx": true, "doc": true, "docm": true, "odt": true, "rtf": true, "pptx": true, "ppt": true, "pptm": true, "odp": true}
	sheetExt = map[string]bool{"xlsx": true, "xlsm": true, "xls": true, "ods": true}
)

// Kind is doc or sheet by the extension of a file name; "" — no preview.
func Kind(name string) string {
	ext := strings.ToLower(strings.TrimPrefix(path.Ext(name), "."))
	switch {
	case docExt[ext]:
		return "doc"
	case sheetExt[ext]:
		return "sheet"
	}
	return ""
}

// Service makes and serves previews.
type Service struct {
	Pool *pgxpool.Pool
	S3   storage.Storage
	Cfg  Config
	HTTP *http.Client

	once sync.Once
	sem  chan struct{}
}

type source struct {
	name, key, sha string
	size           int64
	attachment     uuid.UUID // the row to store the hash of an old attachment
}

// Routes mounts GET /preview (under /api/v1, like the content of attachments).
func (s *Service) Routes(r chi.Router) {
	r.Get("/preview", httpx.Handler(s.serve))
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) error {
	if !s.Cfg.Enabled() {
		metrics.PreviewRequests.WithLabelValues("", "disabled").Inc()
		return apperr.NotFound("preview_disabled", "previews are not enabled")
	}
	p, err := httpx.MustPrincipal(r)
	if err != nil {
		return err
	}
	ctx := r.Context()
	src, err := s.source(ctx, p.UserID, r.URL.Query())
	if err != nil {
		return err
	}
	kind := Kind(src.name)
	if kind == "" {
		return apperr.Unsupported("preview_unsupported", "no preview for this file")
	}
	if src.size > s.max() {
		metrics.PreviewRequests.WithLabelValues(kind, "too_large").Inc()
		return apperr.Unprocessable("preview_unavailable", "the file is too large").With("reason", "too_large").With("limitMb", s.max()>>20)
	}
	if src.sha == "" {
		if src.sha, err = s.hash(ctx, src); err != nil {
			return err
		}
	}
	var (
		state, reason        string
		shown, total         *int
		startedAt, expiresAt *time.Time
	)
	err = s.Pool.QueryRow(ctx, `SELECT state, coalesce(reason, ''), pages_shown, pages_total, started_at, expires_at FROM previews WHERE sha256 = $1`,
		src.sha).Scan(&state, &reason, &shown, &total, &startedAt, &expiresAt)
	switch {
	case err == nil && state == "ready":
		_, _ = s.Pool.Exec(ctx, `UPDATE previews SET accessed_at = now() WHERE sha256 = $1 AND accessed_at < now() - interval '1 day'`, src.sha)
		metrics.PreviewRequests.WithLabelValues(kind, "ok").Inc()
		metrics.PreviewSeconds.WithLabelValues(kind, "hit").Observe(0)
		return s.send(ctx, w, src.sha, kind, shown, total)
	case err == nil && state == "error" && expiresAt != nil && expiresAt.After(time.Now()):
		metrics.PreviewRequests.WithLabelValues(kind, reason).Inc()
		return apperr.Unprocessable("preview_unavailable", "the preview is not available").With("reason", reason)
	case err == nil && state == "pending" && startedAt != nil && time.Since(*startedAt) < pendingFor:
		metrics.PreviewRequests.WithLabelValues(kind, "pending").Inc()
		httpx.JSON(w, http.StatusAccepted, map[string]string{"state": "pending"})
		return nil
	case err != nil && !postgres.IsNoRows(err):
		return err
	}
	// Claim the conversion: one api pod does it, the others answer pending.
	tag, err := s.Pool.Exec(ctx, `INSERT INTO previews (sha256, kind, state, started_at) VALUES ($1, $2, 'pending', now())
		ON CONFLICT (sha256) DO UPDATE SET state = 'pending', started_at = now(), reason = NULL, expires_at = NULL
		WHERE previews.state <> 'pending' OR previews.started_at < now() - make_interval(secs => $3)`, src.sha, kind, pendingFor.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		go s.convert(src, kind)
	}
	metrics.PreviewRequests.WithLabelValues(kind, "pending").Inc()
	httpx.JSON(w, http.StatusAccepted, map[string]string{"state": "pending"})
	return nil
}

func (s *Service) max() int64 {
	if s.Cfg.MaxBytes > 0 {
		return s.Cfg.MaxBytes
	}
	return 25 << 20
}

// source finds the file the user may read: an own attachment or a file of
// the own space — the same rule as their content (PRV-14).
func (s *Service) source(ctx context.Context, uid uuid.UUID, q url.Values) (source, error) {
	var src source
	if id := q.Get("attachment"); id != "" {
		aid, err := uuid.Parse(id)
		if err != nil {
			return src, apperr.NotFound("not_found", "attachment not found")
		}
		var sha *string
		err = s.Pool.QueryRow(ctx, `SELECT file_name, size_bytes, s3_key, sha256 FROM attachments WHERE id = $1 AND user_id = $2`, aid, uid).
			Scan(&src.name, &src.size, &src.key, &sha)
		if err != nil {
			return src, apperr.NotFound("not_found", "attachment not found")
		}
		if sha != nil {
			src.sha = *sha
		}
		src.attachment = aid
		return src, nil
	}
	p, err := space.CleanPath(q.Get("path"))
	if err != nil {
		return src, apperr.NotFound("not_found", "file not found")
	}
	err = s.Pool.QueryRow(ctx, `SELECT size, sha256 FROM space_files WHERE user_id = $1 AND path = $2`, uid, p).Scan(&src.size, &src.sha)
	if err != nil {
		return src, apperr.NotFound("not_found", "file not found")
	}
	src.name, src.key = path.Base(p), space.Key(uid, p)
	return src, nil
}

// hash computes and stores the hash of an attachment saved before it was kept.
func (s *Service) hash(ctx context.Context, src source) (string, error) {
	rc, err := s.S3.Get(ctx, src.key)
	if err != nil {
		return "", apperr.NotFound("not_found", "file not found")
	}
	defer rc.Close()
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(rc, s.max()+1)); err != nil {
		return "", err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if src.attachment != uuid.Nil {
		_, _ = s.Pool.Exec(ctx, `UPDATE attachments SET sha256 = $2 WHERE id = $1`, src.attachment, sum)
	}
	return sum, nil
}

func (s *Service) send(ctx context.Context, w http.ResponseWriter, sha, kind string, shown, total *int) error {
	ext, ctype := ".pdf", "application/pdf"
	if kind == "sheet" {
		ext, ctype = ".json", "application/json"
	}
	rc, err := s.S3.Get(ctx, cachePrefix+sha+ext)
	if err != nil {
		// the object is gone (a sweep in between): make it again on the next open
		_, _ = s.Pool.Exec(ctx, `DELETE FROM previews WHERE sha256 = $1`, sha)
		httpx.JSON(w, http.StatusAccepted, map[string]string{"state": "pending"})
		return nil
	}
	defer rc.Close()
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Cache-Control", "private, max-age=3600")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "sandbox")
	if kind == "doc" {
		pages := "?"
		if total != nil {
			pages = strconv.Itoa(*total)
		}
		n := 0
		if shown != nil {
			n = *shown
		}
		w.Header().Set("X-Preview-Pages", fmt.Sprintf("%d/%s", n, pages))
	}
	_, err = io.Copy(w, rc)
	return err
}

// ─── Conversion ─────────────────────────────────────────────────────

var errBusy = errors.New("the converter is busy")

func (s *Service) convert(src source, kind string) {
	s.once.Do(func() {
		n := s.Cfg.Parallel
		if n <= 0 {
			n = 2
		}
		s.sem = make(chan struct{}, n)
	})
	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()
	start := time.Now()
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		s.fail(src.sha, kind, "timeout")
		return
	}
	data, err := s.read(ctx, src.key)
	if err != nil {
		s.fail(src.sha, kind, "broken")
		return
	}
	var (
		out          []byte
		shown, total *int
	)
	if kind == "doc" {
		out, err = s.toPDF(ctx, src.name, data)
		if err == nil {
			n := countPages(out)
			shown, total = &n, docPages(src.name, data)
		}
	} else {
		out, err = s.sheets(ctx, src.name, data)
	}
	switch {
	case errors.Is(err, errBusy):
		s.fail(src.sha, kind, "busy")
		return
	case ctx.Err() != nil:
		s.fail(src.sha, kind, "timeout")
		return
	case err != nil:
		slog.Warn("preview", "kind", kind, "err", err)
		s.fail(src.sha, kind, "broken")
		return
	}
	ext, ctype := ".pdf", "application/pdf"
	if kind == "sheet" {
		ext, ctype = ".json", "application/json"
	}
	if err := s.S3.Put(ctx, cachePrefix+src.sha+ext, bytes.NewReader(out), int64(len(out)), ctype); err != nil {
		s.fail(src.sha, kind, "broken")
		return
	}
	_, _ = s.Pool.Exec(context.Background(), `UPDATE previews SET state = 'ready', pages_shown = $2, pages_total = $3, bytes = $4, ready_at = now(), accessed_at = now()
		WHERE sha256 = $1`, src.sha, shown, total, len(out))
	metrics.PreviewSeconds.WithLabelValues(kind, "miss").Observe(time.Since(start).Seconds())
}

// fail records why a preview is not available; a busy converter may be asked
// again at once, other failures — after an hour.
func (s *Service) fail(sha, kind, reason string) {
	expires := time.Now().Add(errorKeep)
	if reason == "busy" {
		expires = time.Now()
	}
	_, _ = s.Pool.Exec(context.Background(), `UPDATE previews SET state = 'error', reason = $2, expires_at = $3 WHERE sha256 = $1`, sha, reason, expires)
	metrics.PreviewRequests.WithLabelValues(kind, reason).Inc()
}

func (s *Service) read(ctx context.Context, key string) ([]byte, error) {
	rc, err := s.S3.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(io.LimitReader(rc, s.max()+1))
}

func (s *Service) client() *http.Client {
	if s.HTTP != nil {
		return s.HTTP
	}
	return http.DefaultClient
}

// toPDF converts the first pages of a document with LibreOffice (Gotenberg).
func (s *Service) toPDF(ctx context.Context, name string, data []byte) ([]byte, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("files", safeName(name))
	if err != nil {
		return nil, err
	}
	_, _ = fw.Write(data)
	_ = mw.WriteField("nativePageRanges", fmt.Sprintf("1-%d", pagesShown))
	_ = mw.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.Cfg.ConvertURL, "/")+"/forms/libreoffice/convert", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.SetBasicAuth(s.Cfg.ConvertUser, s.Cfg.ConvertPassword)
	return s.do(req, 100<<20)
}

// sheets reads the sheets of a workbook through docgen; XLS and ODS become
// XLSX in the legacy sidecar first.
func (s *Service) sheets(ctx context.Context, name string, data []byte) ([]byte, error) {
	ext := strings.ToLower(path.Ext(name))
	if ext == ".xls" || ext == ".ods" {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.Cfg.LegacyURL, "/")+"/convert?to=xlsx&name="+url.QueryEscape(safeName(name)), bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		req.SetBasicAuth(s.Cfg.ConvertUser, s.Cfg.ConvertPassword)
		if data, err = s.do(req, 100<<20); err != nil {
			return nil, err
		}
	}
	pre, ok := s.S3.(storage.Presigner)
	if !ok {
		return nil, errors.New("the object store gives no links")
	}
	key := cachePrefix + "tmp/" + uuid.NewString() + ".xlsx"
	if err := s.S3.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		return nil, err
	}
	defer func() { _ = s.S3.Delete(context.Background(), key) }()
	u, err := pre.PresignGet(ctx, key, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	b, _ := json.Marshal(map[string]any{"url": u, "maxRows": sheetRows, "maxCols": sheetCols})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.Cfg.DocgenURL, "/")+"/internal/xlsx-preview", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.Cfg.DocgenKey)
	return s.do(req, 32<<20)
}

func (s *Service) do(req *http.Request, limit int64) ([]byte, error) {
	resp, err := s.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests:
		return nil, errBusy
	case resp.StatusCode/100 != 2:
		return nil, fmt.Errorf("%s: HTTP %d: %s", req.URL.Path, resp.StatusCode, strings.TrimSpace(string(b[:min(len(b), 200)])))
	}
	return b, nil
}

func safeName(name string) string {
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	if name == "" || name == "." || name == "/" {
		return "file"
	}
	return name
}

var pageRe = regexp.MustCompile(`/Type\s*/Page[^s]`)

// countPages counts the page objects of a PDF made by LibreOffice.
func countPages(pdf []byte) int { return len(pageRe.FindAll(pdf, -1)) }

var (
	pagesRe  = regexp.MustCompile(`<Pages>(\d+)</Pages>`)
	slidesRe = regexp.MustCompile(`<Slides>(\d+)</Slides>`)
)

// docPages reads the number of pages (DOCX) or slides (PPTX) Word and
// PowerPoint wrote into docProps/app.xml; nil for other formats.
func docPages(name string, data []byte) *int {
	ext := strings.ToLower(path.Ext(name))
	if ext != ".docx" && ext != ".pptx" && ext != ".docm" && ext != ".pptm" {
		return nil
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	for _, f := range zr.File {
		if f.Name != "docProps/app.xml" {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil
		}
		b, _ := io.ReadAll(io.LimitReader(rc, 1<<20))
		rc.Close()
		re := pagesRe
		if strings.HasPrefix(ext, ".ppt") {
			re = slidesRe
		}
		if m := re.FindSubmatch(b); m != nil {
			if n, err := strconv.Atoi(string(m[1])); err == nil && n > 0 {
				return &n
			}
		}
	}
	return nil
}

// Sweep deletes previews unused for 30 days and old errors (tech §6.3).
func (s *Service) Sweep(ctx context.Context) (int, error) {
	rows, err := s.Pool.Query(ctx, `DELETE FROM previews WHERE (state = 'ready' AND accessed_at < now() - make_interval(secs => $1))
		OR (state = 'error' AND expires_at < now() - interval '1 day') OR (state = 'pending' AND started_at < now() - interval '1 hour')
		RETURNING sha256, kind`, keepUnused.Seconds())
	if err != nil {
		return 0, err
	}
	type gone struct{ sha, kind string }
	var list []gone
	for rows.Next() {
		var g gone
		if rows.Scan(&g.sha, &g.kind) == nil {
			list = append(list, g)
		}
	}
	rows.Close()
	for _, g := range list {
		ext := ".pdf"
		if g.kind == "sheet" {
			ext = ".json"
		}
		_ = s.S3.Delete(ctx, cachePrefix+g.sha+ext)
	}
	return len(list), rows.Err()
}
