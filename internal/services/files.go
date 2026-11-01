package services

// The working copy of a run and its changes (FTR.HMR.CMN-0007 R5, R8, R9,
// R13, tech §2): the caller uploads a tar.gz of the copy, the run's sandbox
// unpacks it and, after the agent, uploads what changed; the caller takes the
// changes within RUN_FILES_KEEP.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
	"github.com/GreenOnGrey/nabu-core/internal/runfiles"
)

// FilesConfig bounds working copies (tech §10).
type FilesConfig struct {
	MaxFiles  int           // RUN_FILES_MAX_FILES, 5000
	MaxBytes  int64         // RUN_FILES_MAX_BYTES, 200 MB unpacked
	UploadTTL time.Duration // RUN_FILES_UPLOAD_TTL, 1 h
	Keep      time.Duration // RUN_FILES_KEEP, 24 h
}

// Files stores working copies and changes of runs in S3.
type Files struct {
	Store storage.Storage
	Cfg   FilesConfig
}

const (
	uploadsPrefix = "run-files/uploads/"
	runPrefix     = "run-files/"
	// ChangesManifest is the list of changes inside the archive of changes.
	ChangesManifest = "changes.json"
)

// put streams r to S3 under key while CheckArchive reads the same bytes; a
// failed check aborts the upload and removes the object.
func (f *Files) put(ctx context.Context, key string, r io.Reader, maxFiles int) (runfiles.Stats, error) {
	pr, pw := io.Pipe()
	type result struct {
		st  runfiles.Stats
		err error
	}
	done := make(chan result, 1)
	go func() {
		st, err := runfiles.Check(pr, maxFiles, f.Cfg.MaxBytes)
		if err != nil {
			pr.CloseWithError(err)
		} else {
			_, _ = io.Copy(io.Discard, pr) // anything after the gzip stream
		}
		done <- result{st, err}
	}()
	perr := f.Store.Put(ctx, key, io.TeeReader(r, pw), -1, "application/gzip")
	pw.CloseWithError(perr)
	res := <-done
	if res.err != nil || perr != nil {
		_ = f.Store.Delete(context.WithoutCancel(ctx), key)
	}
	if res.err != nil {
		var tl *runfiles.TooLargeError
		if errors.As(res.err, &tl) {
			return res.st, apperr.TooLarge("workspace_too_large", tl.Error()).
				With("files", tl.Stats.Files).With("bytes", tl.Stats.Bytes).With("maxFiles", tl.MaxFiles).
				With("maxBytes", tl.MaxBytes).With("partial", true)
		}
		return res.st, apperr.BadRequest("bad_archive", res.err.Error())
	}
	return res.st, perr
}

// Upload stores a working copy of a client (tech §2.1). A repeated id
// replaces the upload until a run takes it.
func (s *Service) Upload(ctx context.Context, clientID, id uuid.UUID, body io.Reader) (*UploadInfo, error) {
	var owner uuid.UUID
	var run *uuid.UUID
	err := s.Pool.QueryRow(ctx, `SELECT client_id, run_id FROM run_uploads WHERE id = $1`, id).Scan(&owner, &run)
	switch {
	case err == nil && (owner != clientID || run != nil):
		return nil, apperr.Conflict("upload_in_use", "the upload belongs to a run")
	case err != nil && !postgres.IsNoRows(err):
		return nil, err
	}
	key := uploadsPrefix + clientID.String() + "/" + id.String() + ".tar.gz"
	st, err := s.Files.put(ctx, key, body, s.Files.Cfg.MaxFiles)
	if err != nil {
		_, _ = s.Pool.Exec(context.WithoutCancel(ctx), `DELETE FROM run_uploads WHERE id = $1 AND run_id IS NULL`, id)
		return nil, err
	}
	if _, err := s.Pool.Exec(ctx, `INSERT INTO run_uploads (id, client_id, key, files, bytes) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (id) DO UPDATE SET key = EXCLUDED.key, files = EXCLUDED.files, bytes = EXCLUDED.bytes, created_at = now()`,
		id, clientID, key, st.Files, st.Bytes); err != nil {
		return nil, err
	}
	metrics.RunFilesBytes.WithLabelValues("in").Observe(float64(st.Bytes))
	return &UploadInfo{ID: id, Files: st.Files, Bytes: st.Bytes, ExpiresAt: time.Now().Add(s.Files.Cfg.UploadTTL)}, nil
}

// UploadInfo is the answer of PUT /client/v1/uploads/{id}.
type UploadInfo struct {
	ID        uuid.UUID `json:"id"`
	Files     int       `json:"files"`
	Bytes     int64     `json:"bytes"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// attachFiles gives the upload to a new run in the transaction of its insert.
func (s *Service) attachFiles(ctx context.Context, tx pgx.Tx, clientID, runID uuid.UUID, upload string) error {
	id, err := uuid.Parse(upload)
	if err != nil {
		return apperr.NotFound("upload_not_found", "no such upload").With("field", "files")
	}
	var key string
	var files int
	var bytes int64
	err = tx.QueryRow(ctx, `UPDATE run_uploads SET run_id = $3 WHERE id = $1 AND client_id = $2 AND run_id IS NULL
		AND created_at > now() - make_interval(secs => $4) RETURNING key, files, bytes`,
		id, clientID, runID, s.Files.Cfg.UploadTTL.Seconds()).Scan(&key, &files, &bytes)
	if postgres.IsNoRows(err) {
		return apperr.NotFound("upload_not_found", "no such upload").With("field", "files")
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE runs SET files_key = $2, files_count = $3, files_bytes = $4 WHERE id = $1`, runID, key, files, bytes)
	return err
}

// moveFiles moves the upload of a new run to run-files/<run>/in.tar.gz
// (tech §2.2) before the run is queued, so the uploads hold only what no run
// took. The run keeps the upload key if the copy fails: it still works.
func (s *Service) moveFiles(ctx context.Context, runID uuid.UUID) {
	var src string
	if err := s.Pool.QueryRow(ctx, `SELECT files_key FROM runs WHERE id = $1`, runID).Scan(&src); err != nil || !strings.HasPrefix(src, uploadsPrefix) {
		return
	}
	dst := runPrefix + runID.String() + "/in.tar.gz"
	if err := storage.Copy(ctx, s.Files.Store, src, dst, "application/gzip"); err != nil {
		slog.WarnContext(ctx, "move run files", "run", runID, "err", err)
		return
	}
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE runs SET files_key = $2 WHERE id = $1`, runID, dst); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE run_uploads SET key = $2 WHERE run_id = $1`, runID, dst)
		return err
	})
	if err != nil {
		_ = s.Files.Store.Delete(context.WithoutCancel(ctx), dst)
		return
	}
	_ = s.Files.Store.Delete(ctx, src)
}

// dropUpload removes an upload no run took: the one a repeat of a run
// brought (tech §2.2).
func (s *Service) dropUpload(ctx context.Context, clientID uuid.UUID, upload string) {
	id, err := uuid.Parse(upload)
	if err != nil {
		return
	}
	var key string
	if s.Pool.QueryRow(ctx, `DELETE FROM run_uploads WHERE id = $1 AND client_id = $2 AND run_id IS NULL RETURNING key`, id, clientID).Scan(&key) == nil {
		_ = s.Files.Store.Delete(ctx, key)
	}
}

// Changes opens the archive of changes of a client's run (tech §2.3).
func (s *Service) Changes(ctx context.Context, clientID, runID uuid.UUID) (io.ReadCloser, int, error) {
	var owner uuid.UUID
	var status string
	var filesKey, changesKey *string
	var count *int
	var expired bool
	err := s.Pool.QueryRow(ctx, `SELECT client_id, status, files_key, changes_key, changes_count, files_expired FROM runs WHERE id = $1`, runID).
		Scan(&owner, &status, &filesKey, &changesKey, &count, &expired)
	if postgres.IsNoRows(err) || (err == nil && owner != clientID) {
		return nil, 0, apperr.NotFound("not_found", "run not found")
	}
	if err != nil {
		return nil, 0, err
	}
	switch {
	case status == "queued" || status == "running":
		return nil, 0, apperr.Conflict("run_not_finished", "the run is not finished")
	case expired:
		return nil, 0, apperr.New(http.StatusGone, "expired", "the files of the run are deleted")
	case filesKey == nil || changesKey == nil:
		return nil, 0, apperr.NotFound("no_changes", "the run has no changes")
	}
	rc, err := s.Files.Store.Get(ctx, *changesKey)
	if err != nil {
		return nil, 0, err
	}
	n := 0
	if count != nil {
		n = *count
	}
	return rc, n, nil
}

// RunFiles opens the working copy of a run for its sandbox (tech §2.4).
func (s *Service) RunFiles(ctx context.Context, runID uuid.UUID) (io.ReadCloser, error) {
	var key *string
	err := s.Pool.QueryRow(ctx, `SELECT files_key FROM runs WHERE id = $1 AND NOT files_expired`, runID).Scan(&key)
	if postgres.IsNoRows(err) || (err == nil && key == nil) {
		return nil, apperr.NotFound("no_files", "the run has no working copy")
	}
	if err != nil {
		return nil, err
	}
	return s.Files.Store.Get(ctx, *key)
}

// PutChanges stores the changes the sandbox of a run uploads (tech §2.4);
// the engine stores their count with SetChangesCount.
func (s *Service) PutChanges(ctx context.Context, runID uuid.UUID, body io.Reader) error {
	key := runPrefix + runID.String() + "/out.tar.gz"
	st, err := s.Files.put(ctx, key, body, 0)
	if err != nil {
		return err
	}
	metrics.RunFilesBytes.WithLabelValues("out").Observe(float64(st.Bytes))
	_, err = s.Pool.Exec(ctx, `UPDATE runs SET changes_key = $2 WHERE id = $1`, runID, key)
	return err
}

// SetChangesCount stores the number of changes the sandbox reported.
func (s *Service) SetChangesCount(ctx context.Context, runID uuid.UUID, n int) {
	metrics.RunChangesFiles.Observe(float64(n))
	_, _ = s.Pool.Exec(ctx, `UPDATE runs SET changes_count = $2 WHERE id = $1 AND changes_key IS NOT NULL`, runID, n)
}

// SweepFiles removes forgotten uploads and the files of old runs (tech §2.7).
func (s *Service) SweepFiles(ctx context.Context) {
	rows, err := s.Pool.Query(ctx, `DELETE FROM run_uploads WHERE run_id IS NULL AND created_at < now() - make_interval(secs => $1) RETURNING key`,
		s.Files.Cfg.UploadTTL.Seconds())
	if err == nil {
		keys, _ := pgx.CollectRows(rows, pgx.RowTo[string])
		for _, k := range keys {
			_ = s.Files.Store.Delete(ctx, k)
		}
	}
	rows, err = s.Pool.Query(ctx, `UPDATE runs SET files_expired = true WHERE files_key IS NOT NULL AND NOT files_expired
		AND finished_at < now() - make_interval(secs => $1) RETURNING files_key, coalesce(changes_key, '')`, s.Files.Cfg.Keep.Seconds())
	if err != nil {
		slog.WarnContext(ctx, "sweep run files", "err", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var in, out string
		if rows.Scan(&in, &out) != nil {
			continue
		}
		for _, k := range []string{in, out} {
			if k != "" {
				_ = s.Files.Store.Delete(ctx, k)
			}
		}
	}
	_, _ = s.Pool.Exec(ctx, `DELETE FROM run_uploads WHERE run_id IN (SELECT id FROM runs WHERE files_expired)`)
}

// FilesRoutes mounts the uploads and the changes of the client API.
func (s *Service) FilesRoutes(r chi.Router) {
	r.Put("/uploads/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		info, err := s.Upload(r.Context(), httpx.ClientFrom(r.Context()).ID, id, r.Body)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, info)
		return nil
	}))
	r.Get("/runs/{id}/changes", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		rc, n, err := s.Changes(r.Context(), httpx.ClientFrom(r.Context()).ID, id)
		if err != nil {
			return err
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Nabu-Changes-Files", fmt.Sprint(n))
		_, err = io.Copy(w, rc)
		return err
	}))
}

// InternalRoutes mount the files of a run for its sandbox (tech §2.4): the
// run token of the sandbox (aud nabu:sandbox, sub run) names the run.
func (s *Service) InternalRoutes(r chi.Router, signer *jwt.Signer) {
	runOf := func(r *http.Request) (uuid.UUID, error) {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return uuid.Nil, err
		}
		c, err := signer.Verify(httpx.Bearer(r), jwt.AudSandbox)
		if err != nil || c.Subject != "run" || c.Run != id.String() {
			return uuid.Nil, apperr.Forbidden("forbidden", "the run token of this run is required")
		}
		return id, nil
	}
	r.Get("/internal/v1/sandbox/runs/{id}/files", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := runOf(r)
		if err != nil {
			return err
		}
		rc, err := s.RunFiles(r.Context(), id)
		if err != nil {
			return err
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/gzip")
		_, err = io.Copy(w, rc)
		return err
	}))
	r.Put("/internal/v1/sandbox/runs/{id}/changes", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := runOf(r)
		if err != nil {
			return err
		}
		if err := s.PutChanges(r.Context(), id, r.Body); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}
