package httpx

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/logging"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Flush keeps SSE working through the wrapper.
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("hijack not supported")
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Observe adds a request id, recovers panics, logs requests and records metrics.
func Observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" {
			reqID = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", reqID)
		info := &reqInfo{}
		ctx := logging.With(context.WithValue(r.Context(), infoKey, info), slog.String("request_id", reqID))
		r = r.WithContext(ctx)
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			if rec := recover(); rec != nil {
				slog.ErrorContext(ctx, "panic", "panic", rec, "stack", string(debug.Stack()))
				if sw.status == 0 {
					JSON(sw, http.StatusInternalServerError, map[string]any{"error": map[string]string{"code": "internal", "message": "internal error"}})
				}
			}
			route := "unmatched"
			if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
				route = rc.RoutePattern()
			}
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			metrics.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(status)).Inc()
			metrics.HTTPDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
			attrs := []any{"method", r.Method, "route", route, "status", status, "duration_ms", time.Since(start).Milliseconds()}
			if info.userID != "" {
				attrs = append(attrs, "user_id", info.userID)
			}
			slog.InfoContext(r.Context(), "http request", attrs...)
		}()
		next.ServeHTTP(sw, r)
	})
}

type reqInfo struct{ userID string }

const infoKey ctxKey = 100

// SetUserForLog records the authenticated user for the access log.
func SetUserForLog(r *http.Request, userID string) {
	if info, ok := r.Context().Value(infoKey).(*reqInfo); ok {
		info.userID = userID
	}
}
