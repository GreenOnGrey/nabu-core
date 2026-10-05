// Package logging configures JSON logs with trace and request context.
package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

type ctxKey int

const fieldsKey ctxKey = 0

// With returns a context whose log records carry the extra attributes
// (user_id, feature, request_id).
func With(ctx context.Context, attrs ...slog.Attr) context.Context {
	prev, _ := ctx.Value(fieldsKey).([]slog.Attr)
	next := make([]slog.Attr, 0, len(prev)+len(attrs))
	next = append(next, prev...)
	next = append(next, attrs...)
	return context.WithValue(ctx, fieldsKey, next)
}

// New builds a JSON logger writing to w.
func New(w io.Writer, level string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	return slog.New(&handler{Handler: slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lv})})
}

type handler struct{ slog.Handler }

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	if attrs, ok := ctx.Value(fieldsKey).([]slog.Attr); ok {
		r.AddAttrs(attrs...)
	}
	return h.Handler.Handle(ctx, r)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &handler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{Handler: h.Handler.WithGroup(name)}
}
