// Package telemetry sets up OpenTelemetry tracing with OTLP export.
package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the shared tracer for manual spans.
var Tracer trace.Tracer = otel.Tracer("nabu")

// Setup installs a tracer provider. Without an endpoint, spans are still created
// (so trace_id appears in logs) but never exported.
func Setup(ctx context.Context, endpoint, mode string) (func(context.Context) error, error) {
	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName("nabu-"+mode)))
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if endpoint != "" {
		// The exporter reads OTEL_EXPORTER_OTLP_ENDPOINT itself.
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	Tracer = tp.Tracer("nabu")
	return tp.Shutdown, nil
}

// Start opens a span with the shared tracer.
func Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name)
}
