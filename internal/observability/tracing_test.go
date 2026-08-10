package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestSetupTracingEmptyEndpointIsNoOp(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), "test", "", 1)
	if err != nil {
		t.Fatalf("SetupTracing: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown is nil")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestInjectExtractTraceRoundTrip(t *testing.T) {
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(oldPropagator) })

	traceID, err := trace.TraceIDFromHex("70f5bc9a7e324cf0bcb631e5df422f9e")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("6e0c63257de34c92")
	if err != nil {
		t.Fatal(err)
	}
	source := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), source)
	headers := map[string]string{}

	InjectTrace(ctx, headers)
	if headers["traceparent"] == "" {
		t.Fatal("traceparent is empty")
	}
	extracted := trace.SpanContextFromContext(ExtractTrace(context.Background(), headers))
	if extracted.TraceID() != traceID {
		t.Fatalf("trace ID = %s, want %s", extracted.TraceID(), traceID)
	}
	if !extracted.IsRemote() {
		t.Fatal("extracted span context is not remote")
	}
}
