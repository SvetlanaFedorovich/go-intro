package observability

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestRequestIDMiddleware(t *testing.T) {
	var requestID string
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID = RequestID(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(RequestIDHeader, "client-request-42")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if requestID != "client-request-42" {
		t.Fatalf("context request id = %q", requestID)
	}
	if got := rec.Header().Get(RequestIDHeader); got != requestID {
		t.Fatalf("response request id = %q, want %q", got, requestID)
	}
}

func TestRequestIDMiddlewareReplacesInvalidID(t *testing.T) {
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if RequestID(r.Context()) == "" {
			t.Fatal("request id is empty")
		}
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(RequestIDHeader, "contains spaces")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get(RequestIDHeader); got == "" || got == "contains spaces" {
		t.Fatalf("generated request id = %q", got)
	}
}

func TestContextWithRequestIDRejectsInvalidValue(t *testing.T) {
	ctx := ContextWithRequestID(context.Background(), "contains spaces")
	if got := RequestID(ctx); got != "" {
		t.Fatalf("request ID = %q, want empty", got)
	}
}

func TestLoggerIncludesCorrelationFields(t *testing.T) {
	var output bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&output, nil))
	traceID, err := trace.TraceIDFromHex("70f5bc9a7e324cf0bcb631e5df422f9e")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("6e0c63257de34c92")
	if err != nil {
		t.Fatal(err)
	}
	ctx := ContextWithRequestID(context.Background(), "request-42")
	ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	Logger(ctx, base).Info("test")
	logged := output.String()
	if !strings.Contains(logged, `"request_id":"request-42"`) {
		t.Fatalf("log has no request ID: %s", logged)
	}
	if !strings.Contains(logged, `"trace_id":"70f5bc9a7e324cf0bcb631e5df422f9e"`) {
		t.Fatalf("log has no trace ID: %s", logged)
	}
}
