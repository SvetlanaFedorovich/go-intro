package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

const RequestIDHeader = "X-Request-ID"

type requestIDKey struct{}

func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := normalizeRequestID(r.Header.Get(RequestIDHeader))
		if requestID == "" {
			requestID = randomRequestID()
		}
		w.Header().Set(RequestIDHeader, requestID)
		ctx := context.WithValue(r.Context(), requestIDKey{}, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}

func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	if requestID = normalizeRequestID(requestID); requestID != "" {
		return context.WithValue(ctx, requestIDKey{}, requestID)
	}
	return ctx
}

func Logger(ctx context.Context, base *slog.Logger) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	attrs := make([]any, 0, 4)
	if requestID := RequestID(ctx); requestID != "" {
		attrs = append(attrs, "request_id", requestID)
	}
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		attrs = append(attrs, "trace_id", spanContext.TraceID().String())
	}
	return base.With(attrs...)
}

func normalizeRequestID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return ""
	}
	for _, r := range value {
		if !(r == '-' || r == '_' || r == '.' ||
			r >= '0' && r <= '9' ||
			r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z') {
			return ""
		}
	}
	return value
}

func randomRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "request-id-unavailable"
	}
	return hex.EncodeToString(value[:])
}
