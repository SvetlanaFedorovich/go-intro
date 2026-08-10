package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func SetupTracing(ctx context.Context, serviceName, endpoint string, sampleRatio float64) (func(context.Context) error, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes("", attribute.String("service.name", serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("create trace resource: %w", err)
	}

	if sampleRatio < 0 {
		sampleRatio = 0
	}
	if sampleRatio > 1 {
		sampleRatio = 1
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return provider.Shutdown, nil
}

func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

func InjectTrace(ctx context.Context, headers map[string]string) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
}

func ExtractTrace(ctx context.Context, headers map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}
