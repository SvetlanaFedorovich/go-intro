package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpapi "github.com/AntonYurchenko/go-intro/internal/api"
	"github.com/AntonYurchenko/go-intro/internal/config"
	"github.com/AntonYurchenko/go-intro/internal/health"
	"github.com/AntonYurchenko/go-intro/internal/kafka"
	"github.com/AntonYurchenko/go-intro/internal/observability"
	"github.com/AntonYurchenko/go-intro/internal/retry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func Run() error {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "api")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	retryPolicy, err := retry.ParsePolicy(cfg.RetryMaxAttempts, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	if err != nil {
		return fmt.Errorf("retry config: %w", err)
	}
	shutdownTracing, err := observability.SetupTracing(ctx, "go-intro-api", cfg.OTLPEndpoint, cfg.TraceSampleRatio)
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			logger.Error("shutdown tracing", "error", err)
		}
	}()

	producer := kafka.NewProducerWithRetry(cfg.KafkaBrokers, cfg.KafkaTopic, retryPolicy)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("close producer", "error", err)
		}
	}()

	// Один mux: бизнес-роуты (/data) + пробы (/healthz, /readyz).
	mux := http.NewServeMux()
	dataHandler := httpapi.NewHandler(producer, logger).Routes()
	dataHandler = observability.HTTPMiddleware("/data", dataHandler)
	dataHandler = otelhttp.NewHandler(dataHandler, "POST /data")
	dataHandler = observability.RequestIDMiddleware(dataHandler)
	mux.Handle("/data", dataHandler)
	health.New(logger, 2*time.Second,
		health.Checker{Name: "kafka", Check: producer.Ping},
	).Register(mux)
	mux.Handle("/metrics", observability.MetricsHandler())

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return fmt.Errorf("listen: %w", err)
	}
}
