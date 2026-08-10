package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/config"
	"github.com/AntonYurchenko/go-intro/internal/health"
	"github.com/AntonYurchenko/go-intro/internal/kafka"
	"github.com/AntonYurchenko/go-intro/internal/model"
	"github.com/AntonYurchenko/go-intro/internal/observability"
	"github.com/AntonYurchenko/go-intro/internal/retry"
	"github.com/AntonYurchenko/go-intro/internal/store"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	workerPollInterval = 10 * time.Millisecond
	workerBatchLimit   = 1000
	workerConcurrency  = 16
)

type batchConsumer interface {
	FetchBatch(ctx context.Context, max int) ([]kafka.Message, error)
	Commit(ctx context.Context, messages ...kafka.Message) error
}

type dlqPublisher interface {
	PublishWithHeaders(ctx context.Context, value []byte, headers []kafkago.Header) error
}

func Run() error {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "worker")

	eventRetention, err := positiveDuration("EVENT_RETENTION", cfg.EventRetention)
	if err != nil {
		return err
	}
	cleanupInterval, err := positiveDuration("EVENT_CLEANUP_INTERVAL", cfg.EventCleanupInterval)
	if err != nil {
		return err
	}
	retryPolicy, err := retry.ParsePolicy(cfg.RetryMaxAttempts, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	if err != nil {
		return fmt.Errorf("retry config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, "go-intro-worker", cfg.OTLPEndpoint, cfg.TraceSampleRatio)
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

	db, err := store.NewWithRetry(ctx, cfg.StoreDriver, cfg.PostgresDSN, retryPolicy)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer db.Close()

	consumer := kafka.NewConsumerWithRetry(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, retryPolicy)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("close consumer", "error", err)
		}
	}()

	dlq := kafka.NewProducerWithRetry(cfg.KafkaBrokers, cfg.KafkaDLQTopic, retryPolicy)
	defer func() {
		if err := dlq.Close(); err != nil {
			logger.Error("close dlq producer", "error", err)
		}
	}()

	// Worker не обслуживает бизнес-трафик по HTTP, но health-пробы нужны
	// оркестратору. Поднимаем отдельный лёгкий HTTP-сервер только для проб.
	healthSrv := startHealthServer(cfg.HealthAddr, logger, db, consumer)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(shutdownCtx)
	}()

	logger.Info("started",
		"store", cfg.StoreDriver,
		"topic", cfg.KafkaTopic,
		"dlq", cfg.KafkaDLQTopic,
		"health_addr", cfg.HealthAddr,
		"event_retention", eventRetention,
		"cleanup_interval", cleanupInterval,
	)

	ticker := time.NewTicker(workerPollInterval)
	defer ticker.Stop()
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down")
			return nil
		case <-ticker.C:
			if err := processBatch(ctx, consumer, dlq, db, logger); err != nil {
				logger.Error("batch", "error", err)
				observability.ObserveRetry("batch")
			}
			lagCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			lags, err := consumer.Lags(lagCtx)
			cancel()
			if err != nil {
				logger.Warn("measure consumer lag", "error", err)
			} else {
				for _, lag := range lags {
					observability.SetConsumerLag(lag.Topic, strconv.Itoa(lag.Partition), lag.Lag)
				}
			}
		case <-cleanupTicker.C:
			cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			deleted, err := db.CleanupProcessed(cleanupCtx, time.Now().UTC().Add(-eventRetention))
			cancel()
			if err != nil {
				logger.Error("cleanup processed events", "error", err)
				continue
			}
			if deleted > 0 {
				logger.Info("cleaned processed events", "deleted", deleted)
				observability.AddProcessedCleanup(deleted)
			}
		}
	}
}

func positiveDuration(name, value string) (time.Duration, error) {
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %q", name, value)
	}
	return d, nil
}

// startHealthServer поднимает HTTP-сервер с /healthz и /readyz в отдельной горутине.
// readiness проверяет доступность БД и Kafka.
func startHealthServer(addr string, logger *slog.Logger, db store.DataStore, consumer *kafka.Consumer) *http.Server {
	mux := http.NewServeMux()
	health.New(logger, 2*time.Second,
		health.Checker{Name: "postgres", Check: db.Ping},
		health.Checker{Name: "kafka", Check: consumer.Ping},
	).Register(mux)
	mux.Handle("/metrics", observability.MetricsHandler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("health server", "error", err)
		}
	}()
	return srv
}

func processBatch(
	ctx context.Context,
	consumer batchConsumer,
	dlq dlqPublisher,
	db store.DataStore,
	logger *slog.Logger,
) error {
	started := time.Now()
	pollCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()

	// FetchBatch может вернуть уже вычитанные сообщения ВМЕСТЕ с ошибкой сети.
	// Не выбрасываем их: сначала обрабатываем то, что успели получить,
	// затем возвращаем fetchErr (её залогирует вызывающий).
	messages, fetchErr := consumer.FetchBatch(pollCtx, workerBatchLimit)
	defer observability.ObserveBatch(len(messages), started)
	if len(messages) == 0 {
		return fetchErr
	}

	logger.Debug("fetched messages", "count", len(messages))

	// Обрабатываем batch параллельно: store и kafka.Writer безопасны для
	// concurrent use. Ошибки храним по исходному индексу, чтобы offset можно
	// было коммитить только до первого неуспешного сообщения.
	messageErrors := make([]error, len(messages))
	sem := make(chan struct{}, workerConcurrency)
	var wg sync.WaitGroup
	for i := range messages {
		wg.Add(1)
		sem <- struct{}{}
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }()
			messageErrors[index] = handleMessage(ctx, dlq, db, logger, messages[index])
		}(i)
	}
	wg.Wait()

	lastOK := -1
	var firstErr error
	for i, err := range messageErrors {
		if err != nil {
			firstErr = err
			break
		}
		lastOK = i
	}
	if lastOK >= 0 {
		if err := consumer.Commit(ctx, partitionOffsets(messages[:lastOK+1])...); err != nil {
			if firstErr != nil {
				return fmt.Errorf("handle: %w; commit prefix: %v", firstErr, err)
			}
			return err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return fetchErr
}

func partitionOffsets(messages []kafka.Message) []kafka.Message {
	type partitionKey struct {
		topic     string
		partition int
	}
	latest := make(map[partitionKey]kafka.Message)
	for _, message := range messages {
		key := partitionKey{topic: message.Topic, partition: message.Partition}
		current, ok := latest[key]
		if !ok || message.Offset > current.Offset {
			latest[key] = message
		}
	}
	offsets := make([]kafka.Message, 0, len(latest))
	for _, message := range latest {
		offsets = append(offsets, message)
	}
	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i].Topic != offsets[j].Topic {
			return offsets[i].Topic < offsets[j].Topic
		}
		return offsets[i].Partition < offsets[j].Partition
	})
	return offsets
}

func handleMessage(
	ctx context.Context,
	dlq dlqPublisher,
	db store.DataStore,
	logger *slog.Logger,
	msg kafka.Message,
) error {
	started := time.Now()
	outcome := "unknown"
	defer func() {
		observability.ObserveMessage(outcome, started)
	}()
	defer observability.ObserveEventLatency(msg.Time)
	ctx = observability.ExtractTrace(ctx, msg.Headers)
	ctx = observability.ContextWithRequestID(ctx, msg.Headers[strings.ToLower(observability.RequestIDHeader)])
	ctx, span := observability.Tracer("go-intro/worker").Start(
		ctx,
		"process kafka message",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.Int("messaging.kafka.partition", msg.Partition),
			attribute.Int64("messaging.kafka.message.offset", msg.Offset),
		),
	)
	defer span.End()
	logger = observability.Logger(ctx, logger).With(
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

	var event model.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		outcome = "invalid_json"
		return rejectWithTrace(ctx, span, dlq, logger, msg, "invalid json: "+err.Error())
	}
	if event.EventID == "" {
		// Совместимость с сообщениями старого формата, созданными до event_id.
		event.EventID = fmt.Sprintf("legacy:%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
	}
	if len(event.EventID) > 128 {
		outcome = "invalid_event_id"
		return rejectWithTrace(ctx, span, dlq, logger, msg, "invalid event_id")
	}
	if err := event.Data.Validate(); err != nil {
		outcome = "invalid_payload"
		return rejectWithTrace(ctx, span, dlq, logger, msg, "invalid payload: "+err.Error())
	}

	inserted, err := db.InsertOnce(ctx, event.EventID, msg.Topic, msg.Partition, msg.Offset, event.Data)
	if err != nil {
		if errors.Is(err, store.ErrEventConflict) {
			outcome = "conflict"
			return rejectWithTrace(ctx, span, dlq, logger, msg, err.Error())
		}
		outcome = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, "store event")
		return err
	}
	if inserted {
		outcome = "saved"
		logger.Info("saved", "event_id", event.EventID)
	} else {
		outcome = "duplicate"
		logger.Info("skip duplicate event", "event_id", event.EventID)
	}
	return nil
}

func rejectWithTrace(
	ctx context.Context,
	span trace.Span,
	dlq dlqPublisher,
	logger *slog.Logger,
	msg kafka.Message,
	reason string,
) error {
	err := rejectToDLQ(ctx, dlq, logger, msg, reason)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish DLQ")
		return err
	}
	span.SetStatus(codes.Error, reason)
	return nil
}

func rejectToDLQ(
	ctx context.Context,
	dlq dlqPublisher,
	logger *slog.Logger,
	msg kafka.Message,
	reason string,
) error {
	headers := []kafkago.Header{
		{Key: "error", Value: []byte(reason)},
		{Key: "source_topic", Value: []byte(msg.Topic)},
		{Key: "source_partition", Value: []byte(strconv.Itoa(msg.Partition))},
		{Key: "source_offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
	}
	traceHeaders := map[string]string{}
	if requestID := observability.RequestID(ctx); requestID != "" {
		traceHeaders[strings.ToLower(observability.RequestIDHeader)] = requestID
	}
	observability.InjectTrace(ctx, traceHeaders)
	for name, value := range traceHeaders {
		headers = append(headers, kafkago.Header{Key: name, Value: []byte(value)})
	}

	err := dlq.PublishWithHeaders(ctx, msg.Value, headers)
	observability.ObserveDLQ(err)
	if err != nil {
		return fmt.Errorf("publish dlq: %w", err)
	}
	logger.Warn("sent to dlq", "reason", reason)
	return nil
}
