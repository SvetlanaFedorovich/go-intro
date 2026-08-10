package store

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/model"
	"github.com/AntonYurchenko/go-intro/internal/observability"
	"github.com/AntonYurchenko/go-intro/internal/retry"
	"github.com/jackc/pgx/v5/pgconn"
)

type retryStore struct {
	next   DataStore
	policy retry.Policy
}

func (s *retryStore) InsertOnce(
	ctx context.Context,
	eventID, topic string,
	partition int,
	offset int64,
	data model.Data,
) (bool, error) {
	var inserted bool
	err := retry.Do(ctx, postgresRetryPolicy(s.policy, "postgres_insert"), func() error {
		var err error
		inserted, err = s.next.InsertOnce(ctx, eventID, topic, partition, offset, data)
		return err
	})
	return inserted, err
}

func (s *retryStore) CleanupProcessed(ctx context.Context, before time.Time) (int64, error) {
	var deleted int64
	err := retry.Do(ctx, postgresRetryPolicy(s.policy, "postgres_cleanup"), func() error {
		var err error
		deleted, err = s.next.CleanupProcessed(ctx, before)
		return err
	})
	return deleted, err
}

func (s *retryStore) Ping(ctx context.Context) error {
	return retry.Do(ctx, postgresRetryPolicy(s.policy, "postgres_ping"), func() error {
		return s.next.Ping(ctx)
	})
}

func (s *retryStore) Close() {
	s.next.Close()
}

func postgresRetryPolicy(policy retry.Policy, operation string) retry.Policy {
	onRetry := policy.OnRetry
	policy.Retryable = isRetryablePostgres
	policy.OnRetry = func(attempt int, delay time.Duration, err error) {
		observability.ObserveRetry(operation)
		if onRetry != nil {
			onRetry(attempt, delay, err)
		}
	}
	return policy
}

func isRetryablePostgres(err error) bool {
	if err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrEventConflict) {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		code := pgErr.Code
		return strings.HasPrefix(code, "08") ||
			strings.HasPrefix(code, "40") ||
			strings.HasPrefix(code, "53") ||
			code == "57P01" ||
			code == "57P02" ||
			code == "57P03"
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// pgx/gorm connection failures can be wrapped in driver-specific types.
	// Unknown non-SQL errors are retried, but only within the bounded policy.
	return true
}
