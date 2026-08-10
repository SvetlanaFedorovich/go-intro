package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/model"
	"github.com/AntonYurchenko/go-intro/internal/retry"
)

const (
	DriverPGX  = "pgx"
	DriverGORM = "gorm"
)

var ErrEventConflict = errors.New("event id already exists with different payload")

type DataStore interface {
	InsertOnce(ctx context.Context, eventID, topic string, partition int, offset int64, d model.Data) (inserted bool, err error)
	CleanupProcessed(ctx context.Context, before time.Time) (deleted int64, err error)
	// Ping проверяет доступность БД - используется в readiness-пробе.
	Ping(ctx context.Context) error
	Close()
}

func New(ctx context.Context, driver, dsn string) (DataStore, error) {
	return NewWithRetry(ctx, driver, dsn, retry.DefaultPolicy())
}

func NewWithRetry(ctx context.Context, driver, dsn string, policy retry.Policy) (DataStore, error) {
	var connect func() (DataStore, error)
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "", DriverPGX:
		connect = func() (DataStore, error) { return newPGX(ctx, dsn) }
	case DriverGORM:
		connect = func() (DataStore, error) { return newGORM(ctx, dsn) }
	default:
		return nil, fmt.Errorf("unknown store driver %q (want %q or %q)", driver, DriverPGX, DriverGORM)
	}

	var db DataStore
	connectPolicy := postgresRetryPolicy(policy, "postgres_connect")
	err := retry.Do(ctx, connectPolicy, func() error {
		var err error
		db, err = connect()
		return err
	})
	if err != nil {
		return nil, err
	}
	return &retryStore{next: db, policy: policy}, nil
}
