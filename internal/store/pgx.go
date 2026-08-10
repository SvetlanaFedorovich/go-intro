package store

import (
	"context"
	"fmt"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pgxStore struct {
	pool *pgxpool.Pool
}

func newPGX(ctx context.Context, dsn string) (*pgxStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres (pgx): %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres (pgx): %w", err)
	}
	return &pgxStore{pool: pool}, nil
}

func (s *pgxStore) InsertOnce(ctx context.Context, eventID, topic string, partition int, offset int64, d model.Data) (bool, error) {
	hash, err := payloadHash(d)
	if err != nil {
		return false, fmt.Errorf("hash payload: %w", err)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, sqlInsertDataOnce, eventID, hash, d.User, d.Age, d.Email)
	if err != nil {
		return false, fmt.Errorf("insert data once: %w", err)
	}
	inserted := tag.RowsAffected() == 1
	if !inserted {
		var existingHash string
		if err := tx.QueryRow(ctx, sqlDataPayloadHash, eventID).Scan(&existingHash); err != nil {
			return false, fmt.Errorf("read existing data event: %w", err)
		}
		if existingHash != hash {
			return false, fmt.Errorf("%w: %s", ErrEventConflict, eventID)
		}
	}

	if _, err := tx.Exec(ctx, sqlRecordProcessedEvent, eventID, hash, topic, partition, offset); err != nil {
		return false, fmt.Errorf("record processed event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return inserted, nil
}

func (s *pgxStore) CleanupProcessed(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, sqlCleanupProcessed, before)
	if err != nil {
		return 0, fmt.Errorf("cleanup processed events: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (s *pgxStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *pgxStore) Close() {
	s.pool.Close()
}
