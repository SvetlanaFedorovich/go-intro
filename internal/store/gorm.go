package store

import (
	"context"
	"fmt"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/model"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type dataRow struct {
	ID          int32  `gorm:"column:id;primaryKey;autoIncrement"`
	EventID     string `gorm:"column:event_id;uniqueIndex"`
	PayloadHash string `gorm:"column:payload_hash"`
	User        string `gorm:"column:user"`
	Age         int16  `gorm:"column:age"`
	Email       string `gorm:"column:email"`
}

func (dataRow) TableName() string { return "data" }

type processedEvent struct {
	EventID     string    `gorm:"column:event_id;primaryKey"`
	PayloadHash string    `gorm:"column:payload_hash"`
	Topic       string    `gorm:"column:topic"`
	Partition   int       `gorm:"column:partition"`
	Offset      int64     `gorm:"column:offset"`
	ProcessedAt time.Time `gorm:"column:processed_at;autoCreateTime;index"`
}

func (processedEvent) TableName() string { return "processed_events" }

type gormStore struct {
	db *gorm.DB
}

func newGORM(ctx context.Context, dsn string) (*gormStore, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Silent),
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, fmt.Errorf("connect postgres (gorm): %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("gorm sql db: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping postgres (gorm): %w", err)
	}

	return &gormStore{db: db}, nil
}

func (s *gormStore) InsertOnce(ctx context.Context, eventID, topic string, partition int, offset int64, d model.Data) (bool, error) {
	hash, err := payloadHash(d)
	if err != nil {
		return false, fmt.Errorf("hash payload: %w", err)
	}

	inserted := false
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := dataRow{
			EventID:     eventID,
			PayloadHash: hash,
			User:        d.User,
			Age:         int16(d.Age),
			Email:       d.Email,
		}
		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "event_id"}},
			DoNothing: true,
		}).Create(&row)
		if res.Error != nil {
			return fmt.Errorf("insert data once: %w", res.Error)
		}
		inserted = res.RowsAffected == 1
		if !inserted {
			var existing dataRow
			if err := tx.Where("event_id = ?", eventID).Take(&existing).Error; err != nil {
				return fmt.Errorf("read existing data event: %w", err)
			}
			if existing.PayloadHash != hash {
				return fmt.Errorf("%w: %s", ErrEventConflict, eventID)
			}
		}

		res = tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "event_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"payload_hash", "topic", "partition", "offset", "processed_at",
			}),
		}).Create(&processedEvent{
			EventID:     eventID,
			PayloadHash: hash,
			Topic:       topic,
			Partition:   partition,
			Offset:      offset,
			ProcessedAt: time.Now().UTC(),
		})
		if res.Error != nil {
			return fmt.Errorf("record processed event: %w", res.Error)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return inserted, nil
}

func (s *gormStore) CleanupProcessed(ctx context.Context, before time.Time) (int64, error) {
	result := s.db.WithContext(ctx).
		Where("processed_at < ?", before).
		Delete(&processedEvent{})
	if result.Error != nil {
		return 0, fmt.Errorf("cleanup processed events: %w", result.Error)
	}
	return result.RowsAffected, nil
}

func (s *gormStore) Ping(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

func (s *gormStore) Close() {
	sqlDB, err := s.db.DB()
	if err != nil {
		return
	}
	_ = sqlDB.Close()
}
