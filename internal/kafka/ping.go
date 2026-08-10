package kafka

import (
	"context"
	"errors"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// pingKafka проверяет доступность кластера: подключается к одному из брокеров
// и запрашивает метаданные (список брокеров). Пустой список брокеров или
// невозможность установить соединение считаются ошибкой readiness.
func pingKafka(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}

	// Dial с учётом дедлайна из ctx - не виснем дольше, чем отведено пробе.
	var dialer kafkago.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	if _, err := conn.Brokers(); err != nil {
		return fmt.Errorf("read kafka metadata: %w", err)
	}
	return nil
}
