package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/observability"
	"github.com/AntonYurchenko/go-intro/internal/retry"
	kafkago "github.com/segmentio/kafka-go"
)

type messageWriter interface {
	WriteMessages(ctx context.Context, messages ...kafkago.Message) error
	Close() error
}

type Producer struct {
	writer  messageWriter
	brokers []string
	topic   string
	retry   retry.Policy
}

func NewProducer(brokers []string, topic string) *Producer {
	return NewProducerWithRetry(brokers, topic, retry.DefaultPolicy())
}

func NewProducerWithRetry(brokers []string, topic string, policy retry.Policy) *Producer {
	return &Producer{
		brokers: brokers,
		topic:   topic,
		retry:   kafkaRetryPolicy(policy, "kafka_publish"),
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafkago.LeastBytes{},
			RequiredAcks:           kafkago.RequireAll,
			Async:                  false,
			MaxAttempts:            1,
			BatchSize:              100,
			BatchBytes:             1 << 20,
			BatchTimeout:           10 * time.Millisecond,
			AllowAutoTopicCreation: true,
		},
	}
}

// Ping проверяет доступность Kafka: устанавливает соединение с брокером
// и запрашивает метаданные. Используется в readiness-пробе.
func (p *Producer) Ping(ctx context.Context) error {
	return pingKafka(ctx, p.brokers)
}

func (p *Producer) Publish(ctx context.Context, value []byte) error {
	return p.PublishWithHeaders(ctx, value, nil)
}

func (p *Producer) PublishKeyed(ctx context.Context, key, value []byte, headers map[string]string) error {
	kafkaHeaders := make([]kafkago.Header, 0, len(headers))
	for name, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafkago.Header{Key: name, Value: []byte(value)})
	}
	return p.publish(ctx, kafkago.Message{Key: key, Value: value, Headers: kafkaHeaders})
}

func (p *Producer) PublishWithHeaders(ctx context.Context, value []byte, headers []kafkago.Header) error {
	return p.publish(ctx, kafkago.Message{
		Value:   value,
		Headers: headers,
	})
}

func (p *Producer) publish(ctx context.Context, msg kafkago.Message) error {
	msg.Time = time.Now().UTC()
	started := time.Now()
	err := retry.Do(ctx, p.retry, func() error {
		return p.writer.WriteMessages(ctx, msg)
	})
	// Метрика находится здесь, чтобы учитывать API, DLQ и будущих producers.
	observability.ObserveKafkaPublish(p.topic, started, err)
	if err != nil {
		return fmt.Errorf("publish kafka message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
