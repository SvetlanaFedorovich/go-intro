//go:build integration

package kafka

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func TestProducerConsumerIntegration(t *testing.T) {
	rawBrokers := os.Getenv("TEST_KAFKA_BROKERS")
	if rawBrokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not set")
	}
	brokers := strings.Split(rawBrokers, ",")
	topic := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	groupID := topic + "-group"
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	created, err := client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{
		Topics: []kafkago.TopicConfig{{
			Topic:             topic,
			NumPartitions:     2,
			ReplicationFactor: 1,
		}},
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if topicErr := created.Errors[topic]; topicErr != nil {
		t.Fatalf("create topic response: %v", topicErr)
	}
	t.Cleanup(func() {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer deleteCancel()
		_, _ = client.DeleteTopics(deleteCtx, &kafkago.DeleteTopicsRequest{Topics: []string{topic}})
	})

	producer := NewProducer(brokers, topic)
	defer producer.Close()
	if err := producer.Ping(ctx); err != nil {
		t.Fatalf("producer ping: %v", err)
	}
	for i := 0; i < 4; i++ {
		err := producer.PublishKeyed(
			ctx,
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
			map[string]string{"X-Request-ID": fmt.Sprintf("request-%d", i)},
		)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	consumer := NewConsumer(brokers, topic, groupID)
	defer consumer.Close()
	if err := consumer.Ping(ctx); err != nil {
		t.Fatalf("consumer ping: %v", err)
	}
	messages, err := consumer.FetchBatch(ctx, 4)
	if err != nil {
		t.Fatalf("fetch batch: %v", err)
	}
	if len(messages) != 4 {
		t.Fatalf("message count = %d, want 4", len(messages))
	}
	for _, message := range messages {
		if message.Headers["x-request-id"] == "" {
			t.Fatalf("message offset %d has no request ID", message.Offset)
		}
		if len(message.Key) == 0 || len(message.Value) == 0 || message.Time.IsZero() {
			t.Fatalf("incomplete message: %+v", message)
		}
	}
	if err := consumer.Commit(ctx, messages...); err != nil {
		t.Fatalf("commit: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		lags, err := consumer.Lags(ctx)
		if err != nil {
			t.Fatalf("read lags: %v", err)
		}
		total := int64(0)
		for _, lag := range lags {
			total += lag.Lag
		}
		if total == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer lag = %d, want 0", total)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
