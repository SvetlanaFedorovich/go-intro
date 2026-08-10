package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/AntonYurchenko/go-intro/internal/observability"
	"github.com/AntonYurchenko/go-intro/internal/retry"
	kafkago "github.com/segmentio/kafka-go"
)

type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, messages ...kafkago.Message) error
	Close() error
}

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Time      time.Time
	msg       kafkago.Message
}

type PartitionLag struct {
	Topic     string
	Partition int
	Lag       int64
}

type Consumer struct {
	reader  messageReader
	brokers []string
	topic   string
	groupID string
	client  *kafkago.Client
	retry   retry.Policy
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return NewConsumerWithRetry(brokers, topic, groupID, retry.DefaultPolicy())
}

func NewConsumerWithRetry(brokers []string, topic, groupID string, policy retry.Policy) *Consumer {
	return &Consumer{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
		retry:   policy,
		client:  &kafkago.Client{Addr: kafkago.TCP(brokers...)},
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0, // manual commit
			StartOffset:    kafkago.FirstOffset,
		}),
	}
}

// Ping проверяет доступность Kafka. Используется в readiness-пробе worker.
func (c *Consumer) Ping(ctx context.Context) error {
	return pingKafka(ctx, c.brokers)
}

// FetchBatch reads available messages until the deadline or max messages.
func (c *Consumer) FetchBatch(ctx context.Context, max int) ([]Message, error) {
	if max <= 0 {
		max = 100
	}

	out := make([]Message, 0, max)
	for len(out) < max {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return out, nil
			}
			return out, fmt.Errorf("fetch kafka message: %w", err)
		}
		out = append(out, Message{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   messageHeaders(msg.Headers),
			Time:      msg.Time,
			msg:       msg,
		})
	}
	return out, nil
}

func (c *Consumer) Lags(ctx context.Context) ([]PartitionLag, error) {
	var lags []PartitionLag
	err := retry.Do(ctx, kafkaRetryPolicy(c.retry, "kafka_lag"), func() error {
		var err error
		lags, err = c.readLags(ctx)
		return err
	})
	return lags, err
}

func (c *Consumer) readLags(ctx context.Context) ([]PartitionLag, error) {
	metadata, err := c.client.Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{c.topic}})
	if err != nil {
		return nil, fmt.Errorf("read topic metadata for lag: %w", err)
	}
	if len(metadata.Topics) != 1 {
		return nil, fmt.Errorf("read topic metadata for lag: topic %q not found", c.topic)
	}
	topic := metadata.Topics[0]
	if topic.Error != nil {
		return nil, fmt.Errorf("read topic metadata for lag: %w", topic.Error)
	}

	partitions := make([]int, 0, len(topic.Partitions))
	lastOffsetRequests := make([]kafkago.OffsetRequest, 0, len(topic.Partitions))
	for _, partition := range topic.Partitions {
		if partition.Error != nil {
			return nil, fmt.Errorf("read partition %d metadata for lag: %w", partition.ID, partition.Error)
		}
		partitions = append(partitions, partition.ID)
		lastOffsetRequests = append(lastOffsetRequests, kafkago.LastOffsetOf(partition.ID))
	}

	coordinator, err := c.client.FindCoordinator(ctx, &kafkago.FindCoordinatorRequest{
		Key:     c.groupID,
		KeyType: kafkago.CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		return nil, fmt.Errorf("find consumer group coordinator for lag: %w", err)
	}
	if coordinator.Error != nil {
		return nil, fmt.Errorf("find consumer group coordinator for lag: %w", coordinator.Error)
	}
	if coordinator.Coordinator == nil {
		return nil, fmt.Errorf("find consumer group coordinator for lag: empty coordinator")
	}
	coordinatorAddr := kafkago.TCP(net.JoinHostPort(
		coordinator.Coordinator.Host,
		strconv.Itoa(coordinator.Coordinator.Port),
	))
	committed, err := c.client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{
		Addr:    coordinatorAddr,
		GroupID: c.groupID,
		Topics:  map[string][]int{c.topic: partitions},
	})
	if err != nil {
		return nil, fmt.Errorf("read committed offsets for lag: %w", err)
	}
	if committed.Error != nil {
		return nil, fmt.Errorf("read committed offsets for lag: %w", committed.Error)
	}

	ends, err := c.client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{
		Topics: map[string][]kafkago.OffsetRequest{c.topic: lastOffsetRequests},
	})
	if err != nil {
		return nil, fmt.Errorf("read end offsets for lag: %w", err)
	}

	committedByPartition := make(map[int]int64, len(partitions))
	for _, offset := range committed.Topics[c.topic] {
		if offset.Error != nil {
			return nil, fmt.Errorf("read partition %d committed offset for lag: %w", offset.Partition, offset.Error)
		}
		committedByPartition[offset.Partition] = offset.CommittedOffset
	}

	lags := make([]PartitionLag, 0, len(partitions))
	for _, offset := range ends.Topics[c.topic] {
		if offset.Error != nil {
			return nil, fmt.Errorf("read partition %d end offset for lag: %w", offset.Partition, offset.Error)
		}
		committedOffset := committedByPartition[offset.Partition]
		if committedOffset < 0 {
			committedOffset = 0
		}
		lag := offset.LastOffset - committedOffset
		if lag < 0 {
			lag = 0
		}
		lags = append(lags, PartitionLag{
			Topic:     c.topic,
			Partition: offset.Partition,
			Lag:       lag,
		})
	}
	return lags, nil
}

func messageHeaders(headers []kafkago.Header) map[string]string {
	result := make(map[string]string, len(headers))
	for _, header := range headers {
		result[strings.ToLower(header.Key)] = string(header.Value)
	}
	return result
}

func (c *Consumer) Commit(ctx context.Context, messages ...Message) error {
	if len(messages) == 0 {
		return nil
	}
	raw := make([]kafkago.Message, len(messages))
	for i, m := range messages {
		raw[i] = m.msg
	}
	if err := retry.Do(ctx, kafkaRetryPolicy(c.retry, "kafka_commit"), func() error {
		return c.reader.CommitMessages(ctx, raw...)
	}); err != nil {
		return fmt.Errorf("commit kafka messages: %w", err)
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func kafkaRetryPolicy(policy retry.Policy, operation string) retry.Policy {
	onRetry := policy.OnRetry
	policy.OnRetry = func(attempt int, delay time.Duration, err error) {
		observability.ObserveRetry(operation)
		if onRetry != nil {
			onRetry(attempt, delay, err)
		}
	}
	return policy
}
