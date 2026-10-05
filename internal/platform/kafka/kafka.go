// Package kafka is the message bus between api, worker and the channel
// adapters (FTR.NAB.CMN-0001 arch §6). Topics carry the prefix nabu.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"

	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/telemetry"
)

// Topics (tech spec §8, §9a).
const (
	// TopicInbound: a user message from any channel; key — user id, so the
	// messages of one user are handled in order (CONV-04).
	TopicInbound = "nabu.inbound"
	// TopicOutbound: a finished answer for the messenger it came from.
	TopicOutbound = "nabu.outbound"
	// TopicTaskRun: a due run of a scheduled task.
	TopicTaskRun = "nabu.task.run"
	// TopicRuns: a run of a service agent requested by a client.
	TopicRuns = "nabu.runs"
)

// AllTopics lists every topic of Nabu.
var AllTopics = []string{TopicInbound, TopicOutbound, TopicTaskRun, TopicRuns}

// Publisher publishes keyed messages.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// Producer publishes to Kafka.
type Producer struct{ w *kgo.Writer }

// NewProducer creates a synchronous producer with key-hash partitioning.
func NewProducer(brokers []string) *Producer {
	return &Producer{w: &kgo.Writer{
		Addr:                   kgo.TCP(brokers...),
		Balancer:               &kgo.Hash{},
		RequiredAcks:           kgo.RequireAll,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}}
}

// Publish writes one message.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	ctx, span := telemetry.Start(ctx, "kafka.produce "+topic)
	defer span.End()
	span.SetAttributes(attribute.String("messaging.destination", topic), attribute.String("messaging.key", key))
	return p.w.WriteMessages(ctx, kgo.Message{Topic: topic, Key: []byte(key), Value: value})
}

// Close flushes the producer.
func (p *Producer) Close() error { return p.w.Close() }

// EnsureTopics creates topics if they do not exist.
func EnsureTopics(ctx context.Context, brokers []string, topics ...string) error {
	if len(brokers) == 0 {
		return errors.New("no kafka brokers")
	}
	d := &kgo.Dialer{Timeout: 10 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	ctrl, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()
	cfgs := make([]kgo.TopicConfig, 0, len(topics))
	for _, t := range topics {
		cfgs = append(cfgs, kgo.TopicConfig{Topic: t, NumPartitions: 6, ReplicationFactor: 1})
	}
	return cc.CreateTopics(cfgs...)
}

// Ping checks broker reachability for readiness.
func Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return errors.New("no kafka brokers")
	}
	d := &kgo.Dialer{Timeout: 3 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}

// Handler processes one message. Returning an error retries the message.
type Handler func(ctx context.Context, key, value []byte) error

// Consume runs a consumer group until ctx is done. Messages are committed only
// after successful handling (at-least-once); handlers must be idempotent.
func Consume(ctx context.Context, brokers []string, group, topic string, h Handler) error {
	r := kgo.NewReader(kgo.ReaderConfig{
		Brokers: brokers, GroupID: group, Topic: topic,
		MinBytes: 1, MaxBytes: 10 << 20, MaxWait: time.Second,
		CommitInterval: 0,
	})
	defer r.Close()
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				metrics.KafkaLag.WithLabelValues(topic).Set(float64(r.Stats().Lag))
			}
		}
	}()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch %s: %w", topic, err)
		}
		backoff := time.Second
		for attempt := 1; ; attempt++ {
			hctx, span := telemetry.Start(ctx, "kafka.consume "+topic)
			err := h(hctx, m.Key, m.Value)
			span.End()
			if err == nil {
				break
			}
			if attempt >= 8 {
				slog.ErrorContext(ctx, "message dropped after retries", "topic", topic, "offset", m.Offset, "err", err)
				break
			}
			slog.ErrorContext(ctx, "message handling failed, retrying", "topic", topic, "offset", m.Offset, "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff < time.Minute {
				backoff *= 2
			}
		}
		if err := r.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "commit failed", "topic", topic, "err", err)
		}
	}
}

// ConsumeParallel runs n handlers of one consumer group: a long agent turn of
// one partition does not hold back the others.
func ConsumeParallel(ctx context.Context, brokers []string, group, topic string, n int, h Handler) error {
	if n < 1 {
		n = 1
	}
	errc := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() { errc <- Consume(ctx, brokers, group, topic, h) }()
	}
	var first error
	for i := 0; i < n; i++ {
		if err := <-errc; err != nil && first == nil {
			first = err
		}
	}
	return first
}
