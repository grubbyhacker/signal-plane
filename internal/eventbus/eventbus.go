package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/nats-io/nats.go"
)

type Bus struct {
	conn   *nats.Conn
	js     nats.JetStreamContext
	stream string
}

type Message struct {
	Subject  string
	Sequence uint64
	Signal   envelope.Signal
}

func Connect(url string, stream string, subjects []string) (*Bus, error) {
	conn, err := nats.Connect(url, nats.Name("signal-plane"))
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open jetstream: %w", err)
	}

	if err := ensureStream(js, stream, subjects); err != nil {
		conn.Close()
		return nil, err
	}

	return &Bus{conn: conn, js: js, stream: stream}, nil
}

func (bus *Bus) Close() {
	if bus == nil || bus.conn == nil {
		return
	}
	bus.conn.Drain()
	bus.conn.Close()
}

func (bus *Bus) Publish(subject string, signal envelope.Signal) error {
	data, err := json.Marshal(signal)
	if err != nil {
		return fmt.Errorf("marshal signal: %w", err)
	}
	opts := []nats.PubOpt{}
	if signal.Meta.SourceDeliveryID != "" {
		opts = append(opts, nats.MsgId(signal.Meta.SourceDeliveryID))
	}
	if _, err := bus.js.Publish(subject, data, opts...); err != nil {
		return fmt.Errorf("publish signal: %w", err)
	}
	return nil
}

type ConsumerConfig struct {
	Subject       string
	Durable       string
	AckWait       time.Duration
	MaxAckPending int
	MaxDeliver    int
	// StartSequence is a recovery-only contract. Use Store.RecoverySequence
	// with a new durable name after restoring dispatcher SQLite state.
	StartSequence uint64
}

func (bus *Bus) NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.AckWait <= 0 || cfg.MaxAckPending <= 0 || cfg.MaxDeliver <= 0 {
		return nil, errors.New("consumer acknowledgement settings must be positive")
	}
	if err := bus.ensureConsumer(cfg); err != nil {
		return nil, err
	}
	sub, err := bus.js.PullSubscribe(cfg.Subject, cfg.Durable, nats.Bind(bus.stream, cfg.Durable), nats.ManualAck())
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	return &Consumer{bus: bus, sub: sub, durable: cfg.Durable}, nil
}

func (bus *Bus) ensureConsumer(want ConsumerConfig) error {
	info, err := bus.js.ConsumerInfo(bus.stream, want.Durable)
	if errors.Is(err, nats.ErrConsumerNotFound) {
		cfg := &nats.ConsumerConfig{Durable: want.Durable, FilterSubject: want.Subject, AckPolicy: nats.AckExplicitPolicy, AckWait: want.AckWait, MaxAckPending: want.MaxAckPending, MaxDeliver: want.MaxDeliver}
		if want.StartSequence > 0 {
			cfg.DeliverPolicy = nats.DeliverByStartSequencePolicy
			cfg.OptStartSeq = want.StartSequence
		}
		_, err = bus.js.AddConsumer(bus.stream, cfg)
		if err != nil {
			return fmt.Errorf("create consumer: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect consumer configuration: %w", err)
	}
	if info.Config.FilterSubject != want.Subject {
		return fmt.Errorf("consumer %q filter subject is immutable: have %q want %q", want.Durable, info.Config.FilterSubject, want.Subject)
	}
	if want.StartSequence > 0 && (info.Config.DeliverPolicy != nats.DeliverByStartSequencePolicy || info.Config.OptStartSeq != want.StartSequence) {
		return fmt.Errorf("consumer %q recovery start is immutable; use a new durable name", want.Durable)
	}
	cfg := info.Config
	cfg.AckPolicy, cfg.AckWait, cfg.MaxAckPending, cfg.MaxDeliver = nats.AckExplicitPolicy, want.AckWait, want.MaxAckPending, want.MaxDeliver
	if _, err := bus.js.UpdateConsumer(bus.stream, &cfg); err != nil {
		return fmt.Errorf("configure consumer: %w", err)
	}
	return nil
}

// Ready verifies that the NATS connection is live and that the configured
// stream is still inspectable. It intentionally does not publish a probe.
func (bus *Bus) Ready(_ context.Context) error {
	if bus == nil || bus.conn == nil || bus.conn.Status() != nats.CONNECTED {
		return errors.New("nats_not_connected")
	}
	if _, err := bus.js.StreamInfo(bus.stream); err != nil {
		return fmt.Errorf("inspect stream: %w", err)
	}
	return nil
}

// Consumer is a durable pull subscription with explicit acknowledgements.
type Consumer struct {
	bus     *Bus
	sub     *nats.Subscription
	durable string
}

const (
	ObserverAckWait       = 30 * time.Second
	ObserverMaxAckPending = 64
	ObserverMaxDeliver    = 5
)

func (bus *Bus) NewObserverConsumer(subject string, durable string) (*Consumer, error) {
	return bus.NewConsumer(ConsumerConfig{Subject: subject, Durable: durable, AckWait: ObserverAckWait, MaxAckPending: ObserverMaxAckPending, MaxDeliver: ObserverMaxDeliver})
}

// Fetch returns an unacknowledged message. Callers must explicitly AckSync or
// Term after their responsibility has completed.
func (consumer *Consumer) Fetch(maxWait time.Duration) (*nats.Msg, error) {
	msgs, err := consumer.sub.Fetch(1, nats.MaxWait(maxWait))
	if err != nil {
		return nil, fmt.Errorf("fetch message: %w", err)
	}
	return msgs[0], nil
}

func (consumer *Consumer) Ready(_ context.Context) (*nats.ConsumerInfo, error) {
	if err := consumer.bus.Ready(context.Background()); err != nil {
		return nil, err
	}
	info, err := consumer.bus.js.ConsumerInfo(consumer.bus.stream, consumer.durable)
	if err != nil {
		return nil, fmt.Errorf("inspect consumer: %w", err)
	}
	return info, nil
}

func ensureStream(js nats.JetStreamContext, stream string, subjects []string) error {
	current, err := js.StreamInfo(stream)
	if err == nil {
		if sameSubjects(current.Config.Subjects, subjects) {
			return nil
		}
		current.Config.Subjects = subjects
		if _, err := js.UpdateStream(&current.Config); err != nil {
			return fmt.Errorf("update stream: %w", err)
		}
		return nil
	}
	if err != nats.ErrStreamNotFound {
		return fmt.Errorf("inspect stream: %w", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: subjects,
		Storage:  nats.FileStorage,
	}); err != nil {
		return fmt.Errorf("add stream: %w", err)
	}
	return nil
}

func sameSubjects(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
