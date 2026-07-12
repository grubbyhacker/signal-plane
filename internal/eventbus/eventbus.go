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
	if _, err := bus.js.Publish(subject, data); err != nil {
		return fmt.Errorf("publish signal: %w", err)
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
// It is created once and reused for the observer's lifetime.
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
	sub, err := bus.js.PullSubscribe(subject, durable,
		nats.BindStream(bus.stream),
		nats.ManualAck(),
		nats.AckWait(ObserverAckWait),
		nats.MaxAckPending(ObserverMaxAckPending),
		nats.MaxDeliver(ObserverMaxDeliver),
	)
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	if err := bus.configureObserverConsumer(durable); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	return &Consumer{bus: bus, sub: sub, durable: durable}, nil
}

func (bus *Bus) configureObserverConsumer(durable string) error {
	info, err := bus.js.ConsumerInfo(bus.stream, durable)
	if err != nil {
		return fmt.Errorf("inspect consumer configuration: %w", err)
	}
	config := info.Config
	if config.AckPolicy == nats.AckExplicitPolicy && config.AckWait == ObserverAckWait && config.MaxAckPending == ObserverMaxAckPending && config.MaxDeliver == ObserverMaxDeliver {
		return nil
	}
	config.AckPolicy = nats.AckExplicitPolicy
	config.AckWait = ObserverAckWait
	config.MaxAckPending = ObserverMaxAckPending
	config.MaxDeliver = ObserverMaxDeliver
	if _, err := bus.js.UpdateConsumer(bus.stream, &config); err != nil {
		return fmt.Errorf("configure consumer: %w", err)
	}
	return nil
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
