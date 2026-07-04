package eventbus

import (
	"encoding/json"
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

func (bus *Bus) FetchOne(subject string, durable string, maxWait time.Duration) (Message, error) {
	sub, err := bus.js.PullSubscribe(subject, durable, nats.BindStream(bus.stream))
	if err != nil {
		return Message{}, fmt.Errorf("subscribe: %w", err)
	}

	msgs, err := sub.Fetch(1, nats.MaxWait(maxWait))
	if err != nil {
		return Message{}, fmt.Errorf("fetch message: %w", err)
	}
	msg := msgs[0]
	defer msg.Ack()

	var signal envelope.Signal
	if err := json.Unmarshal(msg.Data, &signal); err != nil {
		return Message{}, fmt.Errorf("decode signal: %w", err)
	}

	meta, err := msg.Metadata()
	if err != nil {
		return Message{Subject: msg.Subject, Signal: signal}, nil
	}
	return Message{Subject: msg.Subject, Sequence: meta.Sequence.Stream, Signal: signal}, nil
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
