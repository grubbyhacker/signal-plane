package eventbus

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const (
	testStream  = "SIGNALS"
	testSubject = "signals.test"
	testDurable = "signal-observer"
)

func TestNewObserverConsumerMigratesExistingDurableBeforeBinding(t *testing.T) {
	bus := newIntegrationBus(t)
	if _, err := bus.js.AddConsumer(testStream, &nats.ConsumerConfig{
		Durable:       testDurable,
		FilterSubject: testSubject,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       time.Minute,
		MaxAckPending: 1,
		MaxDeliver:    -1,
	}); err != nil {
		t.Fatalf("create existing consumer: %v", err)
	}
	before, err := bus.js.ConsumerInfo(testStream, testDurable)
	if err != nil {
		t.Fatalf("inspect existing consumer: %v", err)
	}

	consumer, err := bus.NewObserverConsumer(testSubject, testDurable)
	if err != nil {
		t.Fatalf("attach migrated consumer: %v", err)
	}
	defer consumer.sub.Unsubscribe()

	after := assertObserverConsumerConfig(t, bus, testDurable)
	if !after.Created.Equal(before.Created) {
		t.Errorf("consumer was recreated: created = %s, want %s", after.Created, before.Created)
	}
}

func TestNewObserverConsumerCreatesDurable(t *testing.T) {
	bus := newIntegrationBus(t)

	consumer, err := bus.NewObserverConsumer(testSubject, testDurable)
	if err != nil {
		t.Fatalf("create and attach consumer: %v", err)
	}
	defer consumer.sub.Unsubscribe()

	assertObserverConsumerConfig(t, bus, testDurable)
}

func newIntegrationBus(t *testing.T) *Bus {
	t.Helper()
	natsServer, err := server.NewServer(&server.Options{JetStream: true, StoreDir: t.TempDir(), Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	natsServer.Start()
	if !natsServer.ReadyForConnections(5 * time.Second) {
		natsServer.Shutdown()
		t.Fatal("NATS server did not become ready")
	}
	t.Cleanup(natsServer.Shutdown)

	bus, err := Connect(natsServer.ClientURL(), testStream, []string{testSubject})
	if err != nil {
		t.Fatalf("connect bus: %v", err)
	}
	t.Cleanup(bus.Close)
	return bus
}

func assertObserverConsumerConfig(t *testing.T, bus *Bus, durable string) *nats.ConsumerInfo {
	t.Helper()
	info, err := bus.js.ConsumerInfo(testStream, durable)
	if err != nil {
		t.Fatalf("inspect consumer: %v", err)
	}
	if info.Config.AckPolicy != nats.AckExplicitPolicy {
		t.Errorf("AckPolicy = %v, want %v", info.Config.AckPolicy, nats.AckExplicitPolicy)
	}
	if info.Config.AckWait != ObserverAckWait {
		t.Errorf("AckWait = %s, want %s", info.Config.AckWait, ObserverAckWait)
	}
	if info.Config.MaxAckPending != ObserverMaxAckPending {
		t.Errorf("MaxAckPending = %d, want %d", info.Config.MaxAckPending, ObserverMaxAckPending)
	}
	if info.Config.MaxDeliver != ObserverMaxDeliver {
		t.Errorf("MaxDeliver = %d, want %d", info.Config.MaxDeliver, ObserverMaxDeliver)
	}
	return info
}
