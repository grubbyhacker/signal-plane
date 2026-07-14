package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestGitHubDispatcherFileJetStreamEndToEndAndPublishDedupe(t *testing.T) {
	bus := newIntegrationBus(t)
	signal := envelope.Signal{Meta: envelope.Meta{Source: "github", SourceEvent: "issues", SourceAction: "labeled", SourceDeliveryID: "delivery-e2e"}, Payload: json.RawMessage(`{"action":"labeled","repository":{"full_name":"example/automation-target"},"issue":{"number":12,"state":"open"},"label":{"name":"agent:implement"},"sender":{"login":"test-user"}}`)}
	if err := bus.Publish(testSubject, signal); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(testSubject, signal); err != nil {
		t.Fatal(err)
	}
	info, err := bus.js.StreamInfo(testStream)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream messages = %d, want Nats-Msg-Id dedupe", info.State.Msgs)
	}
	consumer, err := bus.NewConsumer(ConsumerConfig{Subject: testSubject, Durable: "github-task-dispatcher-test", AckWait: time.Second, MaxAckPending: 1, MaxDeliver: 3})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Fetch(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	store, err := dispatcher.OpenStore(filepath.Join(t.TempDir(), "dispatcher.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	metrics := dispatcher.NewMetrics()
	if !dispatcher.Process(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), metrics, store, dispatcher.NATSDelivery{Message: msg}, time.Unix(1, 0)) {
		t.Fatal("delivery not processed")
	}
	deliveries, jobs, err := store.Counts(context.Background())
	if err != nil || deliveries != 1 || jobs != 1 {
		t.Fatalf("counts=%d,%d err=%v", deliveries, jobs, err)
	}
	consumerInfo, err := consumer.Ready(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if consumerInfo.NumAckPending != 0 {
		t.Fatalf("ack pending = %d", consumerInfo.NumAckPending)
	}
	recoverySequence, err := store.RecoverySequence(context.Background())
	metadata, metadataErr := msg.Metadata()
	if err != nil || metadataErr != nil || recoverySequence != metadata.Sequence.Stream+1 {
		t.Fatalf("recovery sequence=%d err=%v", recoverySequence, err)
	}
}

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

func TestRecoveryConsumerStartsAfterSQLiteCheckpoint(t *testing.T) {
	bus := newIntegrationBus(t)
	for i := 1; i <= 3; i++ {
		signal := envelope.Signal{Meta: envelope.Meta{SourceDeliveryID: fmt.Sprintf("recovery-%d", i)}}
		if err := bus.Publish(testSubject, signal); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := bus.NewConsumer(ConsumerConfig{Subject: testSubject, Durable: "dispatcher-recovery", AckWait: time.Second, MaxAckPending: 1, MaxDeliver: 3, StartSequence: 2})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Fetch(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := msg.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Sequence.Stream != 2 {
		t.Fatalf("stream sequence=%d", metadata.Sequence.Stream)
	}
	if _, err := bus.NewConsumer(ConsumerConfig{Subject: testSubject, Durable: "dispatcher-recovery", AckWait: time.Second, MaxAckPending: 1, MaxDeliver: 3, StartSequence: 3}); err == nil {
		t.Fatal("expected immutable recovery start rejection")
	}
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
