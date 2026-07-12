package observer

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeDelivery struct {
	data                 []byte
	sequence, deliveries uint64
	calls                []string
	ackErr, termErr      error
}

func (d *fakeDelivery) Data() []byte                      { return d.data }
func (d *fakeDelivery) Metadata() (uint64, uint64, error) { return d.sequence, d.deliveries, nil }
func (d *fakeDelivery) AckSync() error                    { d.calls = append(d.calls, "ack"); return d.ackErr }
func (d *fakeDelivery) Term() error                       { d.calls = append(d.calls, "term"); return d.termErr }

func TestProcessAcknowledgesOnlyAfterDecodeAndReportsAckFailure(t *testing.T) {
	metrics := NewMetrics([]string{"manual-local"})
	delivery := &fakeDelivery{data: []byte(`{"meta":{"source":"manual","route_id":"manual-local"}}`), sequence: 42, ackErr: errors.New("no ack")}
	if Process(slog.Default(), metrics, delivery) {
		t.Fatal("ack failure reported as success")
	}
	if len(delivery.calls) != 1 || delivery.calls[0] != "ack" {
		t.Fatalf("calls = %#v", delivery.calls)
	}
}

func TestProcessTerminatesMalformedMessageWithoutAck(t *testing.T) {
	metrics := NewMetrics(nil)
	delivery := &fakeDelivery{data: []byte(`not json`), sequence: 42}
	if Process(slog.Default(), metrics, delivery) {
		t.Fatal("malformed message reported as success")
	}
	if len(delivery.calls) != 1 || delivery.calls[0] != "term" {
		t.Fatalf("calls = %#v", delivery.calls)
	}
}

func TestReadinessEndpointDoesNotRequireTraffic(t *testing.T) {
	metrics := NewMetrics(nil)
	metrics.SetReady(true, 0, 0)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	metrics.SetReady(false, 0, 0)
	rec = httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
}
