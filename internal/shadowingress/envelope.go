// Package shadowingress is the host-side, unprivileged intake for Stage-5
// shadow admission.
//
// Per the agent-platform-coupling design ("The intake path"): host ingress is a
// Unix-domain socket guarded by filesystem permissions and SO_PEERCRED
// peer-credential checks. Plain loopback HTTP proves locality but not which
// local process called, and a socket carries no secret to rotate. This ingress
// accepts a BOUNDED, source-neutral domain-fact envelope, maps NO image /
// release / generation (an emitter states a domain fact and names no agent),
// and calls the shadowadmit service only.
//
// It is a dry-run intake: it never launches, never enqueues to the legacy
// launcher, never mutates legacy dispatcher tables, opens no external network
// listener, and touches no YouKnowMe container credential path. The external
// route-table schema and launcher cutover remain later stages. This ingress
// asks an injected deployment-owned RouteResolver for the route snapshot and
// (agent_type, mode) binding; the emitter supplies only the domain fact.
//
// Disabled by default: an empty SocketPath or Enabled=false makes Serve a no-op.
package shadowingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/shadowadmit"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// Bounds cap a single request so a local caller cannot exhaust the process.
const (
	// MaxEnvelopeBytes bounds one framed request payload.
	MaxEnvelopeBytes = 64 << 10 // 64 KiB
	// readTimeout bounds how long a connection may take to send its envelope.
	readTimeout = 5 * time.Second
	// socketMode is the required permission on the listening socket: owner
	// read/write only. Group/other access would widen who can enqueue.
	socketMode = 0o600
)

// admitService is the narrow slice of *shadowadmit.Shadow this ingress needs.
// It is admission-only by construction: there is no launch, claim, broker, or
// dispatcher method reachable from here.
type admitService interface {
	Admit(ctx context.Context, candidate shadowadmit.Candidate) (shadowadmit.Outcome, error)
	Enabled() bool
}

// Envelope is the bounded, source-neutral domain-fact intake message. An
// emitter states WHAT happened (source, namespace, object, revision, evidence)
// ONLY. It carries NO routing (route snapshot), agent type, mode, image,
// release, generation, broker run, or PR correlation: routing to a route
// snapshot and (agent_type, mode) is deployment-owned and chosen by the
// RouteResolver, never by the caller.
type Envelope struct {
	SignalID         string `json:"signal_id"`
	SourceDeliveryID string `json:"source_delivery_id"`
	TransportStream  string `json:"transport_stream"`
	TransportSeq     uint64 `json:"transport_sequence"`
	Source           string `json:"source"`
	Namespace        string `json:"namespace"`
	ObjectKind       string `json:"object_kind"`
	ObjectID         string `json:"object_id"`
	EventKind        string `json:"event_kind"`
	Action           string `json:"action"`
	ActorClass       string `json:"actor_class"`
	SourceRevision   string `json:"source_revision"`
	PayloadDigest    string `json:"payload_digest"`
	EvidenceRef      string `json:"evidence_ref"`
}

// Result is the deterministic reply written back to the caller. It reports the
// admission outcome and never a launch. Matched=false means the domain fact
// resolved to no route and was deterministically dropped (no admission).
type Result struct {
	Matched    bool   `json:"matched"`
	WorkItemID string `json:"work_item_id"`
	EventID    string `json:"event_id"`
	Duplicate  bool   `json:"duplicate"`
	Launched   bool   `json:"launched"` // always false
}

var (
	// ErrEnvelopeTooLarge is returned when a request exceeds MaxEnvelopeBytes.
	ErrEnvelopeTooLarge = errors.New("shadow ingress envelope exceeds bound")
	// ErrUnauthorizedPeer is returned when the connecting process fails the
	// peer-credential check.
	ErrUnauthorizedPeer = errors.New("shadow ingress peer is not authorized")
)

// DecodeEnvelope parses a bounded envelope from raw bytes, rejecting unknown
// fields and any oversize input. It never allocates beyond the bound.
func DecodeEnvelope(raw []byte) (Envelope, error) {
	if len(raw) > MaxEnvelopeBytes {
		return Envelope{}, ErrEnvelopeTooLarge
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode shadow ingress envelope: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return Envelope{}, errors.New("shadow ingress envelope has trailing content")
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

// Validate bounds every field and requires the identity the ledger needs to
// admit. It does not consult any live system, so it is runnable offline.
func (envelope Envelope) Validate() error {
	required := map[string]string{
		"source_delivery_id": envelope.SourceDeliveryID,
		"transport_stream":   envelope.TransportStream,
		"source":             envelope.Source,
		"namespace":          envelope.Namespace,
		"object_kind":        envelope.ObjectKind,
		"object_id":          envelope.ObjectID,
		"event_kind":         envelope.EventKind,
		"source_revision":    envelope.SourceRevision,
		"payload_digest":     envelope.PayloadDigest,
		"evidence_ref":       envelope.EvidenceRef,
	}
	for name, value := range required {
		if value == "" {
			return fmt.Errorf("shadow ingress envelope missing %s", name)
		}
		if len(value) > 512 {
			return fmt.Errorf("shadow ingress envelope %s is oversized", name)
		}
	}
	// Optional fields still bounded.
	for name, value := range map[string]string{"signal_id": envelope.SignalID, "action": envelope.Action, "actor_class": envelope.ActorClass} {
		if len(value) > 512 {
			return fmt.Errorf("shadow ingress envelope %s is oversized", name)
		}
	}
	if envelope.TransportSeq == 0 {
		return errors.New("shadow ingress envelope requires a positive transport sequence")
	}
	return nil
}

// Event projects the envelope onto a source-neutral ledger event. The received
// time is stamped by the ingress, not supplied by the caller.
func (envelope Envelope) Event(now time.Time) workledger.Event {
	return workledger.Event{
		SignalID:          envelope.SignalID,
		SourceDeliveryID:  envelope.SourceDeliveryID,
		TransportStream:   envelope.TransportStream,
		TransportSequence: envelope.TransportSeq,
		Source:            envelope.Source,
		Namespace:         envelope.Namespace,
		ObjectKind:        envelope.ObjectKind,
		ObjectID:          envelope.ObjectID,
		EventKind:         envelope.EventKind,
		Action:            envelope.Action,
		ActorClass:        envelope.ActorClass,
		SourceRevision:    envelope.SourceRevision,
		PayloadDigest:     envelope.PayloadDigest,
		EvidenceRef:       envelope.EvidenceRef,
		ReceivedAt:        now.UTC(),
	}
}
