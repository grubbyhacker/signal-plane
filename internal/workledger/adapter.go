package workledger

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

// AuthenticatedIngressAdapter converts an already authenticated source
// envelope into source-neutral ledger metadata. Authentication remains the
// gateway's responsibility; adapters must fail closed if that proof is absent.
type AuthenticatedIngressAdapter interface {
	Source() string
	Normalize(envelope.Signal, string, uint64) (Event, error)
}

type GitHubAdapter struct{}

func (GitHubAdapter) Source() string { return "github" }

func (GitHubAdapter) Normalize(signal envelope.Signal, stream string, sequence uint64) (Event, error) {
	meta := signal.Meta
	if meta.Source != "github" || !meta.Authentication.Verified || meta.Authentication.Method != "github_hmac_sha256" {
		return Event{}, errors.New("verified GitHub HMAC authentication is required")
	}
	if meta.Namespace == "" || meta.ObjectKind == "" || meta.ObjectID == "" || meta.SourceRevision == "" {
		return Event{}, errors.New("GitHub envelope lacks normalized namespace, object, or revision identity")
	}
	if stream == "" || sequence == 0 {
		return Event{}, errors.New("transport stream and positive sequence are required")
	}
	payloadDigest := sha256.Sum256(signal.Payload)
	return Event{SignalID: meta.SignalID, SourceDeliveryID: meta.SourceDeliveryID, TransportStream: stream, TransportSequence: sequence, Source: meta.Source, Namespace: meta.Namespace, ObjectKind: meta.ObjectKind, ObjectID: meta.ObjectID, EventKind: meta.SourceEvent, Action: meta.SourceAction, ActorClass: meta.ActorClass, SourceRevision: meta.SourceRevision, CorrelationID: meta.CorrelationID, CausationID: meta.CausationID, RootWorkItemID: meta.RootWorkItemID, ParentWorkItemID: meta.ParentWorkItemID, OriginatingSession: meta.OriginatingSession, OriginatingTurn: meta.OriginatingTurn, HopCount: meta.HopCount, ExpiresAt: meta.ExpiresAt, PayloadDigest: "sha256:" + hex.EncodeToString(payloadDigest[:]), EvidenceRef: fmt.Sprintf("jetstream://%s/%d", stream, sequence), ReceivedAt: meta.ReceivedAt}, nil
}

func AdapterFor(source string) (AuthenticatedIngressAdapter, error) {
	if source == "github" {
		return GitHubAdapter{}, nil
	}
	return nil, fmt.Errorf("no authenticated adapter registered for source %q", source)
}
