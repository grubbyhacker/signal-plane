package envelope

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

type Signal struct {
	Meta    Meta            `json:"meta"`
	Payload json.RawMessage `json:"payload"`
}

type Meta struct {
	SignalID           string         `json:"signal_id"`
	Source             string         `json:"source"`
	RouteID            string         `json:"route_id"`
	ReceivedAt         time.Time      `json:"received_at"`
	SourceEvent        string         `json:"source_event,omitempty"`
	SourceAction       string         `json:"source_action,omitempty"`
	SourceDeliveryID   string         `json:"source_delivery_id,omitempty"`
	Namespace          string         `json:"namespace,omitempty"`
	ObjectKind         string         `json:"object_kind,omitempty"`
	ObjectID           string         `json:"object_id,omitempty"`
	SourceRevision     string         `json:"source_revision,omitempty"`
	ActorClass         string         `json:"actor_class,omitempty"`
	CorrelationID      string         `json:"correlation_id,omitempty"`
	CausationID        string         `json:"causation_id,omitempty"`
	RootWorkItemID     string         `json:"root_work_item_id,omitempty"`
	ParentWorkItemID   string         `json:"parent_work_item_id,omitempty"`
	OriginatingSession string         `json:"originating_session_id,omitempty"`
	OriginatingTurn    string         `json:"originating_turn_id,omitempty"`
	HopCount           int            `json:"hop_count,omitempty"`
	ExpiresAt          *time.Time     `json:"expires_at,omitempty"`
	Authentication     Authentication `json:"authentication"`
}

type Authentication struct {
	Method   string `json:"method"`
	Verified bool   `json:"verified"`
}

func NewSignalID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		now := time.Now().UTC().Format("20060102T150405.000000000")
		return "signal-" + now
	}
	return "signal-" + hex.EncodeToString(bytes[:])
}
