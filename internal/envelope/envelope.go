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
	SignalID         string    `json:"signal_id"`
	Source           string    `json:"source"`
	RouteID          string    `json:"route_id"`
	ReceivedAt       time.Time `json:"received_at"`
	SourceEvent      string    `json:"source_event,omitempty"`
	SourceAction     string    `json:"source_action,omitempty"`
	SourceDeliveryID string    `json:"source_delivery_id,omitempty"`
}

func NewSignalID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		now := time.Now().UTC().Format("20060102T150405.000000000")
		return "signal-" + now
	}
	return "signal-" + hex.EncodeToString(bytes[:])
}
