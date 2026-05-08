package client

import (
	"encoding/json"
	"time"
)

const (
	brokerSendSize = 256
	subBufferSize  = 64
	ackTimeout     = 5 * time.Second
)

// frame - Envelope de mensagem entre cliente e broker.
type frame struct {
	Type  string          `json:"type"`
	ID    string          `json:"id,omitempty"`
	Topic string          `json:"topic,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	Ok    bool            `json:"ok,omitempty"`
	Error string          `json:"error,omitempty"`
}
