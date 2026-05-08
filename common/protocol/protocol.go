package protocol

import "encoding/json"

// Frame - Envelope de mensagem entre cliente e broker.
type Frame struct {
	Type  string          `json:"type"`
	ID    string          `json:"id,omitempty"`
	Topic string          `json:"topic,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	Ok    bool            `json:"ok,omitempty"`
	Error string          `json:"error,omitempty"`
}
