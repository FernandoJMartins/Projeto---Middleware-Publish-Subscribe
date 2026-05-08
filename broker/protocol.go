package main

import "encoding/json"

const (
	topicInboxSize = 256 // Tamanho do canal de mensagens por tópico
	clientSendSize = 256 // Tamanho do canal de envio por cliente
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
