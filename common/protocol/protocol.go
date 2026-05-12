package protocol

import "encoding/json"

// Frame - Envelope de mensagem entre cliente e broker.
type Frame struct {
	// Type define a acao: subscribe, publish, message, ack, etc.
	Type string `json:"type"`
	// ID correlaciona requisicoes e respostas (ACK).
	ID string `json:"id,omitempty"`
	// Topic identifica o canal logico de mensagens.
	Topic string `json:"topic,omitempty"`
	// Data e o payload JSON bruto.
	Data json.RawMessage `json:"data,omitempty"`
	// Ok e o status no ACK.
	Ok bool `json:"ok,omitempty"`
	// Error detalha o erro quando Ok=false.
	Error string `json:"error,omitempty"`
}
