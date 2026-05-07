package client

import (
	"encoding/json"
)

// Message - Estrutura de mensagem
type Message struct {
	Topic string          `json:"topic"`
	Data  json.RawMessage `json:"data"`
}

// Client - Cliente pub/sub
type Client struct {
	// TODO: Implementar
}

// NewClient - Cria um novo cliente
func NewClient(brokerAddr string) (*Client, error) {
	// TODO: Implementar
	return &Client{}, nil
}

// Publish - Publica uma mensagem
func (c *Client) Publish(topic string, data interface{}) error {
	// TODO: Implementar
	return nil
}

// Subscribe - Se inscreve em um tópico
func (c *Client) Subscribe(topic string) (<-chan Message, error) {
	// TODO: Implementar
	return nil, nil
}

// Unsubscribe - Remove inscrição de um tópico
func (c *Client) Unsubscribe(topic string) error {
	// TODO: Implementar
	return nil
}

// Close - Fecha a conexão
func (c *Client) Close() error {
	// TODO: Implementar
	return nil
}
