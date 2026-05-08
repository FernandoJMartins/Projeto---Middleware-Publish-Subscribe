package client

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
)

// Message - Estrutura de mensagem
type Message struct {
	Topic string          `json:"topic"`
	Data  json.RawMessage `json:"data"`
}

// Client - Cliente pub/sub
type Client struct {
	mu        sync.Mutex
	brokers   []string
	conns     map[string]*brokerConn
	subs      map[string]chan Message
	pending   map[string]chan frame
	idCounter uint64
	closed    bool
}

// NewClient - Cria um novo cliente
func NewClient(brokerAddr string) (*Client, error) {
	parts := strings.Split(brokerAddr, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		addr := strings.TrimSpace(p)
		if addr != "" {
			brokers = append(brokers, addr)
		}
	}
	if len(brokers) == 0 {
		return nil, errors.New("broker address is required")
	}

	return &Client{
		brokers: brokers,
		conns:   make(map[string]*brokerConn),
		subs:    make(map[string]chan Message),
		pending: make(map[string]chan frame),
	}, nil
}

// Publish - Publica uma mensagem
func (c *Client) Publish(topic string, data interface{}) error {
	if topic == "" {
		return errors.New("topic is required")
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	bc, err := c.connForTopic(topic)
	if err != nil {
		return err
	}

	f := frame{
		Type:  "publish",
		Topic: topic,
		Data:  payload,
	}
	return c.sendAndWait(bc, f)
}

// Subscribe - Se inscreve em um tópico
func (c *Client) Subscribe(topic string) (<-chan Message, error) {
	if topic == "" {
		return nil, errors.New("topic is required")
	}

	bc, err := c.connForTopic(topic)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("client is closed")
	}
	if ch, ok := c.subs[topic]; ok {
		c.mu.Unlock()
		return ch, nil
	}
	ch := make(chan Message, subBufferSize)
	c.subs[topic] = ch
	c.mu.Unlock()

	f := frame{
		Type:  "subscribe",
		Topic: topic,
	}
	if err := c.sendAndWait(bc, f); err != nil {
		c.mu.Lock()
		delete(c.subs, topic)
		c.mu.Unlock()
		close(ch)
		return nil, err
	}

	return ch, nil
}

// Unsubscribe - Remove inscrição de um tópico
func (c *Client) Unsubscribe(topic string) error {
	if topic == "" {
		return errors.New("topic is required")
	}

	bc, err := c.connForTopic(topic)
	if err != nil {
		return err
	}

	f := frame{
		Type:  "unsubscribe",
		Topic: topic,
	}
	if err := c.sendAndWait(bc, f); err != nil {
		return err
	}

	c.mu.Lock()
	ch := c.subs[topic]
	delete(c.subs, topic)
	c.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}

// Close - Fecha a conexão
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true

	for _, ch := range c.subs {
		close(ch)
	}
	for _, ch := range c.pending {
		close(ch)
	}
	c.subs = make(map[string]chan Message)
	c.pending = make(map[string]chan frame)

	conns := make([]*brokerConn, 0, len(c.conns))
	for _, bc := range c.conns {
		conns = append(conns, bc)
	}
	c.conns = make(map[string]*brokerConn)
	c.mu.Unlock()

	for _, bc := range conns {
		bc.close()
		bc.wg.Wait()
	}
	return nil
}
