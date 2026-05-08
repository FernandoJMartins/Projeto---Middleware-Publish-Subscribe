package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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

type brokerConn struct {
	addr      string
	conn      net.Conn
	enc       *json.Encoder
	dec       *json.Decoder
	send      chan frame
	quit      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func (bc *brokerConn) close() {
	bc.closeOnce.Do(func() {
		close(bc.quit)
		close(bc.send)
		_ = bc.conn.Close()
	})
}

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

func (c *Client) connForTopic(topic string) (*brokerConn, error) {
	addr := c.pickBroker(topic)
	return c.getConn(addr)
}

func (c *Client) getConn(addr string) (*brokerConn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("client is closed")
	}
	if bc, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		return bc, nil
	}
	c.mu.Unlock()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	bc := &brokerConn{
		addr: addr,
		conn: conn,
		enc:  json.NewEncoder(conn),
		dec:  json.NewDecoder(conn),
		send: make(chan frame, brokerSendSize),
		quit: make(chan struct{}),
	}

	bc.wg.Add(2)
	go c.writerLoop(bc)
	go c.readerLoop(bc)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		bc.close()
		bc.wg.Wait()
		return nil, errors.New("client is closed")
	}
	if existing, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		bc.close()
		bc.wg.Wait()
		return existing, nil
	}
	c.conns[addr] = bc
	c.mu.Unlock()

	return bc, nil
}

func (c *Client) writerLoop(bc *brokerConn) {
	defer bc.wg.Done()
	for {
		select {
		case f, ok := <-bc.send:
			if !ok {
				return
			}
			_ = bc.enc.Encode(&f)
		case <-bc.quit:
			return
		}
	}
}

func (c *Client) readerLoop(bc *brokerConn) {
	defer bc.wg.Done()
	defer func() {
		bc.close()
		c.removeConn(bc.addr, bc)
	}()

	for {
		var f frame
		if err := bc.dec.Decode(&f); err != nil {
			return
		}
		switch f.Type {
		case "message":
			c.dispatchMessage(f)
		case "ack":
			c.dispatchAck(f)
		default:
			// Ignora frames desconhecidos
		}
	}
}

func (c *Client) dispatchMessage(f frame) {
	if f.Topic == "" {
		return
	}

	c.mu.Lock()
	ch := c.subs[f.Topic]
	c.mu.Unlock()
	if ch == nil {
		return
	}

	msg := Message{Topic: f.Topic, Data: f.Data}
	select {
	case ch <- msg:
	default:
		// Evita bloquear a leitura caso o consumidor esteja lento.
	}
}

func (c *Client) dispatchAck(f frame) {
	if f.ID == "" {
		return
	}

	c.mu.Lock()
	ch := c.pending[f.ID]
	if ch != nil {
		delete(c.pending, f.ID)
	}
	c.mu.Unlock()

	if ch != nil {
		ch <- f
		close(ch)
	}
}

func (c *Client) sendAndWait(bc *brokerConn, f frame) error {
	if f.ID == "" {
		f.ID = c.nextID()
	}

	ch := make(chan frame, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("client is closed")
	}
	c.pending[f.ID] = ch
	c.mu.Unlock()

	select {
	case bc.send <- f:
	case <-bc.quit:
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return errors.New("broker connection closed")
	}

	select {
	case ack := <-ch:
		if !ack.Ok {
			if ack.Error == "" {
				return errors.New("request failed")
			}
			return errors.New(ack.Error)
		}
		return nil
	case <-time.After(ackTimeout):
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return errors.New("ack timeout")
	}
}

func (c *Client) removeConn(addr string, bc *brokerConn) {
	c.mu.Lock()
	if cur, ok := c.conns[addr]; ok && cur == bc {
		delete(c.conns, addr)
	}
	c.mu.Unlock()
}

func (c *Client) nextID() string {
	n := atomic.AddUint64(&c.idCounter, 1)
	return fmt.Sprintf("req-%d", n)
}

func (c *Client) pickBroker(topic string) string {
	if len(c.brokers) == 1 {
		return c.brokers[0]
	}
	if topic == "" {
		return c.brokers[0]
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	idx := int(h.Sum32()) % len(c.brokers)
	return c.brokers[idx]
}
