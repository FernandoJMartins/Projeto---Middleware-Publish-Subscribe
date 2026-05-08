package main

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
)

const (
	topicInboxSize = 256 // Tamanho do canal de mensagens para cada tópico
	clientSendSize = 256 // Tamanho do canal de envio para cada cliente
)

type frame struct {
	Type  string          `json:"type"`
	ID    string          `json:"id,omitempty"`
	Topic string          `json:"topic,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	Ok    bool            `json:"ok,omitempty"`
	Error string          `json:"error,omitempty"`
}

type topic struct { // Estrutura para representar um tópico
	name  string
	inbox chan frame
	quit  chan struct{}
	subs  map[*clientConn]struct{}
}

type clientConn struct { // Estrutura para representar uma conexão de cliente
	conn net.Conn
	enc  *json.Encoder
	dec  *json.Decoder
	send chan frame
	subs map[string]struct{}
	quit chan struct{}
}

type Broker struct { // Estrutura principal do broker
	mu       sync.RWMutex // Mutex para proteger o acesso às estruturas de dados compartilhadas
	topics   map[string]*topic
	clients  map[*clientConn]struct{}
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
}

// NewBroker - Cria uma nova instância do broker
func NewBroker() *Broker {
	return &Broker{
		topics:  make(map[string]*topic),
		clients: make(map[*clientConn]struct{}),
		quit:    make(chan struct{}),
	}
}

// Start - Inicia o broker
func (b *Broker) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.listener = ln

	b.wg.Add(1) // Inicia a goroutine de aceitação de conexões
	go b.acceptLoop()
	return nil
}

// Stop - Para o broker
func (b *Broker) Stop() error {
	// TODO: Implementar
	if b.listener != nil {
		return errors.New("broker not started")
	}
	close(b.quit)
	_ = b.listener.Close()

	b.mu.Lock()
	for c := range b.clients {
		c.conn.Close()
	}
	for _, t := range b.topics {
		close(t.quit)
	}
	b.mu.Unlock()

	b.wg.Wait() // Aguarda todas as goroutines terminarem
	return nil
}

func (b *Broker) acceptLoop() {
	defer b.wg.Done()

	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.quit:
				return
			default:
				time.Sleep(100 * time.Millisecond) // Evita loop de erro
				continue
			}
		}
		c := &clientConn{
			conn: conn,
			enc:  json.NewEncoder(conn),
			dec:  json.NewDecoder(conn),
			subs: make(map[string]struct{}),
			quit: make(chan struct{}),
		}

		b.mu.Lock()
		b.clients[c] = struct{}{}
		b.mu.Unlock()

		b.wg.Add(2) // Inicia as goroutines de leitura e escrita para o cliente
		go b.writerLoop(c)
		go b.readerLoop(c)
	}
}

func (b *Broker) readerLoop(c *clientConn) {
	defer b.wg.Done()
	defer b.cleanupClient(c)

	for {
		var f frame
		if err := c.dec.Decode(&f); err != nil {
			return
		}

		switch f.Type {
		case "subscribe":
			b.handleSubscribe(c, f)
		case "publish":
			b.handlePublish(c, f)
		case "unsubscribe":
			b.handleUnsubscribe(c, f)
		default:
			_ = b.sendAck(c, f.ID, false, "unknown frame type")
		}
	}
}

func (b *Broker) writerLoop(c *clientConn) {
	defer b.wg.Done()

	for {
		select {
		case f, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.enc.Encode(&f)
		case <-c.quit:
			return
		}
	}
}

func (b *Broker) handleSubscribe(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	b.mu.Lock()
	t := b.topics[f.Topic]
	if t == nil {
		t = &topic{
			name:  f.Topic,
			inbox: make(chan frame, topicInboxSize),
			quit:  make(chan struct{}),
			subs:  make(map[*clientConn]struct{}),
		}
		b.topics[f.Topic] = t
		b.wg.Add(1)
		go b.topicLoop(t)
	}
	t.subs[c] = struct{}{}       // Adiciona o cliente à lista de inscritos do tópico
	c.subs[f.Topic] = struct{}{} // Marca que o cliente está inscrito no tópico
	b.mu.Unlock()                // Protege o acesso às estruturas de dados compartilhadas

	_ = b.sendAck(c, f.ID, true, "")
}

func (b *Broker) handlePublish(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	b.mu.Lock()
	t := b.topics[f.Topic]
	b.mu.RUnlock()
	if t == nil {
		_ = b.sendAck(c, f.ID, false, "topic not found")
		return
	}

	msg := frame{
		Type:  "message",
		Topic: f.Topic,
		Data:  f.Data,
	}
	t.inbox <- msg // Envia a mensagem para o canal do tópico

	_ = b.sendAck(c, f.ID, true, "")
}

func (b *Broker) handleUnsubscribe(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	b.mu.Lock() // Protege o acesso às estruturas de dados compartilhadas
	t := b.topics[f.Topic]
	if t == nil {
		b.mu.Unlock()
		_ = b.sendAck(c, f.ID, false, "topic not found")
		return
	}

}

func (b *Broker) topicLoop(t *topic) {
	defer b.wg.Done()
	for {
		select {
		case msg := <-t.inbox:
			b.mu.RLock() // Protege o acesso às estruturas de dados compartilhadas
			subs := make([]*clientConn, 0, len(t.subs))
			for c := range t.subs {
				subs = append(subs, c)
			}
			b.mu.RUnlock()

			for _, c := range subs {
				select {
				case c.send <- msg:
				case <-c.quit:
				}
			}
		case <-t.quit:
			return

		}
	}
}

func (b *Broker) sendAck(c *clientConn, id string, ok bool, errMsg string) error {
	ack := frame{
		Type:  "ack",
		ID:    id,
		Ok:    ok,
		Error: errMsg,
	}
	c.send <- ack // Envia a resposta de volta para o cliente
	return nil
}

func (b *Broker) cleanupClient(c *clientConn) {
	b.mu.Lock()
	for topicName := range c.subs {
		t := b.topics[topicName]
		delete(t.subs, c) // Remove o cliente da lista de inscritos do tópico
		if len(t.subs) == 0 {
			close(t.quit) // Encerra o tópico se não houver mais inscritos
			delete(b.topics, topicName)
		}
	}
	delete(b.clients, c)
	b.mu.Unlock()
	c.conn.Close() // Fecha a conexão do cliente
}
