package client

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

const rebalanceInterval = 5 * time.Second

// Message - Estrutura de mensagem
type Message struct {
	// Topic e o topico ao qual a mensagem pertence.
	Topic string `json:"topic"`
	// Data e o payload JSON bruto (a aplicacao deve interpretar os campos).
	Data json.RawMessage `json:"data"`
}

// Client - Cliente pub/sub
type Client struct {
	mu sync.Mutex

	// brokers e a lista de enderecos conhecidos (host:port).
	brokers []string
	// conns cacheia conexoes TCP abertas por endereco.
	conns map[string]*brokerConn
	// subs guarda canais de entrega por topico.
	subs map[string]chan Message
	// subBrokers mapeia topico -> broker atual (para failover e rebalanceamento).
	subBrokers map[string]string
	// pending correlaciona requisicoes com respostas ACK.
	pending map[string]chan frame

	// rebalanceQuit encerra o loop de rebalanceamento.
	rebalanceQuit chan struct{}

	// idCounter gera IDs unicos para as requisicoes.
	idCounter uint64
	// closed marca o cliente como encerrado.
	closed bool
}

// NewClient - Cria um novo cliente
func NewClient(brokerAddr string) (*Client, error) {
	// Aceita lista separada por virgula: "host1:9000,host2:9001".
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

	c := &Client{
		brokers:       brokers,
		conns:         make(map[string]*brokerConn),
		subs:          make(map[string]chan Message),
		subBrokers:    make(map[string]string),
		pending:       make(map[string]chan frame),
		rebalanceQuit: make(chan struct{}),
	}

	// Loop de rebalanceamento tenta voltar topicos ao broker preferido.
	go c.rebalanceLoop()

	return c, nil
}

// Publish - Publica uma mensagem
func (c *Client) Publish(topic string, data interface{}) error {
	// Publicacao exige topico valido.
	if topic == "" {
		return errors.New("topic is required")
	}
	// Serializa o payload para JSON antes de enviar.
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	// Seleciona o broker pela regra de balanceamento.
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
	// Inscricao exige topico valido.
	if topic == "" {
		return nil, errors.New("topic is required")
	}

	bc, err := c.connForTopic(topic)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	// Garante que o cliente ainda esta ativo.
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("client is closed")
	}
	// Reaproveita canal se ja estiver inscrito.
	if ch, ok := c.subs[topic]; ok {
		c.mu.Unlock()
		return ch, nil
	}
	// Cria canal bufferizado para este topico.
	ch := make(chan Message, subBufferSize)
	c.subs[topic] = ch
	c.mu.Unlock()

	f := frame{
		Type:  "subscribe",
		Topic: topic,
	}
	if err := c.sendAndWait(bc, f); err != nil {
		// Em falha, remove o canal criado e devolve erro.
		c.mu.Lock()
		delete(c.subs, topic)
		c.mu.Unlock()
		close(ch)
		return nil, err
	}

	// Registra o broker atual da assinatura.
	c.mu.Lock()
	c.subBrokers[topic] = bc.addr
	c.mu.Unlock()

	return ch, nil
}

// Unsubscribe - Remove inscrição de um tópico
func (c *Client) Unsubscribe(topic string) error {
	// Remove a inscricao, caso exista.
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
	delete(c.subBrokers, topic)
	c.mu.Unlock()
	if ch != nil {
		// Fecha o canal para avisar o consumidor.
		close(ch)
	}
	return nil
}

// Close - Fecha a conexão
func (c *Client) Close() error {
	c.mu.Lock()
	// Fecha apenas uma vez.
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.rebalanceQuit)

	for _, ch := range c.subs {
		// Fecha canais de assinaturas ativas.
		close(ch)
	}
	for _, ch := range c.pending {
		// Fecha aguardas pendentes para nao vazar goroutines.
		close(ch)
	}
	c.subs = make(map[string]chan Message)
	c.subBrokers = make(map[string]string)
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

// recoverSubscriptions tenta reabrir assinaturas ligadas ao broker que caiu.
func (c *Client) recoverSubscriptions(badAddr string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	topics := make([]string, 0, len(c.subBrokers))
	for topic, addr := range c.subBrokers {
		if addr == badAddr {
			topics = append(topics, topic)
		}
	}
	c.mu.Unlock()

	for _, topic := range topics {
		bc, err := c.connForTopic(topic)
		if err != nil {
			continue
		}
		f := frame{Type: "subscribe", Topic: topic}
		if err := c.sendAndWait(bc, f); err != nil {
			continue
		}
		c.mu.Lock()
		if !c.closed {
			c.subBrokers[topic] = bc.addr
		}
		c.mu.Unlock()
	}
}

// rebalanceLoop tenta trazer topicos de volta ao broker preferido quando ele volta.
func (c *Client) rebalanceLoop() {
	ticker := time.NewTicker(rebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.rebalanceSubscriptions()
		case <-c.rebalanceQuit:
			return
		}
	}
}

// rebalanceSubscriptions move topicos para o broker preferido se ele estiver disponivel.
func (c *Client) rebalanceSubscriptions() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	topics := make([]string, 0, len(c.subs))
	current := make(map[string]string, len(c.subBrokers))
	for topic := range c.subs {
		current[topic] = c.subBrokers[topic]
		topics = append(topics, topic)
	}
	c.mu.Unlock()

	for _, topic := range topics {
		preferred := c.preferredBroker(topic)
		if preferred == "" || current[topic] == preferred {
			continue
		}

		bc, err := c.getConn(preferred)
		if err != nil {
			continue
		}

		f := frame{Type: "subscribe", Topic: topic}
		if err := c.sendAndWait(bc, f); err != nil {
			continue
		}

		c.mu.Lock()
		if !c.closed {
			c.subBrokers[topic] = preferred
		}
		c.mu.Unlock()

		// Se o broker antigo ainda existir, remove a assinatura antiga.
		old := current[topic]
		if old != "" && old != preferred {
			if oldConn, err := c.getConn(old); err == nil {
				_ = c.sendAndWait(oldConn, frame{Type: "unsubscribe", Topic: topic})
			}
		}
	}
}
