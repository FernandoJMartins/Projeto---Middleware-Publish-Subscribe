package main

import (
	"encoding/json"
	"net"
	"sync"
	"time"
)

// clientConn - Conexão ativa com um cliente.
type clientConn struct {
	// addr identifica o cliente nos logs.
	addr string
	// conn e o socket TCP.
	conn net.Conn
	// enc/dec fazem encode/decode JSON linha a linha.
	enc *json.Encoder
	dec *json.Decoder
	// send e o buffer de envio para o cliente (writerLoop).
	send chan frame
	// subs guarda topicos assinados por este cliente.
	subs map[string]struct{}
	// quit sinaliza encerramento do cliente.
	quit chan struct{}
	// pending guarda mensagens enviadas sem confirmacao.
	pending map[string]frame
	// pendingMu protege o mapa pending.
	pendingMu sync.Mutex
	// closeOnce evita fechar canais duas vezes.
	closeOnce sync.Once
}

func (c *clientConn) close() {
	c.closeOnce.Do(func() {
		close(c.quit)
		close(c.send)
		_ = c.conn.Close()
	})
}

// readerLoop - Lê frames do cliente e despacha para o broker.
func (b *Broker) readerLoop(c *clientConn) {
	defer b.wg.Done()
	defer b.cleanupClient(c)
	brokerLog.Debugf("Reader iniciado para %s", c.addr)

	for {
		var f frame
		if err := c.dec.Decode(&f); err != nil {
			brokerLog.Infof("Cliente %s desconectou (reader): %v", c.addr, err)
			return
		}

		switch f.Type {
		case "subscribe":
			b.handleSubscribe(c, f)
		case "publish":
			b.handlePublish(c, f)
		case "unsubscribe":
			b.handleUnsubscribe(c, f)
		case "delivery_ack":
			b.handleDeliveryAck(c, f)
		default:
			brokerLog.Warnf("Frame desconhecido de %s: type=%s", c.addr, f.Type)
			_ = b.sendAck(c, f.ID, false, "unknown frame type")
		}
	}
}

// writerLoop - Envia frames para o cliente.
func (b *Broker) writerLoop(c *clientConn) {
	defer b.wg.Done()
	brokerLog.Debugf("Writer iniciado para %s", c.addr)

	for {
		select {
		case f, ok := <-c.send:
			if !ok {
				return
			}
			if err := c.enc.Encode(&f); err != nil {
				brokerLog.Infof("Falha ao enviar para %s: %v", c.addr, err)
				return
			}
		case <-c.quit:
			return
		}
	}
}

// cleanupClient - Remove o cliente de todos os tópicos e encerra a conexão.
func (b *Broker) cleanupClient(c *clientConn) {
	brokerLog.Infof("Encerrando cliente %s", c.addr)
	b.mu.Lock()
	for topicName := range c.subs {
		t := b.topics[topicName]
		if t == nil {
			continue
		}
		delete(t.subs, c)
		if len(t.subs) == 0 {
			close(t.quit)
			delete(b.topics, topicName)
			brokerLog.Infof("Topico removido (sem inscritos): %s", topicName)
		}
	}
	delete(b.clients, c)
	b.mu.Unlock()

	c.pendingMu.Lock()
	for key := range c.pending {
		delete(c.pending, key)
	}
	c.pendingMu.Unlock()

	c.close()
}

// abaixo é tudo necessário para o mecanismo de at-least-once delivery. Primordial para o Sistema.
func deliveryKey(topic, id string) string {
	return topic + "|" + id
}

// enqueueDelivery registra e envia mensagem para o cliente com at-least-once.
func (b *Broker) enqueueDelivery(c *clientConn, msg frame) {
	if msg.ID == "" || msg.Topic == "" {
		return
	}

	key := deliveryKey(msg.Topic, msg.ID)
	c.pendingMu.Lock()
	if _, ok := c.pending[key]; !ok {
		c.pending[key] = msg
	}
	c.pendingMu.Unlock()

	b.trySend(c, msg)
}

// handleDeliveryAck remove a mensagem confirmada pelo subscriber.
func (b *Broker) handleDeliveryAck(c *clientConn, f frame) {
	if f.ID == "" || f.Topic == "" {
		return
	}

	key := deliveryKey(f.Topic, f.ID)
	c.pendingMu.Lock()
	delete(c.pending, key)
	c.pendingMu.Unlock()
}

// deliveryRetryLoop reenvia mensagens pendentes periodicamente.
func (b *Broker) deliveryRetryLoop(c *clientConn) {
	defer b.wg.Done()
	ticker := time.NewTicker(deliveryRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.retryPending(c)
		case <-c.quit:
			return
		}
	}
}

func (b *Broker) retryPending(c *clientConn) {
	items := make([]frame, 0)
	c.pendingMu.Lock()
	for _, msg := range c.pending {
		items = append(items, msg)
	}
	c.pendingMu.Unlock()

	for _, msg := range items {
		b.trySend(c, msg)
	}
}

func (b *Broker) trySend(c *clientConn, msg frame) {
	select {
	case <-c.quit:
		return
	default:
	}

	select {
	case c.send <- msg:
	default:
		// buffer cheio; sera reenviado pelo retry loop
	}
}
