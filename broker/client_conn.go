package main

import (
	"encoding/json"
	"net"
)

// clientConn - Conexão ativa com um cliente.
type clientConn struct {
	conn net.Conn
	enc  *json.Encoder
	dec  *json.Decoder
	send chan frame
	subs map[string]struct{}
	quit chan struct{}
}

// readerLoop - Lê frames do cliente e despacha para o broker.
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

// writerLoop - Envia frames para o cliente.
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

// cleanupClient - Remove o cliente de todos os tópicos e encerra a conexão.
func (b *Broker) cleanupClient(c *clientConn) {
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
		}
	}
	delete(b.clients, c)
	b.mu.Unlock()

	close(c.quit)
	_ = c.conn.Close()
}
