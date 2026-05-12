package main

import (
	"encoding/json"
	"net"
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

	close(c.quit)
	_ = c.conn.Close()
}
