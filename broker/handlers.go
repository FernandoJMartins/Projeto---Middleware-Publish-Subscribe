package main

// handleSubscribe - Registra o cliente no tópico.
func (b *Broker) handleSubscribe(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	// Cria topico sob demanda.

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
		brokerLog.Infof("Topico criado: %s", f.Topic)
	}
	t.subs[c] = struct{}{}
	c.subs[f.Topic] = struct{}{}
	b.mu.Unlock()

	brokerLog.Infof("Cliente %s inscrito em %s", c.addr, f.Topic)

	_ = b.sendAck(c, f.ID, true, "")
}

// handlePublish - Publica mensagem em um tópico.
func (b *Broker) handlePublish(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	// Se nao houver topico, nao existem inscritos.

	b.mu.RLock()
	t := b.topics[f.Topic]
	b.mu.RUnlock()
	if t == nil {
		brokerLog.Warnf("Publish descartado (sem inscritos): topic=%s client=%s", f.Topic, c.addr)
		_ = b.sendAck(c, f.ID, false, "no_subscribers")
		return
	}

	msg := frame{
		Type:  "message",
		Topic: f.Topic,
		Data:  f.Data,
	}
	t.inbox <- msg

	brokerLog.Debugf("Publish aceito: topic=%s client=%s", f.Topic, c.addr)

	_ = b.sendAck(c, f.ID, true, "")
}

// handleUnsubscribe - Remove o cliente do tópico.
func (b *Broker) handleUnsubscribe(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	b.mu.Lock()
	t := b.topics[f.Topic]
	if t == nil {
		b.mu.Unlock()
		_ = b.sendAck(c, f.ID, false, "topic not found")
		return
	}

	delete(t.subs, c)
	delete(c.subs, f.Topic)
	if len(t.subs) == 0 {
		delete(b.topics, f.Topic)
		close(t.quit)
		brokerLog.Infof("Topico removido (sem inscritos): %s", f.Topic)
	}
	b.mu.Unlock()

	brokerLog.Infof("Cliente %s saiu de %s", c.addr, f.Topic)

	_ = b.sendAck(c, f.ID, true, "")
}

// sendAck - Responde com ACK/erro para o cliente.
func (b *Broker) sendAck(c *clientConn, id string, ok bool, errMsg string) error {
	ack := frame{
		Type:  "ack",
		ID:    id,
		Ok:    ok,
		Error: errMsg,
	}

	// Envia o ACK pelo canal de envio (writerLoop).
	c.send <- ack
	return nil
}
