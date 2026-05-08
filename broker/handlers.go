package main

// handleSubscribe - Registra o cliente no tópico.
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
	t.subs[c] = struct{}{}
	c.subs[f.Topic] = struct{}{}
	b.mu.Unlock()

	_ = b.sendAck(c, f.ID, true, "")
}

// handlePublish - Publica mensagem em um tópico.
func (b *Broker) handlePublish(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	b.mu.RLock()
	t := b.topics[f.Topic]
	b.mu.RUnlock()
	if t == nil {
		_ = b.sendAck(c, f.ID, false, "no_subscribers")
		return
	}

	msg := frame{
		Type:  "message",
		Topic: f.Topic,
		Data:  f.Data,
	}
	t.inbox <- msg

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
	}
	b.mu.Unlock()

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
	c.send <- ack
	return nil
}
