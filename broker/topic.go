package main

// topic - Estado e fila de mensagens de um tópico.
type topic struct {
	name  string
	inbox chan frame
	quit  chan struct{}
	subs  map[*clientConn]struct{}
}

// topicLoop - Encaminha mensagens do tópico para os inscritos.
func (b *Broker) topicLoop(t *topic) {
	defer b.wg.Done()
	for {
		select {
		case msg := <-t.inbox:
			b.mu.RLock()
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
