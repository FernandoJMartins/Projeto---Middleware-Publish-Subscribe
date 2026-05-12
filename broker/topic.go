package main

// topic - Estado e fila de mensagens de um tópico.
type topic struct {
	// name e o identificador do topico.
	name string
	// inbox bufferiza mensagens publicadas.
	inbox chan frame
	// quit encerra o loop do topico.
	quit chan struct{}
	// subs guarda os clientes inscritos.
	subs map[*clientConn]struct{}
}

// topicLoop - Encaminha mensagens do tópico para os inscritos.
func (b *Broker) topicLoop(t *topic) {
	defer b.wg.Done()
	brokerLog.Infof("Loop do topico iniciado: %s", t.name)
	for {
		select {
		case msg := <-t.inbox:
			// Copia a lista de inscritos para evitar segurar o lock durante o envio.
			b.mu.RLock()
			subs := make([]*clientConn, 0, len(t.subs))
			for c := range t.subs {
				subs = append(subs, c)
			}
			b.mu.RUnlock()

			brokerLog.Debugf("Fanout topic=%s subs=%d", t.name, len(subs))

			for _, c := range subs {
				b.enqueueDelivery(c, msg)
			}
		case <-t.quit:
			brokerLog.Infof("Loop do topico finalizado: %s", t.name)
			return
		}
	}
}
