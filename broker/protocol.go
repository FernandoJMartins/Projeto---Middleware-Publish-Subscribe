package main

import "middleware-pubsub/common/protocol"

const (
	// topicInboxSize define o buffer de mensagens por topico.
	topicInboxSize = 256
	// clientSendSize define o buffer de envio por cliente.
	clientSendSize = 256
)

// frame - Alias local do protocolo compartilhado.
type frame = protocol.Frame
