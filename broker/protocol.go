package main

import (
	"time"

	"middleware-pubsub/common/protocol"
)

const (
	// topicInboxSize define o buffer de mensagens por topico.
	topicInboxSize = 256
	// clientSendSize define o buffer de envio por cliente.
	clientSendSize = 256
	// deliveryRetryInterval define o intervalo de reenvio de mensagens nao confirmadas.
	deliveryRetryInterval = 2 * time.Second
)

// frame - Alias local do protocolo compartilhado.
type frame = protocol.Frame
