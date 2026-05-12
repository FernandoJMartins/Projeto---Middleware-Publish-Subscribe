package client

import (
	"middleware-pubsub/common/protocol"
	"time"
)

const (
	// brokerSendSize define o buffer de envio por conexao.
	brokerSendSize = 256
	// subBufferSize define o buffer por topico no cliente.
	subBufferSize = 64
	// ackTimeout e o tempo maximo de espera por ACK.
	ackTimeout = 5 * time.Second
)

// frame - Alias local do protocolo compartilhado.
type frame = protocol.Frame
