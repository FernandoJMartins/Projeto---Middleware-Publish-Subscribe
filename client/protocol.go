package client

import (
	"middleware-pubsub/common/protocol"
	"time"
)

const (
	brokerSendSize = 256
	subBufferSize  = 64
	ackTimeout     = 5 * time.Second
)

// frame - Alias local do protocolo compartilhado.
type frame = protocol.Frame
