package main

import "middleware-pubsub/common/protocol"

const (
	topicInboxSize = 256 // Tamanho do canal de mensagens por tópico
	clientSendSize = 256 // Tamanho do canal de envio por cliente
)

// frame - Alias local do protocolo compartilhado.
type frame = protocol.Frame
