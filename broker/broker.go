package main

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"middleware-pubsub/common/logx"
)

// brokerLog centraliza os logs do broker com cor consistente.
var brokerLog = logx.New("BROKER", logx.ColorCyan)

// Broker - Coordena tópicos, clientes e conexões.
type Broker struct {
	mu sync.RWMutex // Evita race conditions ao acessar topics e clients.

	// topics guarda os topicos ativos e suas filas de mensagens.
	topics map[string]*topic
	// clients registra conexoes ativas para limpeza coordenada.
	clients map[*clientConn]struct{}

	// listener aceita novas conexoes TCP.
	listener net.Listener
	// quit encerra loops de goroutines quando o broker para.
	quit chan struct{}
	// wg garante que todas as goroutines finalizem no Stop().
	wg sync.WaitGroup
}

// NewBroker - Cria uma nova instância do broker.
func NewBroker() *Broker {
	return &Broker{
		topics:  make(map[string]*topic),
		clients: make(map[*clientConn]struct{}),
		quit:    make(chan struct{}),
	}
}

// Start - Inicia o broker e aceita conexões.
func (b *Broker) Start(addr string) error {
	// Abre o socket TCP para aceitar clientes.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.listener = ln
	brokerLog.Infof("Escutando em %s", addr)

	b.wg.Add(1)
	go b.acceptLoop()
	return nil
}

// Stop - Finaliza o broker e encerra as goroutines.
func (b *Broker) Stop() error {
	if b.listener == nil {
		return errors.New("broker not started")
	}
	brokerLog.Infof("Encerrando broker...")
	close(b.quit)
	_ = b.listener.Close()

	b.mu.Lock() // Fecha conexões dos clientes e sinaliza encerramento dos tópicos.
	for c := range b.clients {
		_ = c.conn.Close()
	}
	for _, t := range b.topics {
		close(t.quit)
	}
	b.mu.Unlock() // Aguarda todas as goroutines finalizarem.

	b.wg.Wait() // Garante que todas as goroutines terminem antes de retornar.
	return nil
}

// acceptLoop - Aceita clientes e cria loops de leitura/escrita.
func (b *Broker) acceptLoop() {
	defer b.wg.Done()
	brokerLog.Infof("Loop de aceitacao iniciado")

	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.quit:
				brokerLog.Infof("Loop de aceitacao finalizado")
				return
			default:
				brokerLog.Warnf("Falha ao aceitar conexao: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		remoteAddr := conn.RemoteAddr().String()
		brokerLog.Infof("Cliente conectado: %s", remoteAddr)

		c := &clientConn{
			addr:    remoteAddr,
			conn:    conn,
			enc:     json.NewEncoder(conn),
			dec:     json.NewDecoder(conn),
			send:    make(chan frame, clientSendSize),
			subs:    make(map[string]struct{}),
			quit:    make(chan struct{}),
			pending: make(map[string]frame),
		}

		b.mu.Lock()
		b.clients[c] = struct{}{}
		b.mu.Unlock()

		b.wg.Add(2)
		go b.writerLoop(c)
		go b.readerLoop(c)
		b.wg.Add(1)
		go b.deliveryRetryLoop(c)
	}
}
