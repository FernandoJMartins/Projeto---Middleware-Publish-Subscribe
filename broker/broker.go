package main

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
)

// Broker - Coordena tópicos, clientes e conexões.
type Broker struct {
	mu       sync.RWMutex
	topics   map[string]*topic
	clients  map[*clientConn]struct{}
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
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
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.listener = ln

	b.wg.Add(1)
	go b.acceptLoop()
	return nil
}

// Stop - Finaliza o broker e encerra as goroutines.
func (b *Broker) Stop() error {
	if b.listener == nil {
		return errors.New("broker not started")
	}
	close(b.quit)
	_ = b.listener.Close()

	b.mu.Lock()
	for c := range b.clients {
		_ = c.conn.Close()
	}
	for _, t := range b.topics {
		close(t.quit)
	}
	b.mu.Unlock()

	b.wg.Wait()
	return nil
}

// acceptLoop - Aceita clientes e cria loops de leitura/escrita.
func (b *Broker) acceptLoop() {
	defer b.wg.Done()

	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.quit:
				return
			default:
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		c := &clientConn{
			conn: conn,
			enc:  json.NewEncoder(conn),
			dec:  json.NewDecoder(conn),
			send: make(chan frame, clientSendSize),
			subs: make(map[string]struct{}),
			quit: make(chan struct{}),
		}

		b.mu.Lock()
		b.clients[c] = struct{}{}
		b.mu.Unlock()

		b.wg.Add(2)
		go b.writerLoop(c)
		go b.readerLoop(c)
	}
}
