package client

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
)

var (
	errBrokerClosed = errors.New("broker connection closed")
	errAckTimeout   = errors.New("ack timeout")
)

const publishRetryDelay = 300 * time.Millisecond

type brokerConn struct {
	// addr identifica o broker (host:port).
	addr string
	// conn e o socket TCP aberto.
	conn net.Conn
	// enc/dec fazem encode/decode JSON.
	enc *json.Encoder
	dec *json.Decoder
	// send e o buffer de envio para o broker.
	send chan frame
	// quit sinaliza encerramento da conexao.
	quit chan struct{}
	// closeOnce evita fechamento duplo.
	closeOnce sync.Once
	// wg espera reader/writer finalizarem.
	wg sync.WaitGroup
}

func (bc *brokerConn) close() {
	// Fecha uma unica vez para evitar panics.
	bc.closeOnce.Do(func() {
		close(bc.quit)
		close(bc.send)
		_ = bc.conn.Close()
	})
}

func (c *Client) connForTopic(topic string) (*brokerConn, error) {
	// Resolve broker por hash do topico e faz failover se necessario.
	order := c.brokerOrder(topic)
	if len(order) == 0 {
		return nil, errors.New("broker address is required")
	}

	var lastErr error
	for _, addr := range order {
		bc, err := c.getConn(addr)
		if err == nil {
			return bc, nil
		}
		lastErr = err
	}

	if lastErr == nil {
		lastErr = errors.New("no brokers available")
	}
	return nil, lastErr
}

func (c *Client) getConn(addr string) (*brokerConn, error) {
	c.mu.Lock()
	// Nao abre novas conexoes quando fechado.
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("client is closed")
	}
	// Reaproveita conexao existente.
	if bc, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		return bc, nil
	}
	c.mu.Unlock()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	bc := &brokerConn{
		addr: addr,
		conn: conn,
		enc:  json.NewEncoder(conn),
		dec:  json.NewDecoder(conn),
		send: make(chan frame, brokerSendSize),
		quit: make(chan struct{}),
	}

	bc.wg.Add(2)
	// Inicia loop de escrita e leitura em paralelo.
	go c.writerLoop(bc)
	go c.readerLoop(bc)

	c.mu.Lock()
	// Se o cliente foi fechado durante a conexao, encerra.
	if c.closed {
		c.mu.Unlock()
		bc.close()
		bc.wg.Wait()
		return nil, errors.New("client is closed")
	}
	// Caso outra goroutine ja tenha aberto conexao, reusa.
	if existing, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		bc.close()
		bc.wg.Wait()
		return existing, nil
	}
	c.conns[addr] = bc
	c.mu.Unlock()

	return bc, nil
}

func (c *Client) writerLoop(bc *brokerConn) {
	defer bc.wg.Done()
	// Consome o canal de envio e escreve no socket.
	for {
		select {
		case f, ok := <-bc.send:
			if !ok {
				return
			}
			_ = bc.enc.Encode(&f)
		case <-bc.quit:
			return
		}
	}
}

func (c *Client) readerLoop(bc *brokerConn) {
	defer bc.wg.Done()
	defer func() {
		bc.close()
		c.removeConn(bc.addr, bc)
	}()

	// Le frames do broker e despacha para handlers locais.

	for {
		var f frame
		if err := bc.dec.Decode(&f); err != nil {
			return
		}
		switch f.Type {
		case "message":
			c.dispatchMessage(bc, f)
		case "ack":
			c.dispatchAck(f)
		default:
			// Ignora frames desconhecidos
		}
	}
}

func (c *Client) dispatchMessage(bc *brokerConn, f frame) {
	if f.Topic == "" {
		return
	}

	c.mu.Lock()
	ch := c.subs[f.Topic]
	c.mu.Unlock()
	if ch == nil {
		return
	}

	msg := Message{Topic: f.Topic, Data: f.Data}
	// Entrega bloqueante garante at-least-once para o consumidor.
	ch <- msg

	if f.ID == "" {
		return
	}

	ack := frame{
		Type:  "delivery_ack",
		ID:    f.ID,
		Topic: f.Topic,
	}

	select {
	case bc.send <- ack:
	case <-bc.quit:
	}
}

func (c *Client) dispatchAck(f frame) {
	if f.ID == "" {
		return
	}

	c.mu.Lock()
	ch := c.pending[f.ID]
	if ch != nil {
		delete(c.pending, f.ID)
	}
	c.mu.Unlock()

	if ch != nil {
		// Entrega o ACK para quem esta aguardando.
		ch <- f
		close(ch)
	}
}

func (c *Client) sendAndWait(bc *brokerConn, f frame) error {
	if f.ID == "" {
		// Gera ID se nao fornecido.
		f.ID = c.nextID()
	}

	for {
		err := c.sendOnceAndWait(bc, f)
		if err == nil {
			return nil
		}
		if err != errAckTimeout && err != errBrokerClosed {
			return err
		}

		// Tenta outro broker (ou o mesmo) apos uma pausa curta.
		time.Sleep(publishRetryDelay)
		next, nextErr := c.connForTopic(f.Topic)
		if nextErr != nil {
			return err
		}
		bc = next
	}
}

func (c *Client) sendOnceAndWait(bc *brokerConn, f frame) error {

	ch := make(chan frame, 1)
	c.mu.Lock()
	// Nao aceita novas operacoes se fechado.
	if c.closed {
		c.mu.Unlock()
		return errors.New("client is closed")
	}
	// Registra o canal de resposta do ACK.
	c.pending[f.ID] = ch
	c.mu.Unlock()

	select {
	case bc.send <- f:
	case <-bc.quit:
		// Conexao caiu antes de enviar.
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return errBrokerClosed
	}

	select {
	case ack := <-ch:
		if !ack.Ok {
			if ack.Error == "" {
				return errors.New("request failed")
			}
			return errors.New(ack.Error)
		}
		return nil
	case <-time.After(ackTimeout):
		// Timeout evita bloqueio infinito.
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return errAckTimeout
	}
}

func (c *Client) removeConn(addr string, bc *brokerConn) {
	c.mu.Lock()
	if cur, ok := c.conns[addr]; ok && cur == bc {
		delete(c.conns, addr)
	}
	c.mu.Unlock()

	// Tenta recuperar assinaturas ligadas ao broker que caiu.
	go c.recoverSubscriptions(addr)
}
