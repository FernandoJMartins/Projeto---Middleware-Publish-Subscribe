package client

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
)

type brokerConn struct {
	addr      string
	conn      net.Conn
	enc       *json.Encoder
	dec       *json.Decoder
	send      chan frame
	quit      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func (bc *brokerConn) close() {
	bc.closeOnce.Do(func() {
		close(bc.quit)
		close(bc.send)
		_ = bc.conn.Close()
	})
}

func (c *Client) connForTopic(topic string) (*brokerConn, error) {
	addr := c.pickBroker(topic)
	return c.getConn(addr)
}

func (c *Client) getConn(addr string) (*brokerConn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("client is closed")
	}
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
	go c.writerLoop(bc)
	go c.readerLoop(bc)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		bc.close()
		bc.wg.Wait()
		return nil, errors.New("client is closed")
	}
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

	for {
		var f frame
		if err := bc.dec.Decode(&f); err != nil {
			return
		}
		switch f.Type {
		case "message":
			c.dispatchMessage(f)
		case "ack":
			c.dispatchAck(f)
		default:
			// Ignora frames desconhecidos
		}
	}
}

func (c *Client) dispatchMessage(f frame) {
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
	select {
	case ch <- msg:
	default:
		// Evita bloquear a leitura caso o consumidor esteja lento.
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
		ch <- f
		close(ch)
	}
}

func (c *Client) sendAndWait(bc *brokerConn, f frame) error {
	if f.ID == "" {
		f.ID = c.nextID()
	}

	ch := make(chan frame, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("client is closed")
	}
	c.pending[f.ID] = ch
	c.mu.Unlock()

	select {
	case bc.send <- f:
	case <-bc.quit:
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return errors.New("broker connection closed")
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
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return errors.New("ack timeout")
	}
}

func (c *Client) removeConn(addr string, bc *brokerConn) {
	c.mu.Lock()
	if cur, ok := c.conns[addr]; ok && cur == bc {
		delete(c.conns, addr)
	}
	c.mu.Unlock()
}
