package client

import (
	"fmt"
	"hash/fnv"
	"sync/atomic"
)

func (c *Client) nextID() string {
	// Usa contador atomico para gerar IDs unicos e ordenados.
	n := atomic.AddUint64(&c.idCounter, 1)
	return fmt.Sprintf("req-%d", n)
}

func (c *Client) pickBroker(topic string) string {
	order := c.brokerOrder(topic)
	if len(order) == 0 {
		return ""
	}
	return order[0]
}

// brokerOrder retorna a lista de brokers em ordem de preferencia para um topico.
func (c *Client) brokerOrder(topic string) []string {
	if len(c.brokers) == 0 {
		return nil
	}
	if len(c.brokers) == 1 {
		return []string{c.brokers[0]}
	}

	start := 0
	if topic != "" {
		// Hash consistente do topico para manter afinidade.
		h := fnv.New32a()
		_, _ = h.Write([]byte(topic))
		start = int(h.Sum32()) % len(c.brokers)
	}

	order := make([]string, 0, len(c.brokers))
	for i := 0; i < len(c.brokers); i++ {
		idx := (start + i) % len(c.brokers)
		order = append(order, c.brokers[idx])
	}
	return order
}

// preferredBroker retorna o broker principal para um topico.
func (c *Client) preferredBroker(topic string) string {
	order := c.brokerOrder(topic)
	if len(order) == 0 {
		return ""
	}
	return order[0]
}
