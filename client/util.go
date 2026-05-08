package client

import (
	"fmt"
	"hash/fnv"
	"sync/atomic"
)

func (c *Client) nextID() string {
	n := atomic.AddUint64(&c.idCounter, 1)
	return fmt.Sprintf("req-%d", n)
}

func (c *Client) pickBroker(topic string) string {
	if len(c.brokers) == 1 {
		return c.brokers[0]
	}
	if topic == "" {
		return c.brokers[0]
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	idx := int(h.Sum32()) % len(c.brokers)
	return c.brokers[idx]
}
