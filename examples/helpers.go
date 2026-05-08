package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"middleware-pubsub/client"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func interruptChan() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	return ch
}

func newRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func printLoop(topic string, ch <-chan client.Message) {
	for msg := range ch {
		fmt.Printf("[%s] %s\n", topic, string(msg.Data))
	}
}

func pickAction(r *rand.Rand) string {
	actions := []string{"novo", "atualizacao", "cancelamento"}
	return actions[r.Intn(len(actions))]
}

func pickLevel(r *rand.Rand) string {
	levels := []string{"info", "warning", "critical"}
	return levels[r.Intn(len(levels))]
}
