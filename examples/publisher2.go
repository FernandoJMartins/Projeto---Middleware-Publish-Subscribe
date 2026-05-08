package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"middleware-pubsub/client"
)

func main() {
	fmt.Println("Publisher 2 iniciado")

	addr := getEnv("BROKER_ADDR", "localhost:9000")
	c, err := client.NewClient(addr)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	randSrc := rand.New(rand.NewSource(time.Now().UnixNano()))
	tUser := time.NewTicker(4 * time.Second)
	tAlert := time.NewTicker(5 * time.Second)
	defer tUser.Stop()
	defer tAlert.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	userID := 1000
	for {
		select {
		case <-tUser.C:
			userID++
			data := map[string]interface{}{
				"user_id": userID,
				"action":  pickAction(randSrc),
			}
			if err := c.Publish("inscricao_usuario", data); err != nil {
				log.Println("publish inscricao_usuario:", err)
			}
		case <-tAlert.C:
			data := map[string]interface{}{
				"level":   pickLevel(randSrc),
				"message": "limiar atingido",
			}
			if err := c.Publish("alerta", data); err != nil {
				log.Println("publish alerta:", err)
			}
		case <-stop:
			return
		}
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

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
