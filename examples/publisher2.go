package main

import (
	"fmt"
	"log"
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

	randSrc := newRand()
	tUser := time.NewTicker(4 * time.Second)
	tAlert := time.NewTicker(5 * time.Second)
	defer tUser.Stop()
	defer tAlert.Stop()

	stop := interruptChan()

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
