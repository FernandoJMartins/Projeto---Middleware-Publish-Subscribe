package main

import (
	"fmt"
	"log"

	"middleware-pubsub/client"
)

func runSubscriber1() {
	fmt.Println("Subscriber 1 iniciado")

	addr := getEnv("BROKER_ADDR", "localhost:9000")
	c, err := client.NewClient(addr)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	chLoc, err := c.Subscribe("localizacao")
	if err != nil {
		log.Fatal(err)
	}
	chUser, err := c.Subscribe("inscricao_usuario")
	if err != nil {
		log.Fatal(err)
	}

	go printLoop("localizacao", chLoc)
	go printLoop("inscricao_usuario", chUser)

	stop := interruptChan()
	<-stop
}
