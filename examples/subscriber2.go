package main

import (
	"fmt"
	"log"

	"middleware-pubsub/client"
)

func runSubscriber2() {
	fmt.Println("Subscriber 2 iniciado")

	addr := getEnv("BROKER_ADDR", "localhost:9000")
	c, err := client.NewClient(addr)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	chTemp, err := c.Subscribe("temperatura")
	if err != nil {
		log.Fatal(err)
	}
	chAlert, err := c.Subscribe("alerta")
	if err != nil {
		log.Fatal(err)
	}

	go printLoop("temperatura", chTemp)
	go printLoop("alerta", chAlert)

	stop := interruptChan()
	<-stop
}
