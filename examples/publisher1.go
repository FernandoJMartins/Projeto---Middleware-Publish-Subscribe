package main

import (
	"fmt"
	"log"
	"time"

	"middleware-pubsub/client"
)

func runPublisher1() {
	fmt.Println("Publisher 1 iniciado")

	addr := getEnv("BROKER_ADDR", "localhost:9000")
	c, err := client.NewClient(addr)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	randSrc := newRand()
	tLoc := time.NewTicker(2 * time.Second)
	tTemp := time.NewTicker(3 * time.Second)
	defer tLoc.Stop()
	defer tTemp.Stop()

	stop := interruptChan()

	for {
		select {
		case <-tLoc.C:
			data := map[string]float64{
				"lat": -7.12 + randSrc.Float64()*0.02,
				"lng": -34.86 + randSrc.Float64()*0.02,
			}
			if err := c.Publish("localizacao", data); err != nil {
				log.Println("publish localizacao:", err)
			}
		case <-tTemp.C:
			data := map[string]interface{}{
				"value": 20 + randSrc.Float64()*10,
				"unit":  "C",
			}
			if err := c.Publish("temperatura", data); err != nil {
				log.Println("publish temperatura:", err)
			}
		case <-stop:
			return
		}
	}
}
