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
	fmt.Println("Publisher 1 iniciado")

	addr := getEnv("BROKER_ADDR", "localhost:9000")
	c, err := client.NewClient(addr)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	randSrc := rand.New(rand.NewSource(time.Now().UnixNano()))
	tLoc := time.NewTicker(2 * time.Second)
	tTemp := time.NewTicker(3 * time.Second)
	defer tLoc.Stop()
	defer tTemp.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

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

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
