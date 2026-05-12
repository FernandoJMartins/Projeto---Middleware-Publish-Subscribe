package main

import (
	"math/rand"
	"sync"
	"time"

	"middleware-pubsub/client"
	"middleware-pubsub/common/logx"
)

type topicSpec struct {
	name     string
	interval time.Duration
	build    func(r *rand.Rand) interface{}
}

func runPublisher() {
	log := newLogger("PUB-IOT", logx.ColorGreen)
	log.Infof("Publisher IoT Industrial iniciado")

	addr := getBrokerAddr()
	c, err := client.NewClient(addr)
	if err != nil {
		log.Errorf("Falha ao criar client: %v", err)
		return
	}
	defer c.Close()
	log.Infof("Brokers configurados: %s", addr)

	specs := []topicSpec{
		{
			name:     TopicTemperatura,
			interval: 2 * time.Second,
			build: func(r *rand.Rand) interface{} {
				return map[string]interface{}{
					"valor":  70 + r.Float64()*10,
					"unit":   "C",
					"sensor": "T-01",
				}
			},
		},
		{
			name:     TopicPressao,
			interval: 3 * time.Second,
			build: func(r *rand.Rand) interface{} {
				return map[string]interface{}{
					"valor": 3.0 + r.Float64()*1.5,
					"unit":  "bar",
					"linha": "A",
				}
			},
		},
		{
			name:     TopicFalhaMotor,
			interval: 5 * time.Second,
			build: func(r *rand.Rand) interface{} {
				severidades := []string{"baixa", "media", "alta"}
				return map[string]interface{}{
					"codigo":    int(100 + r.Float64()*50),
					"gravidade": severidades[r.Intn(len(severidades))],
					"motor":     "M-3",
				}
			},
		},
		{
			name:     TopicConsumo,
			interval: 4 * time.Second,
			build: func(r *rand.Rand) interface{} {
				return map[string]interface{}{
					"kwh":   120 + r.Float64()*35,
					"linha": "B",
				}
			},
		},
	}

	// stop fecha quando chega CTRL+C, para encerrar todas as goroutines.
	stop := make(chan struct{})
	go func() {
		<-interruptChan()
		close(stop)
	}()

	var wg sync.WaitGroup
	for idx, spec := range specs {
		spec := spec
		seed := time.Now().UnixNano() + int64(idx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			ticker := time.NewTicker(spec.interval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					payload := spec.build(r)
					log.Infof("PUB topic=%s payload=%s", spec.name, formatPayload(payload))
					if err := c.Publish(spec.name, payload); err != nil {
						log.Errorf("publish %s: %v", spec.name, err)
					}
				case <-stop:
					return
				}
			}
		}()
	}

	<-stop
	log.Infof("Publisher IoT Industrial encerrando")
	wg.Wait()
}
