package main

import (
	"strings"

	"middleware-pubsub/client"
	"middleware-pubsub/common/logx"
)

const (
	rolePainel  = "painel"
	roleAlertas = "alertas"
)

// normalizeRole garante um valor valido de role para o subscriber.
func normalizeRole(role string) string {
	role = strings.ToLower(strings.TrimSpace(role))
	switch role {
	case rolePainel, roleAlertas:
		return role
	default:
		return rolePainel
	}
}

func runSubscriber(role string) {
	role = normalizeRole(role)

	var (
		component string
		color     string
		topics    []string
		started   string
		ending    string
	)

	switch role {
	case roleAlertas:
		component = "ALERTAS"
		color = logx.ColorYellow
		topics = AllTopics()
		started = "Alertas de manutencao iniciado"
		ending = "Alertas de manutencao encerrando"
	default:
		component = "PAINEL"
		color = logx.ColorBlue
		topics = TopicsExceptLast()
		started = "Painel industrial iniciado"
		ending = "Painel industrial encerrando"
	}

	log := newLogger(component, color)
	log.Infof(started)

	addr := getBrokerAddr()
	c, err := client.NewClient(addr)
	if err != nil {
		log.Errorf("Falha ao criar client: %v", err)
		return
	}
	defer c.Close()
	log.Infof("Brokers configurados: %s", addr)

	subs, err := subscribeTopics(c, topics)
	if err != nil {
		log.Errorf("Falha ao inscrever topicos: %v", err)
		return
	}
	log.Infof("Inscrito em: %s", strings.Join(topics, ", "))

	startPrintLoops(log, subs)

	stop := interruptChan()
	<-stop
	log.Infof(ending)
}
