package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"middleware-pubsub/common/logx"
)

func main() {
	// Flag simples para selecionar a porta do broker.
	addr := flag.String("addr", ":9000", "broker listen address")
	flag.Parse()

	log := logx.New("BROKER", logx.ColorCyan)
	log.Infof("Broker iniciando em %s", *addr)

	b := NewBroker()
	if err := b.Start(*addr); err != nil {
		log.Errorf("Erro ao iniciar broker: %v", err)
		return
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Infof("Encerrando broker...")
	_ = b.Stop()
}
