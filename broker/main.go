package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	addr := flag.String("addr", ":9000", "broker listen address")
	flag.Parse()

	fmt.Println("Broker iniciando em", *addr)

	b := NewBroker()
	if err := b.Start(*addr); err != nil {
		fmt.Println("Erro ao iniciar broker:", err)
		return
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("Encerrando broker...")
	_ = b.Stop()
}
