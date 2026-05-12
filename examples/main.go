package main

import (
	"flag"
	"os"

	"middleware-pubsub/common/logx"
)

func main() {
	// Seleciona qual app de exemplo executar.
	mode := flag.String("mode", "publisher", "publisher|subscriber")
	role := flag.String("role", "painel", "painel|alertas")
	flag.Parse()

	switch *mode {
	case "publisher":
		runPublisher()
	case "subscriber":
		runSubscriber(*role)
	default:
		log := newLogger("EXAMPLES", logx.ColorGray)
		log.Warnf("Modo invalido: %s", *mode)
		log.Infof("Use: -mode publisher|subscriber -role painel|alertas")
		os.Exit(1)
	}
}
