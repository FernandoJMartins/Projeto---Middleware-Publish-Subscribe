package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	mode := flag.String("mode", "publisher1", "publisher1|publisher2|subscriber1|subscriber2")
	flag.Parse()

	switch *mode {
	case "publisher1":
		runPublisher1()
	case "publisher2":
		runPublisher2()
	case "subscriber1":
		runSubscriber1()
	case "subscriber2":
		runSubscriber2()
	default:
		fmt.Println("Modo invalido:", *mode)
		fmt.Println("Use: -mode publisher1|publisher2|subscriber1|subscriber2")
		os.Exit(1)
	}
}
