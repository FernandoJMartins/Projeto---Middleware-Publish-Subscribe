package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"middleware-pubsub/client"
	"middleware-pubsub/common/logx"
)

var envOnce sync.Once

// Topicos do cenario IoT Industrial.
const (
	TopicTemperatura = "temperatura_maquina"
	TopicPressao     = "pressao"
	TopicFalhaMotor  = "falha_motor"
	TopicConsumo     = "consumo_energia"
)

var allTopics = []string{
	TopicTemperatura,
	TopicPressao,
	TopicFalhaMotor,
	TopicConsumo,
}

// AllTopics retorna todos os topicos do cenario.
func AllTopics() []string {
	return append([]string{}, allTopics...)
}

// TopicsExceptLast retorna todos os topicos, exceto o ultimo.
func TopicsExceptLast() []string {
	if len(allTopics) == 0 {
		return nil
	}
	return append([]string{}, allTopics[:len(allTopics)-1]...)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// loadDotEnv carrega variaveis simples do arquivo .env (KEY=VALUE).
// Comentarios comecam com # e linhas vazias sao ignoradas.
func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, "\"'")

		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
}

// ensureEnvLoaded garante que o .env seja lido apenas uma vez.
func ensureEnvLoaded() {
	envOnce.Do(func() {
		loadDotEnv(".env")
	})
}

func defaultBrokerAddr() string {
	addrs := make([]string, 0, 6)
	for port := 9000; port <= 9001; port++ {
		addrs = append(addrs, fmt.Sprintf("localhost:%d", port))
	}
	return strings.Join(addrs, ",")
}

func getBrokerAddr() string {
	// Carrega .env antes de buscar BROKER_ADDR.
	ensureEnvLoaded()
	return getEnv("BROKER_ADDR", defaultBrokerAddr())
}

func interruptChan() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	return ch
}

func newRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

// newLogger cria um logger colorido para as apps de exemplo.
func newLogger(component string, color string) *logx.Logger {
	return logx.New(component, color)
}

// formatPayload tenta exibir o JSON em uma linha compacta.
func formatPayload(value interface{}) string {
	switch v := value.(type) {
	case []byte:
		return formatJSONLine(v)
	case json.RawMessage:
		return formatJSONLine(v)
	default:
		payload, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return formatJSONLine(payload)
	}
}

func formatJSONLine(raw []byte) string {
	if len(raw) == 0 {
		return "{}"
	}

	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return string(raw)
	}
	return buf.String()
}

// printLoop consome mensagens e imprime de forma padronizada.
func printLoop(log *logx.Logger, topic string, ch <-chan client.Message) {
	for msg := range ch {
		payload := formatPayload(msg.Data)
		log.Infof("RECV topic=%s payload=%s", topic, payload)
	}
}

// subscribeTopics inscreve o cliente em uma lista de topicos.
func subscribeTopics(c *client.Client, topics []string) (map[string]<-chan client.Message, error) {
	subs := make(map[string]<-chan client.Message, len(topics))
	for _, topic := range topics {
		ch, err := c.Subscribe(topic)
		if err != nil {
			return nil, fmt.Errorf("inscricao %s: %w", topic, err)
		}
		subs[topic] = ch
	}
	return subs, nil
}

// startPrintLoops inicia o consumo de mensagens para cada topico.
func startPrintLoops(log *logx.Logger, subs map[string]<-chan client.Message) {
	for topic, ch := range subs {
		go printLoop(log, topic, ch)
	}
}

func pickAction(r *rand.Rand) string {
	actions := []string{"novo", "atualizacao", "cancelamento"}
	return actions[r.Intn(len(actions))]
}

func pickLevel(r *rand.Rand) string {
	levels := []string{"info", "warning", "critical"}
	return levels[r.Intn(len(levels))]
}
