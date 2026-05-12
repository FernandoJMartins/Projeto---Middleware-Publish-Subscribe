# Documentacao tecnica - Middleware Publish/Subscribe (Go)

Este documento explica o projeto com foco em bibliotecas de conexao/TCP, fluxo de execucao e codigo completo. Nao ha resumo: todo o codigo relevante esta reimpresso e explicado.

## 1. Biblioteca de conexao e TCP (Go)

### 1.1 Biblioteca usada

O projeto usa a biblioteca padrao `net` do Go para TCP.
- `net.Listen("tcp", addr)` cria o socket do servidor (broker) e faz o bind na porta.
- `Listener.Accept()` bloqueia ate uma conexao TCP ser aceita.
- `net.Dial("tcp", addr)` cria a conexao do lado cliente.
- `net.Conn` representa o socket TCP ativo e fornece `Read`/`Write` (stream).

A serializacao e feita com `encoding/json`:
- `json.NewEncoder(conn)` grava objetos JSON no stream.
- `json.NewDecoder(conn)` le objetos JSON do stream.

Em TCP nao existem mensagens delimitadas. E um fluxo de bytes. O `json.Decoder` sabe ler um objeto JSON completo do fluxo, entao cada frame enviado pelo `Encoder` vira um objeto legivel pelo `Decoder`.

### 1.2 Quando a conexao e criada

No broker:
- A conexao e criada em `Accept()` dentro do loop `acceptLoop`. Isso ocorre quando um cliente conecta.

No cliente:
- A conexao e criada em `net.Dial()` dentro de `getConn`. Isso ocorre quando o cliente precisa enviar um `publish` ou `subscribe` e ainda nao existe conexao para o broker escolhido.

### 1.3 O que acontece por baixo dos panos

1. `net.Listen` abre o socket e registra no sistema operacional.
2. `Accept` bloqueia ate o OS completar o handshake TCP.
3. `net.Dial` inicia o handshake (SYN/SYN-ACK/ACK). Quando retorna, a conexao esta ativa.
4. A partir disso, leitura e escrita sao feitas em um fluxo unico.
5. O projeto usa `json.Encoder/Decoder` para transformar structs em JSON e vice-versa.

Esses passos sao abstraidos pela biblioteca `net`, mas o comportamento observado e exatamente esse fluxo. Por isso, quando o subscriber fecha a conexao, o broker detecta erro no `Decode` e encerra o cliente.

## 2. Codigo completo e explicacao detalhada

Abaixo estao todos os arquivos relevantes do projeto, com o codigo completo e explicacao detalhada.

---

### Arquivo: common/protocol/protocol.go

```go
package protocol

import "encoding/json"

// Frame - Envelope de mensagem entre cliente e broker.
type Frame struct {
	// Type define a acao: subscribe, publish, message, ack, etc.
	Type string `json:"type"`
	// ID correlaciona requisicoes e respostas (ACK).
	ID string `json:"id,omitempty"`
	// Topic identifica o canal logico de mensagens.
	Topic string `json:"topic,omitempty"`
	// Data e o payload JSON bruto.
	Data json.RawMessage `json:"data,omitempty"`
	// Ok e o status no ACK.
	Ok bool `json:"ok,omitempty"`
	// Error detalha o erro quando Ok=false.
	Error string `json:"error,omitempty"`
}
```

Explicacao detalhada:
- `Frame` e o formato padrao para todas as mensagens trocadas entre cliente e broker.
- `Type` define o tipo de operacao: `publish`, `subscribe`, `message`, `ack` e `delivery_ack`.
- `ID` permite correlacao entre requisicao e resposta. Isso e essencial para `ack` e `delivery_ack`.
- `Topic` define o topico logico. O broker usa isso para mapear topicos.
- `Data` guarda o JSON bruto. Assim, cada aplicacao pode escolher o payload.
- `Ok` e `Error` sao usados apenas em `ack` para confirmar sucesso/falha.

---

### Arquivo: common/logx/logx.go

```go
package logx

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// ANSI color codes (simple and portable). Disable with NO_COLOR or LOG_NO_COLOR.
const (
	ColorReset   = "\u001b[0m"
	ColorDim     = "\u001b[2m"
	ColorRed     = "\u001b[31m"
	ColorGreen   = "\u001b[32m"
	ColorYellow  = "\u001b[33m"
	ColorBlue    = "\u001b[34m"
	ColorMagenta = "\u001b[35m"
	ColorCyan    = "\u001b[36m"
	ColorGray    = "\u001b[90m"
)

// Logger is a tiny, colored logger for terminals.
// It keeps output serialized to avoid interleaving in concurrent goroutines.
type Logger struct {
	mu        sync.Mutex
	component string
	color     string
	out       io.Writer
}

// New creates a logger for a component (e.g., BROKER, PUB-1).
func New(component string, color string) *Logger {
	return &Logger{
		component: component,
		color:     color,
		out:       os.Stdout,
	}
}

// SetOutput changes the writer (useful for tests).
func (l *Logger) SetOutput(out io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out = out
}

// Infof prints an informational message.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log("INFO", ColorGreen, format, args...)
}

// Warnf prints a warning message.
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log("WARN", ColorYellow, format, args...)
}

// Errorf prints an error message.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log("ERROR", ColorRed, format, args...)
}

// Debugf prints a debug message (kept simple, always enabled).
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log("DEBUG", ColorGray, format, args...)
}

func (l *Logger) log(level string, levelColor string, format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintf(format, args...)

	l.mu.Lock()
	defer l.mu.Unlock()

	if colorEnabled() {
		fmt.Fprintf(
			l.out,
			"%s%s%s %s[%s]%s %s%s%s | %s\n",
			ColorDim,
			timestamp,
			ColorReset,
			l.color,
			l.component,
			ColorReset,
			levelColor,
			level,
			ColorReset,
			msg,
		)
		return
	}

	fmt.Fprintf(l.out, "%s [%s] %s | %s\n", timestamp, l.component, level, msg)
}

func colorEnabled() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if os.Getenv("LOG_NO_COLOR") != "" {
		return false
	}
	return true
}
```

Explicacao detalhada:
- `Logger` garante logs ordenados entre goroutines usando `Mutex`.
- O timestamp e definido em milissegundos para facilitar debug.
- As cores ajudam a diferenciar componentes (BROKER, PUB, SUB).
- `NO_COLOR` e `LOG_NO_COLOR` desativam cores em ambientes sem suporte ANSI.

---

### Arquivo: broker/protocol.go

```go
package main

import (
	"time"

	"middleware-pubsub/common/protocol"
)

const (
	// topicInboxSize define o buffer de mensagens por topico.
	topicInboxSize = 256
	// clientSendSize define o buffer de envio por cliente.
	clientSendSize = 256
	// deliveryRetryInterval define o intervalo de reenvio de mensagens nao confirmadas.
	deliveryRetryInterval = 2 * time.Second
)

// frame - Alias local do protocolo compartilhado.
type frame = protocol.Frame
```

Explicacao detalhada:
- `topicInboxSize` bufferiza mensagens publicadas em cada topico.
- `clientSendSize` bufferiza mensagens enviadas para um cliente.
- `deliveryRetryInterval` controla o tempo do retry at-least-once.

---

### Arquivo: broker/main.go

```go
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
```

Explicacao detalhada:
- Recebe porta via flag `-addr`.
- Cria o broker e inicia `Start` (abre o socket TCP).
- Espera sinais do OS para encerrar com seguranca.

---

### Arquivo: broker/broker.go

```go
package main

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"middleware-pubsub/common/logx"
)

// brokerLog centraliza os logs do broker com cor consistente.
var brokerLog = logx.New("BROKER", logx.ColorCyan)

// Broker - Coordena topicos, clientes e conexoes.
type Broker struct {
	mu sync.RWMutex // Evita race conditions ao acessar topics e clients.

	// topics guarda os topicos ativos e suas filas de mensagens.
	topics map[string]*topic
	// clients registra conexoes ativas para limpeza coordenada.
	clients map[*clientConn]struct{}

	// listener aceita novas conexoes TCP.
	listener net.Listener
	// quit encerra loops de goroutines quando o broker para.
	quit chan struct{}
	// wg garante que todas as goroutines finalizem no Stop().
	wg sync.WaitGroup
}

// NewBroker - Cria uma nova instancia do broker.
func NewBroker() *Broker {
	return &Broker{
		topics:  make(map[string]*topic),
		clients: make(map[*clientConn]struct{}),
		quit:    make(chan struct{}),
	}
}

// Start - Inicia o broker e aceita conexoes.
func (b *Broker) Start(addr string) error {
	// Abre o socket TCP para aceitar clientes.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.listener = ln
	brokerLog.Infof("Escutando em %s", addr)

	b.wg.Add(1)
	go b.acceptLoop()
	return nil
}

// Stop - Finaliza o broker e encerra as goroutines.
func (b *Broker) Stop() error {
	if b.listener == nil {
		return errors.New("broker not started")
	}
	brokerLog.Infof("Encerrando broker...")
	close(b.quit)
	_ = b.listener.Close()

	b.mu.Lock() // Fecha conexoes dos clientes e sinaliza encerramento dos topicos.
	for c := range b.clients {
		_ = c.conn.Close()
	}
	for _, t := range b.topics {
		close(t.quit)
	}
	b.mu.Unlock() // Aguarda todas as goroutines finalizarem.

	b.wg.Wait() // Garante que todas as goroutines terminem antes de retornar.
	return nil
}

// acceptLoop - Aceita clientes e cria loops de leitura/escrita.
func (b *Broker) acceptLoop() {
	defer b.wg.Done()
	brokerLog.Infof("Loop de aceitacao iniciado")

	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.quit:
				brokerLog.Infof("Loop de aceitacao finalizado")
				return
			default:
				brokerLog.Warnf("Falha ao aceitar conexao: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		remoteAddr := conn.RemoteAddr().String()
		brokerLog.Infof("Cliente conectado: %s", remoteAddr)

		c := &clientConn{
			addr:    remoteAddr,
			conn:    conn,
			enc:     json.NewEncoder(conn),
			dec:     json.NewDecoder(conn),
			send:    make(chan frame, clientSendSize),
			subs:    make(map[string]struct{}),
			quit:    make(chan struct{}),
			pending: make(map[string]frame),
		}

		b.mu.Lock()
		b.clients[c] = struct{}{}
		b.mu.Unlock()

		b.wg.Add(2)
		go b.writerLoop(c)
		go b.readerLoop(c)
		b.wg.Add(1)
		go b.deliveryRetryLoop(c)
	}
}
```

Explicacao detalhada:
- `net.Listen` cria o socket TCP. O broker fica escutando na porta definida.
- `acceptLoop` chama `Accept`, que bloqueia ate uma conexao ser estabelecida.
- Ao aceitar um cliente, ele cria `clientConn` e 3 goroutines: leitura, escrita e retry.
- `Stop` encerra o listener e fecha topicos/clients.

---

### Arquivo: broker/client_conn.go

```go
package main

import (
	"encoding/json"
	"net"
	"sync"
	"time"
)

// clientConn - Conexao ativa com um cliente.
type clientConn struct {
	// addr identifica o cliente nos logs.
	addr string
	// conn e o socket TCP.
	conn net.Conn
	// enc/dec fazem encode/decode JSON linha a linha.
	enc *json.Encoder
	dec *json.Decoder
	// send e o buffer de envio para o cliente (writerLoop).
	send chan frame
	// subs guarda topicos assinados por este cliente.
	subs map[string]struct{}
	// quit sinaliza encerramento do cliente.
	quit chan struct{}
	// pending guarda mensagens enviadas sem confirmacao.
	pending map[string]frame
	// pendingMu protege o mapa pending.
	pendingMu sync.Mutex
	// closeOnce evita fechar canais duas vezes.
	closeOnce sync.Once
}

func (c *clientConn) close() {
	c.closeOnce.Do(func() {
		close(c.quit)
		close(c.send)
		_ = c.conn.Close()
	})
}

// readerLoop - Le frames do cliente e despacha para o broker.
func (b *Broker) readerLoop(c *clientConn) {
	defer b.wg.Done()
	defer b.cleanupClient(c)
	brokerLog.Debugf("Reader iniciado para %s", c.addr)

	for {
		var f frame
		if err := c.dec.Decode(&f); err != nil {
			brokerLog.Infof("Cliente %s desconectou (reader): %v", c.addr, err)
			return
		}

		switch f.Type {
		case "subscribe":
			b.handleSubscribe(c, f)
		case "publish":
			b.handlePublish(c, f)
		case "unsubscribe":
			b.handleUnsubscribe(c, f)
		case "delivery_ack":
			b.handleDeliveryAck(c, f)
		default:
			brokerLog.Warnf("Frame desconhecido de %s: type=%s", c.addr, f.Type)
			_ = b.sendAck(c, f.ID, false, "unknown frame type")
		}
	}
}

// writerLoop - Envia frames para o cliente.
func (b *Broker) writerLoop(c *clientConn) {
	defer b.wg.Done()
	brokerLog.Debugf("Writer iniciado para %s", c.addr)

	for {
		select {
		case f, ok := <-c.send:
			if !ok {
				return
			}
			if err := c.enc.Encode(&f); err != nil {
				brokerLog.Infof("Falha ao enviar para %s: %v", c.addr, err)
				return
			}
		case <-c.quit:
			return
		}
	}
}

// cleanupClient - Remove o cliente de todos os topicos e encerra a conexao.
func (b *Broker) cleanupClient(c *clientConn) {
	brokerLog.Infof("Encerrando cliente %s", c.addr)
	b.mu.Lock()
	for topicName := range c.subs {
		t := b.topics[topicName]
		if t == nil {
			continue
		}
		delete(t.subs, c)
		if len(t.subs) == 0 {
			close(t.quit)
			delete(b.topics, topicName)
			brokerLog.Infof("Topico removido (sem inscritos): %s", topicName)
		}
	}
	delete(b.clients, c)
	b.mu.Unlock()

	c.pendingMu.Lock()
	for key := range c.pending {
		delete(c.pending, key)
	}
	c.pendingMu.Unlock()

	c.close()
}

// abaixo e tudo necessario para o mecanismo de at-least-once delivery. Primordial para o Sistema.
func deliveryKey(topic, id string) string {
	return topic + "|" + id
}

// enqueueDelivery registra e envia mensagem para o cliente com at-least-once.
func (b *Broker) enqueueDelivery(c *clientConn, msg frame) {
	if msg.ID == "" || msg.Topic == "" {
		return
	}

	key := deliveryKey(msg.Topic, msg.ID)
	c.pendingMu.Lock()
	if _, ok := c.pending[key]; !ok {
		c.pending[key] = msg
	}
	c.pendingMu.Unlock()

	b.trySend(c, msg)
}

// handleDeliveryAck remove a mensagem confirmada pelo subscriber.
func (b *Broker) handleDeliveryAck(c *clientConn, f frame) {
	if f.ID == "" || f.Topic == "" {
		return
	}

	key := deliveryKey(f.Topic, f.ID)
	c.pendingMu.Lock()
	delete(c.pending, key)
	c.pendingMu.Unlock()
}

// deliveryRetryLoop reenvia mensagens pendentes periodicamente.
func (b *Broker) deliveryRetryLoop(c *clientConn) {
	defer b.wg.Done()
	ticker := time.NewTicker(deliveryRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.retryPending(c)
		case <-c.quit:
			return
		}
	}
}

func (b *Broker) retryPending(c *clientConn) {
	items := make([]frame, 0)
	c.pendingMu.Lock()
	for _, msg := range c.pending {
		items = append(items, msg)
	}
	c.pendingMu.Unlock()

	for _, msg := range items {
		b.trySend(c, msg)
	}
}

func (b *Broker) trySend(c *clientConn, msg frame) {
	select {
	case <-c.quit:
		return
	default:
	}

	select {
	case c.send <- msg:
	default:
		// buffer cheio; sera reenviado pelo retry loop
	}
}
```

Explicacao detalhada:
- `readerLoop` faz decode de JSON e despacha operacoes para handlers.
- `writerLoop` envia frames pelo `json.Encoder` (stream TCP).
- `pending` guarda mensagens pendentes de `delivery_ack` (at-least-once).
- `deliveryRetryLoop` reenvia periodicamente mensagens nao confirmadas.

---

### Arquivo: broker/handlers.go

```go
package main

import (
	"fmt"
	"time"
)

// handleSubscribe - Registra o cliente no topico.
func (b *Broker) handleSubscribe(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	// Cria topico sob demanda.

	b.mu.Lock()
	t := b.topics[f.Topic]
	if t == nil {
		t = &topic{
			name:  f.Topic,
			inbox: make(chan frame, topicInboxSize),
			quit:  make(chan struct{}),
			subs:  make(map[*clientConn]struct{}),
		}
		b.topics[f.Topic] = t
		b.wg.Add(1)
		go b.topicLoop(t)
		brokerLog.Infof("Topico criado: %s", f.Topic)
	}
	t.subs[c] = struct{}{}
	c.subs[f.Topic] = struct{}{}
	b.mu.Unlock()

	brokerLog.Infof("Cliente %s inscrito em %s", c.addr, f.Topic)

	_ = b.sendAck(c, f.ID, true, "")
}

// handlePublish - Publica mensagem em um topico.
func (b *Broker) handlePublish(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	// Se nao houver topico, nao existem inscritos.

	b.mu.RLock()
	t := b.topics[f.Topic]
	b.mu.RUnlock()
	if t == nil {
		brokerLog.Warnf("Publish descartado (sem inscritos): topic=%s client=%s", f.Topic, c.addr)
		_ = b.sendAck(c, f.ID, false, "no_subscribers")
		return
	}

	msg := frame{
		Type:  "message",
		ID:    f.ID,
		Topic: f.Topic,
		Data:  f.Data,
	}
	if msg.ID == "" {
		msg.ID = fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	t.inbox <- msg

	brokerLog.Debugf("Publish aceito: topic=%s client=%s", f.Topic, c.addr)

	_ = b.sendAck(c, f.ID, true, "")
}

// handleUnsubscribe - Remove o cliente do topico.
func (b *Broker) handleUnsubscribe(c *clientConn, f frame) {
	if f.Topic == "" {
		_ = b.sendAck(c, f.ID, false, "topic is required")
		return
	}

	b.mu.Lock()
	t := b.topics[f.Topic]
	if t == nil {
		b.mu.Unlock()
		_ = b.sendAck(c, f.ID, false, "topic not found")
		return
	}

	delete(t.subs, c)
	delete(c.subs, f.Topic)
	if len(t.subs) == 0 {
		delete(b.topics, f.Topic)
		close(t.quit)
		brokerLog.Infof("Topico removido (sem inscritos): %s", f.Topic)
	}
	b.mu.Unlock()

	brokerLog.Infof("Cliente %s saiu de %s", c.addr, f.Topic)

	_ = b.sendAck(c, f.ID, true, "")
}

// sendAck - Responde com ACK/erro para o cliente.
func (b *Broker) sendAck(c *clientConn, id string, ok bool, errMsg string) error {
	ack := frame{
		Type:  "ack",
		ID:    id,
		Ok:    ok,
		Error: errMsg,
	}

	// Envia o ACK pelo canal de envio (writerLoop).
	c.send <- ack
	return nil
}
```

Explicacao detalhada:
- `handleSubscribe` cria o topico se necessario e registra o cliente.
- `handlePublish` descarta se nao houver inscritos e gera `ack`.
- `handlePublish` garante `ID` para permitir at-least-once.
- `handleUnsubscribe` remove cliente e encerra topico se vazio.

---

### Arquivo: broker/topic.go

```go
package main

// topic - Estado e fila de mensagens de um topico.
type topic struct {
	// name e o identificador do topico.
	name string
	// inbox bufferiza mensagens publicadas.
	inbox chan frame
	// quit encerra o loop do topico.
	quit chan struct{}
	// subs guarda os clientes inscritos.
	subs map[*clientConn]struct{}
}

// topicLoop - Encaminha mensagens do topico para os inscritos.
func (b *Broker) topicLoop(t *topic) {
	defer b.wg.Done()
	brokerLog.Infof("Loop do topico iniciado: %s", t.name)
	for {
		select {
		case msg := <-t.inbox:
			// Copia a lista de inscritos para evitar segurar o lock durante o envio.
			b.mu.RLock()
			subs := make([]*clientConn, 0, len(t.subs))
			for c := range t.subs {
				subs = append(subs, c)
			}
			b.mu.RUnlock()

			brokerLog.Debugf("Fanout topic=%s subs=%d", t.name, len(subs))

			for _, c := range subs {
				b.enqueueDelivery(c, msg)
			}
		case <-t.quit:
			brokerLog.Infof("Loop do topico finalizado: %s", t.name)
			return
		}
	}
}
```

Explicacao detalhada:
- `topicLoop` roda em goroutine por topico.
- O `inbox` desacopla publish e fanout.
- Cada mensagem e entregue via `enqueueDelivery` para garantir retry.

---

### Arquivo: client/protocol.go

```go
package client

import (
	"middleware-pubsub/common/protocol"
	"time"
)

const (
	// brokerSendSize define o buffer de envio por conexao.
	brokerSendSize = 256
	// subBufferSize define o buffer por topico no cliente.
	subBufferSize = 64
	// ackTimeout e o tempo maximo de espera por ACK.
	ackTimeout = 5 * time.Second
)

// frame - Alias local do protocolo compartilhado.
type frame = protocol.Frame
```

Explicacao detalhada:
- `brokerSendSize` e `subBufferSize` controlam buffering no cliente.
- `ackTimeout` define timeout de ACK para publish/subscribe.

---

### Arquivo: client/util.go

```go
package client

import (
	"fmt"
	"hash/fnv"
	"sync/atomic"
)

func (c *Client) nextID() string {
	// Usa contador atomico para gerar IDs unicos e ordenados.
	n := atomic.AddUint64(&c.idCounter, 1)
	return fmt.Sprintf("req-%d", n)
}

func (c *Client) pickBroker(topic string) string {
	order := c.brokerOrder(topic)
	if len(order) == 0 {
		return ""
	}
	return order[0]
}

// brokerOrder retorna a lista de brokers em ordem de preferencia para um topico.
func (c *Client) brokerOrder(topic string) []string {
	if len(c.brokers) == 0 {
		return nil
	}
	if len(c.brokers) == 1 {
		return []string{c.brokers[0]}
	}

	start := 0
	if topic != "" {
		// Hash consistente do topico para manter afinidade.
		h := fnv.New32a()
		_, _ = h.Write([]byte(topic))
		start = int(h.Sum32()) % len(c.brokers)
	}

	order := make([]string, 0, len(c.brokers))
	for i := 0; i < len(c.brokers); i++ {
		idx := (start + i) % len(c.brokers)
		order = append(order, c.brokers[idx])
	}
	return order
}

// preferredBroker retorna o broker principal para um topico.
func (c *Client) preferredBroker(topic string) string {
	order := c.brokerOrder(topic)
	if len(order) == 0 {
		return ""
	}
	return order[0]
}
```

Explicacao detalhada:
- `nextID` gera IDs unicos para correlacao de ACK.
- `brokerOrder` calcula o broker principal via hash e lista os outros como fallback.

---

### Arquivo: client/conn.go

```go
package client

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
)

var (
	errBrokerClosed = errors.New("broker connection closed")
	errAckTimeout   = errors.New("ack timeout")
)

const publishRetryDelay = 300 * time.Millisecond

type brokerConn struct {
	// addr identifica o broker (host:port).
	addr string
	// conn e o socket TCP aberto.
	conn net.Conn
	// enc/dec fazem encode/decode JSON.
	enc *json.Encoder
	dec *json.Decoder
	// send e o buffer de envio para o broker.
	send chan frame
	// quit sinaliza encerramento da conexao.
	quit chan struct{}
	// closeOnce evita fechamento duplo.
	closeOnce sync.Once
	// wg espera reader/writer finalizarem.
	wg sync.WaitGroup
}

func (bc *brokerConn) close() {
	// Fecha uma unica vez para evitar panics.
	bc.closeOnce.Do(func() {
		close(bc.quit)
		close(bc.send)
		_ = bc.conn.Close()
	})
}

func (c *Client) connForTopic(topic string) (*brokerConn, error) {
	// Resolve broker por hash do topico e faz failover se necessario.
	order := c.brokerOrder(topic)
	if len(order) == 0 {
		return nil, errors.New("broker address is required")
	}

	var lastErr error
	for _, addr := range order {
		bc, err := c.getConn(addr)
		if err == nil {
			return bc, nil
		}
		lastErr = err
	}

	if lastErr == nil {
		lastErr = errors.New("no brokers available")
	}
	return nil, lastErr
}

func (c *Client) getConn(addr string) (*brokerConn, error) {
	c.mu.Lock()
	// Nao abre novas conexoes quando fechado.
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("client is closed")
	}
	// Reaproveita conexao existente.
	if bc, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		return bc, nil
	}
	c.mu.Unlock()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	bc := &brokerConn{
		addr: addr,
		conn: conn,
		enc:  json.NewEncoder(conn),
		dec:  json.NewDecoder(conn),
		send: make(chan frame, brokerSendSize),
		quit: make(chan struct{}),
	}

	bc.wg.Add(2)
	// Inicia loop de escrita e leitura em paralelo.
	go c.writerLoop(bc)
	go c.readerLoop(bc)

	c.mu.Lock()
	// Se o cliente foi fechado durante a conexao, encerra.
	if c.closed {
		c.mu.Unlock()
		bc.close()
		bc.wg.Wait()
		return nil, errors.New("client is closed")
	}
	// Caso outra goroutine ja tenha aberto conexao, reusa.
	if existing, ok := c.conns[addr]; ok {
		c.mu.Unlock()
		bc.close()
		bc.wg.Wait()
		return existing, nil
	}
	c.conns[addr] = bc
	c.mu.Unlock()

	return bc, nil
}

func (c *Client) writerLoop(bc *brokerConn) {
	defer bc.wg.Done()
	// Consome o canal de envio e escreve no socket.
	for {
		select {
		case f, ok := <-bc.send:
			if !ok {
				return
			}
			_ = bc.enc.Encode(&f)
		case <-bc.quit:
			return
		}
	}
}

func (c *Client) readerLoop(bc *brokerConn) {
	defer bc.wg.Done()
	defer func() {
		bc.close()
		c.removeConn(bc.addr, bc)
	}()

	// Le frames do broker e despacha para handlers locais.

	for {
		var f frame
		if err := bc.dec.Decode(&f); err != nil {
			return
		}
		switch f.Type {
		case "message":
			c.dispatchMessage(bc, f)
		case "ack":
			c.dispatchAck(f)
		default:
			// Ignora frames desconhecidos
		}
	}
}

func (c *Client) dispatchMessage(bc *brokerConn, f frame) {
	if f.Topic == "" {
		return
	}

	c.mu.Lock()
	ch := c.subs[f.Topic]
	c.mu.Unlock()
	if ch == nil {
		return
	}

	msg := Message{Topic: f.Topic, Data: f.Data}
	// Entrega bloqueante garante at-least-once para o consumidor.
	ch <- msg

	if f.ID == "" {
		return
	}

	ack := frame{
		Type:  "delivery_ack",
		ID:    f.ID,
		Topic: f.Topic,
	}

	select {
	case bc.send <- ack:
	case <-bc.quit:
	}
}

func (c *Client) dispatchAck(f frame) {
	if f.ID == "" {
		return
	}

	c.mu.Lock()
	ch := c.pending[f.ID]
	if ch != nil {
		delete(c.pending, f.ID)
	}
	c.mu.Unlock()

	if ch != nil {
		// Entrega o ACK para quem esta aguardando.
		ch <- f
		close(ch)
	}
}

func (c *Client) sendAndWait(bc *brokerConn, f frame) error {
	if f.ID == "" {
		// Gera ID se nao fornecido.
		f.ID = c.nextID()
	}

	for {
		err := c.sendOnceAndWait(bc, f)
		if err == nil {
			return nil
		}
		if err != errAckTimeout && err != errBrokerClosed {
			return err
		}

		// Tenta outro broker (ou o mesmo) apos uma pausa curta.
		time.Sleep(publishRetryDelay)
		next, nextErr := c.connForTopic(f.Topic)
		if nextErr != nil {
			return err
		}
		bc = next
	}
}

func (c *Client) sendOnceAndWait(bc *brokerConn, f frame) error {

	ch := make(chan frame, 1)
	c.mu.Lock()
	// Nao aceita novas operacoes se fechado.
	if c.closed {
		c.mu.Unlock()
		return errors.New("client is closed")
	}
	// Registra o canal de resposta do ACK.
	c.pending[f.ID] = ch
	c.mu.Unlock()

	select {
	case bc.send <- f:
	case <-bc.quit:
		// Conexao caiu antes de enviar.
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return errBrokerClosed
	}

	select {
	case ack := <-ch:
		if !ack.Ok {
			if ack.Error == "" {
				return errors.New("request failed")
			}
			return errors.New(ack.Error)
		}
		return nil
	case <-time.After(ackTimeout):
		// Timeout evita bloqueio infinito.
		c.mu.Lock()
		delete(c.pending, f.ID)
		c.mu.Unlock()
		return errAckTimeout
	}
}

func (c *Client) removeConn(addr string, bc *brokerConn) {
	c.mu.Lock()
	if cur, ok := c.conns[addr]; ok && cur == bc {
		delete(c.conns, addr)
	}
	c.mu.Unlock()

	// Tenta recuperar assinaturas ligadas ao broker que caiu.
	go c.recoverSubscriptions(addr)
}
```

Explicacao detalhada:
- `net.Dial` cria a conexao TCP do cliente.
- `writerLoop` envia frames via `json.Encoder`.
- `readerLoop` le frames via `json.Decoder` e trata `message` e `ack`.
- `delivery_ack` confirma entrega no subscriber.
- `sendAndWait` implementa retry para publish/subscribe.

---

### Arquivo: client/client.go

```go
package client

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

const rebalanceInterval = 5 * time.Second

// Message - Estrutura de mensagem
type Message struct {
	// Topic e o topico ao qual a mensagem pertence.
	Topic string `json:"topic"`
	// Data e o payload JSON bruto (a aplicacao deve interpretar os campos).
	Data json.RawMessage `json:"data"`
}

// Client - Cliente pub/sub
type Client struct {
	mu sync.Mutex

	// brokers e a lista de enderecos conhecidos (host:port).
	brokers []string
	// conns cacheia conexoes TCP abertas por endereco.
	conns map[string]*brokerConn
	// subs guarda canais de entrega por topico.
	subs map[string]chan Message
	// subBrokers mapeia topico -> broker atual (para failover e rebalanceamento).
	subBrokers map[string]string
	// pending correlaciona requisicoes com respostas ACK.
	pending map[string]chan frame

	// rebalanceQuit encerra o loop de rebalanceamento.
	rebalanceQuit chan struct{}

	// idCounter gera IDs unicos para as requisicoes.
	idCounter uint64
	// closed marca o cliente como encerrado.
	closed bool
}

// NewClient - Cria um novo cliente
func NewClient(brokerAddr string) (*Client, error) {
	// Aceita lista separada por virgula: "host1:9000,host2:9001".
	parts := strings.Split(brokerAddr, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		addr := strings.TrimSpace(p)
		if addr != "" {
			brokers = append(brokers, addr)
		}
	}
	if len(brokers) == 0 {
		return nil, errors.New("broker address is required")
	}

	c := &Client{
		brokers:       brokers,
		conns:         make(map[string]*brokerConn),
		subs:          make(map[string]chan Message),
		subBrokers:    make(map[string]string),
		pending:       make(map[string]chan frame),
		rebalanceQuit: make(chan struct{}),
	}

	// Loop de rebalanceamento tenta voltar topicos ao broker preferido.
	go c.rebalanceLoop()

	return c, nil
}

// Publish - Publica uma mensagem
func (c *Client) Publish(topic string, data interface{}) error {
	// Publicacao exige topico valido.
	if topic == "" {
		return errors.New("topic is required")
	}
	// Serializa o payload para JSON antes de enviar.
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	// Seleciona o broker pela regra de balanceamento.
	bc, err := c.connForTopic(topic)
	if err != nil {
		return err
	}

	f := frame{
		Type:  "publish",
		Topic: topic,
		Data:  payload,
	}
	return c.sendAndWait(bc, f)
}

// Subscribe - Se inscreve em um topico
func (c *Client) Subscribe(topic string) (<-chan Message, error) {
	// Inscricao exige topico valido.
	if topic == "" {
		return nil, errors.New("topic is required")
	}

	bc, err := c.connForTopic(topic)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	// Garante que o cliente ainda esta ativo.
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("client is closed")
	}
	// Reaproveita canal se ja estiver inscrito.
	if ch, ok := c.subs[topic]; ok {
		c.mu.Unlock()
		return ch, nil
	}
	// Cria canal bufferizado para este topico.
	ch := make(chan Message, subBufferSize)
	c.subs[topic] = ch
	c.mu.Unlock()

	f := frame{
		Type:  "subscribe",
		Topic: topic,
	}
	if err := c.sendAndWait(bc, f); err != nil {
		// Em falha, remove o canal criado e devolve erro.
		c.mu.Lock()
		delete(c.subs, topic)
		c.mu.Unlock()
		close(ch)
		return nil, err
	}

	// Registra o broker atual da assinatura.
	c.mu.Lock()
	c.subBrokers[topic] = bc.addr
	c.mu.Unlock()

	return ch, nil
}

// Unsubscribe - Remove inscricao de um topico
func (c *Client) Unsubscribe(topic string) error {
	// Remove a inscricao, caso exista.
	if topic == "" {
		return errors.New("topic is required")
	}

	bc, err := c.connForTopic(topic)
	if err != nil {
		return err
	}

	f := frame{
		Type:  "unsubscribe",
		Topic: topic,
	}
	if err := c.sendAndWait(bc, f); err != nil {
		return err
	}

	c.mu.Lock()
	ch := c.subs[topic]
	delete(c.subs, topic)
	delete(c.subBrokers, topic)
	c.mu.Unlock()
	if ch != nil {
		// Fecha o canal para avisar o consumidor.
		close(ch)
	}
	return nil
}

// Close - Fecha a conexao
func (c *Client) Close() error {
	c.mu.Lock()
	// Fecha apenas uma vez.
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.rebalanceQuit)

	for _, ch := range c.subs {
		// Fecha canais de assinaturas ativas.
		close(ch)
	}
	for _, ch := range c.pending {
		// Fecha aguardas pendentes para nao vazar goroutines.
		close(ch)
	}
	c.subs = make(map[string]chan Message)
	c.subBrokers = make(map[string]string)
	c.pending = make(map[string]chan frame)

	conns := make([]*brokerConn, 0, len(c.conns))
	for _, bc := range c.conns {
		conns = append(conns, bc)
	}
	c.conns = make(map[string]*brokerConn)
	c.mu.Unlock()

	for _, bc := range conns {
		bc.close()
		bc.wg.Wait()
	}
	return nil
}

// recoverSubscriptions tenta reabrir assinaturas ligadas ao broker que caiu.
func (c *Client) recoverSubscriptions(badAddr string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	topics := make([]string, 0, len(c.subBrokers))
	for topic, addr := range c.subBrokers {
		if addr == badAddr {
			topics = append(topics, topic)
		}
	}
	c.mu.Unlock()

	for _, topic := range topics {
		bc, err := c.connForTopic(topic)
		if err != nil {
			continue
		}
		f := frame{Type: "subscribe", Topic: topic}
		if err := c.sendAndWait(bc, f); err != nil {
			continue
		}
		c.mu.Lock()
		if !c.closed {
			c.subBrokers[topic] = bc.addr
		}
		c.mu.Unlock()
	}
}

// rebalanceLoop tenta trazer topicos de volta ao broker preferido quando ele volta.
func (c *Client) rebalanceLoop() {
	ticker := time.NewTicker(rebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.rebalanceSubscriptions()
		case <-c.rebalanceQuit:
			return
		}
	}
}

// rebalanceSubscriptions move topicos para o broker preferido se ele estiver disponivel.
func (c *Client) rebalanceSubscriptions() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	topics := make([]string, 0, len(c.subs))
	current := make(map[string]string, len(c.subBrokers))
	for topic := range c.subs {
		current[topic] = c.subBrokers[topic]
		topics = append(topics, topic)
	}
	c.mu.Unlock()

	for _, topic := range topics {
		preferred := c.preferredBroker(topic)
		if preferred == "" || current[topic] == preferred {
			continue
		}

		bc, err := c.getConn(preferred)
		if err != nil {
			continue
		}

		f := frame{Type: "subscribe", Topic: topic}
		if err := c.sendAndWait(bc, f); err != nil {
			continue
		}

		c.mu.Lock()
		if !c.closed {
			c.subBrokers[topic] = preferred
		}
		c.mu.Unlock()

		// Se o broker antigo ainda existir, remove a assinatura antiga.
		old := current[topic]
		if old != "" && old != preferred {
			if oldConn, err := c.getConn(old); err == nil {
				_ = c.sendAndWait(oldConn, frame{Type: "unsubscribe", Topic: topic})
			}
		}
	}
}
```

Explicacao detalhada:
- `NewClient` inicializa estruturas e inicia o loop de rebalanceamento.
- `Publish` e `Subscribe` chamam `connForTopic`, que resolve broker e abre conexao.
- `recoverSubscriptions` reabre assinaturas se o broker cair.

---

### Arquivo: examples/helpers.go

```go
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
```

Explicacao detalhada:
- Centraliza topicos do cenario IoT.
- Carrega `.env` para definir brokers.
- `subscribeTopics` e `startPrintLoops` reduzem codigo repetitivo.

---

### Arquivo: examples/publisher.go

```go
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
```

Explicacao detalhada:
- Cada topico tem um ticker com intervalo proprio.
- Cada goroutine produz mensagens para um topico.
- `Publish` usa a biblioteca cliente para escolher broker e enviar.

---

### Arquivo: examples/subscriber.go

```go
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
```

Explicacao detalhada:
- `role` define os topicos a assinar.
- O subscriber cria conexao e chama `Subscribe` para cada topico.
- Mensagens sao impressas via `printLoop`.

---

### Arquivo: examples/main.go

```go
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
```

Explicacao detalhada:
- Define os parametros `-mode` e `-role`.
- Direciona para o publisher ou subscriber.

---

## 3. Como a conexao TCP aparece no fluxo

1. Publisher chama `client.NewClient` e depois `Publish`.
2. `Publish` chama `connForTopic`, que chama `getConn`.
3. `getConn` usa `net.Dial` para abrir o TCP.
4. Broker ja esta em `net.Listen` e `Accept` cria o `net.Conn` do lado servidor.
5. Ambas as pontas usam `json.Encoder/Decoder` para enviar/receber frames.

Esse fluxo garante que a conexao e criada sob demanda, e reutilizada quando ja existe.

---

## 4. Como o retry e o ack funcionam

- O broker envia mensagens `message` para o subscriber.
- O subscriber responde com `delivery_ack`.
- Se nao receber `delivery_ack`, o broker reenvia periodicamente.
- O publisher tambem usa ACK do broker e reenfileira em caso de timeout.

Todo esse fluxo esta implementado nas funcoes exibidas nos arquivos acima.
