# Documentacao tecnica - Middleware Publish/Subscribe (Go)

Este documento descreve a arquitetura, o protocolo e o funcionamento do broker, clientes e exemplos. Trechos de codigo sao apresentados com arquivo e linha.

## Sumario

- 1. Visao geral
- 2. Protocolo de comunicacao
- 3. Broker
    - 3.1 Inicio e aceitacao de conexoes
    - 3.2 Criacao e remocao de topicos
    - 3.3 Publicacao e fanout
    - 3.4 At-least-once no broker
- 4. Biblioteca cliente
    - 4.1 Balanceamento, failover e rebalanceamento
    - 4.2 Publish com retry e ACK
    - 4.3 Subscribe e delivery_ack
- 5. Aplicacoes de exemplo
    - 5.1 Topicos e configuracao
    - 5.2 Publisher (IoT Industrial)
    - 5.3 Subscriber unico com role
    - 5.4 CLI dos exemplos
- 6. Logs e observabilidade
- 7. Concorrencia (goroutines)
- 8. Transparencia, balanceamento e retry

## 1. Visao geral

O projeto implementa um middleware Pub/Sub com:
- Broker TCP que aceita conexoes, cria topicos sob demanda, encaminha mensagens e garante at-least-once.
- Biblioteca cliente que faz publish/subscribe, balanceamento por hash, failover e rebalanceamento.
- Aplicacoes de exemplo (publisher e subscriber) para o cenario IoT Industrial.

## 2. Protocolo de comunicacao

Arquivo: common/protocol/protocol.go (linhas 5-18)

```go
type Frame struct {
    Type  string          `json:"type"`
    ID    string          `json:"id,omitempty"`
    Topic string          `json:"topic,omitempty"`
    Data  json.RawMessage `json:"data,omitempty"`
    Ok    bool            `json:"ok,omitempty"`
    Error string          `json:"error,omitempty"`
}
```

Tipos relevantes:
- `subscribe`, `unsubscribe`, `publish`, `message`, `ack`, `delivery_ack`.
- `ack` confirma publish/subscribe no broker.
- `delivery_ack` confirma entrega no subscriber (usado no at-least-once).

## 3. Broker

### 3.1 Inicio e aceitacao de conexoes

Arquivo: broker/main.go (linhas 12-31)

```go
addr := flag.String("addr", ":9000", "broker listen address")
...
b := NewBroker()
if err := b.Start(*addr); err != nil { ... }
```

Arquivo: broker/broker.go (linhas 42-120)

```go
func (b *Broker) Start(addr string) error { ... go b.acceptLoop() }
...
func (b *Broker) acceptLoop() { ... go b.writerLoop(c); go b.readerLoop(c); go b.deliveryRetryLoop(c) }
```

Arquivo: broker/broker.go (linhas 79-118)

```go
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
...
go b.writerLoop(c)
go b.readerLoop(c)
go b.deliveryRetryLoop(c)
```

Detalhes:
- `acceptLoop` aceita conexoes TCP e cria 3 goroutines por cliente: reader, writer e retry.
- O `WaitGroup` garante encerramento limpo no Stop.

### 3.2 Criacao e remocao de topicos

Arquivo: broker/handlers.go (linhas 8-38 e 74-101)

```go
if t == nil { ... go b.topicLoop(t) }
...
if len(t.subs) == 0 { delete(b.topics, f.Topic); close(t.quit) }
```

Detalhes:
- Topicos sao criados sob demanda quando o primeiro subscriber chega.
- Topicos sao removidos quando nao existem mais inscritos.

Arquivo: broker/protocol.go (linhas 6-12)

```go
const (
    topicInboxSize = 256
    clientSendSize = 256
)
```

### 3.3 Publicacao e fanout

Arquivo: broker/handlers.go (linhas 40-72)

```go
if t == nil { ... sendAck(..., "no_subscribers") }
msg := frame{ Type: "message", ID: f.ID, Topic: f.Topic, Data: f.Data }
if msg.ID == "" { msg.ID = fmt.Sprintf("msg-%d", time.Now().UnixNano()) }
t.inbox <- msg
```

Arquivo: broker/topic.go (linhas 15-38)

```go
case msg := <-t.inbox:
    ...
    for _, c := range subs { b.enqueueDelivery(c, msg) }
```

Detalhes:
- O broker descarta publish sem inscritos e retorna `ack` com `ok=false`.
- Mensagens vao para o `inbox` do topico (buffer) e sao encaminhadas por `topicLoop`.

Arquivo: broker/handlers.go (linhas 106-121)

```go
ack := frame{ Type: "ack", ID: id, Ok: ok, Error: errMsg }
c.send <- ack
```

### 3.4 At-least-once no broker

Arquivo: broker/client_conn.go (linhas 119-193)

```go
func (b *Broker) enqueueDelivery(c *clientConn, msg frame) { ... c.pending[key] = msg }
func (b *Broker) handleDeliveryAck(c *clientConn, f frame) { delete(c.pending, key) }
func (b *Broker) deliveryRetryLoop(c *clientConn) { ... b.retryPending(c) }
```

Arquivo: broker/protocol.go (linhas 9-15)

```go
const deliveryRetryInterval = 2 * time.Second
```

Detalhes:
- Cada mensagem enviada ao subscriber fica em `pending` ate receber `delivery_ack`.
- O broker reenvia periodicamente as mensagens pendentes.

Arquivo: broker/client_conn.go (linhas 141-188)

```go
ticker := time.NewTicker(deliveryRetryInterval)
...
for _, msg := range items { b.trySend(c, msg) }
```

Arquivo: broker/client_conn.go (linhas 190-209)

```go
select {
case c.send <- msg:
default:
    // buffer cheio; sera reenviado pelo retry loop
}
```

## 4. Biblioteca cliente

### 4.1 Balanceamento, failover e rebalanceamento

Arquivo: client/util.go (linhas 23-46)

```go
func (c *Client) brokerOrder(topic string) []string { ... }
```

Arquivo: client/util.go (linhas 31-45)

```go
h := fnv.New32a()
_, _ = h.Write([]byte(topic))
start = int(h.Sum32()) % len(c.brokers)
```

Arquivo: client/conn.go (linhas 45-65)

```go
order := c.brokerOrder(topic)
for _, addr := range order { if bc, err := c.getConn(addr); err == nil { return bc, nil } }
```

Arquivo: client/conn.go (linhas 68-111)

```go
conn, err := net.Dial("tcp", addr)
...
go c.writerLoop(bc)
go c.readerLoop(bc)
```

Arquivo: client/client.go (linhas 251-312)

```go
func (c *Client) rebalanceLoop() { ... c.rebalanceSubscriptions() }
```

Detalhes:
- O topico e mapeado por hash para um broker preferido.
- Em falha, o cliente tenta os proximos brokers da lista (failover).
- Quando o broker preferido volta, o cliente reequilibra as assinaturas.

Arquivo: client/client.go (linhas 275-312)

```go
preferred := c.preferredBroker(topic)
if preferred == "" || current[topic] == preferred { continue }
...
_ = c.sendAndWait(oldConn, frame{Type: "unsubscribe", Topic: topic})
```

### 4.2 Publish com retry e ACK

Arquivo: client/conn.go (linhas 195-235)

```go
for { err := c.sendOnceAndWait(bc, f); if err == nil { return nil } ... }
```

Detalhes:
- O publish reenviara em caso de timeout de ACK ou queda de conexao.
- Esse comportamento suporta at-least-once do lado do publisher.

Arquivo: client/conn.go (linhas 236-279)

```go
select {
case ack := <-ch:
    if !ack.Ok { return errors.New(...) }
case <-time.After(ackTimeout):
    return errAckTimeout
}
```

### 4.3 Subscribe e delivery_ack

Arquivo: client/client.go (linhas 100-147)

```go
ch := make(chan Message, subBufferSize)
... sendAndWait(subscribe)
```

Arquivo: client/conn.go (linhas 160-184)

```go
ch <- msg
ack := frame{ Type: "delivery_ack", ID: f.ID, Topic: f.Topic }
```

Arquivo: client/conn.go (linhas 169-186)

```go
select {
case bc.send <- ack:
case <-bc.quit:
}
```

Detalhes:
- A entrega e bloqueante, garantindo que o subscriber processe a mensagem antes do ACK.
- O broker so remove a mensagem pendente apos o delivery_ack.

## 5. Aplicacoes de exemplo

### 5.1 Topicos e configuracao

Arquivo: examples/helpers.go (linhas 21-98)

```go
const (
    TopicTemperatura = "temperatura_maquina"
    TopicPressao     = "pressao"
    TopicFalhaMotor  = "falha_motor"
    TopicConsumo     = "consumo_energia"
)
...
func getBrokerAddr() string { ... }
```

Detalhes:
- Os topicos do cenario IoT Industrial estao centralizados.
- `getBrokerAddr` carrega `.env` e usa fallback se necessario.

Arquivo: examples/helpers.go (linhas 120-150)

```go
func subscribeTopics(c *client.Client, topics []string) (map[string]<-chan client.Message, error) {
    for _, topic := range topics { ... c.Subscribe(topic) }
}
```

Arquivo: examples/helpers.go (linhas 152-160)

```go
for topic, ch := range subs {
    go printLoop(log, topic, ch)
}
```

### 5.2 Publisher (IoT Industrial)

Arquivo: examples/publisher.go (linhas 31-114)

```go
specs := []topicSpec{ ... }
for idx, spec := range specs {
    go func() { ... ticker.C -> Publish(...) }()
}
```

Detalhes:
- Ha uma goroutine por topico, com ticker e payload especifico.
- O publisher pode ser iniciado varias vezes (varios produtores).

Arquivo: examples/publisher.go (linhas 88-108)

```go
for idx, spec := range specs {
    go func() {
        ticker := time.NewTicker(spec.interval)
        ...
        payload := spec.build(r)
        c.Publish(spec.name, payload)
    }()
}
```

### 5.3 Subscriber unico com role

Arquivo: examples/subscriber.go (linhas 10-76)

```go
role := normalizeRole(role)
... topics = AllTopics() ou TopicsExceptLast()
... subscribeTopics(c, topics)
```

Detalhes:
- `role=painel`: assina 3 topicos.
- `role=alertas`: assina todos os topicos.

Arquivo: examples/subscriber.go (linhas 24-55)

```go
switch role {
case roleAlertas:
    topics = AllTopics()
default:
    topics = TopicsExceptLast()
}
```

### 5.4 CLI dos exemplos

Arquivo: examples/main.go (linhas 10-26)

```go
-mode publisher|subscriber
-role painel|alertas
```

## 6. Logs e observabilidade

Arquivo: common/logx/logx.go (linhas 24-105)

```go
func (l *Logger) Infof(...) { l.log("INFO", ...) }
```

Detalhes:
- Logs coloridos por componente (BROKER, PUB-IOT, PAINEL, ALERTAS).
- `NO_COLOR=1` ou `LOG_NO_COLOR=1` desativa cores.

Arquivo: common/logx/logx.go (linhas 62-92)

```go
timestamp := time.Now().Format("15:04:05.000")
fmt.Fprintf(l.out, "%s [%s] %s | %s\n", timestamp, l.component, level, msg)
```

## 7. Concorrencia (goroutines)

Principais pontos:
- Broker: `acceptLoop`, `readerLoop`, `writerLoop`, `topicLoop` e `deliveryRetryLoop`.
- Cliente: loops de leitura/escrita por conexao e rebalanceamento periodico.
- Exemplos: publisher cria uma goroutine por topico; subscriber cria uma goroutine por topico para imprimir.

Arquivos e linhas:
- Broker accept/read/write/retry: broker/broker.go (linhas 79-120) e broker/client_conn.go (linhas 41-166)
- Fanout por topico: broker/topic.go (linhas 15-38)
- Cliente read/write: client/conn.go (linhas 121-158)
- Publisher goroutines: examples/publisher.go (linhas 85-108)
- Subscriber goroutines: examples/helpers.go (linhas 158-162)

## 8. Transparencia, balanceamento e retry

Resumo:
- Transparencia: apps nao precisam saber qual broker atende cada topico; o cliente decide.
- Balanceamento: hash por topico gera afinidade com um broker.
- Retry/ACK: publish tem retry e subscriber envia `delivery_ack`.

Referencias de codigo:
- Balanceamento por hash: client/util.go (linhas 23-46)
- Failover e retry publish: client/conn.go (linhas 45-235)
- Rebalanceamento: client/client.go (linhas 251-312)
- At-least-once broker: broker/client_conn.go (linhas 119-193)
