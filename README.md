# Middleware Publish/Subscribe

Middleware para troca de mensagens entre processos distribuídos utilizando arquitetura Pub/Sub.

## Estrutura do Projeto

```
middleware-pubsub/
├── broker/           # Servidor broker
├── client/           # Biblioteca cliente
├── examples/         # Aplicações de exemplo
├── go.mod           # Módulo Go
└── README.md        # Este arquivo
```

## Componentes

### Broker (`broker/`)
- **main.go**: Inicializa e executa o broker
- **broker.go**: Lógica principal do broker (gerenciamento de tópicos, inscrições e roteamento)

### Client (`client/`)
- **client.go**: Biblioteca cliente para publicar e se inscrever em tópicos

### Examples (`examples/`)
- **publisher1.go**: Primeira aplicação publicadora
- **publisher2.go**: Segunda aplicação publicadora
- **subscriber1.go**: Primeira aplicação consumidora
- **subscriber2.go**: Segunda aplicação consumidora

## Protocolo de Comunicação

Troca de mensagens via JSON (um objeto por linha). Campos principais:

- `type`: tipo da mensagem (`subscribe`, `unsubscribe`, `publish`, `message`, `ack`)
- `id`: identificador de requisicao para correlacionar respostas
- `topic`: nome do topico
- `data`: JSON livre com os dados da aplicacao
- `ok` e `error`: usados no `ack`

Exemplos:

```json
{"type":"subscribe","id":"req-1","topic":"localizacao"}
{"type":"publish","id":"req-2","topic":"localizacao","data":{"lat":-7.12,"lng":-34.86}}
{"type":"message","topic":"localizacao","data":{"lat":-7.12,"lng":-34.86}}
{"type":"ack","id":"req-2","ok":true}
```

Regras importantes:

- Se nao houver inscritos no topico, o `publish` responde com `ok=false` e `error="no_subscribers"`.
- Mensagens sao bufferizadas por topico, entao o broker continua aceitando novos `publish`.

## Balanceamento de Carga

O cliente aceita varios brokers no parametro `brokerAddr` (ex: `"localhost:9000,localhost:9001"`).
O topico e roteado para um broker usando hash do nome do topico, garantindo que o mesmo topico
sempre va para a mesma instancia.

## Cenário de Teste

Cenario com 4 topicos:

- `localizacao` e `temperatura` publicados pelo `publisher1`
- `inscricao_usuario` e `alerta` publicados pelo `publisher2`
- Dois subscribers consomem pares distintos de topicos

## Como Executar

No diretorio do projeto:

1. Inicie o broker:
	- `go run ./broker -addr :9000`
2. (Opcional) Inicie outra instancia para balanceamento:
	- `go run ./broker -addr :9001`
3. Em outro terminal, rode os publishers:
	- `set BROKER_ADDR=localhost:9000` (Windows CMD)
	- `go run ./examples -mode publisher1`
	- `go run ./examples -mode publisher2`

Se estiver usando duas instancias do broker, use:

- `set BROKER_ADDR=localhost:9000,localhost:9001`

Depois rode os subscribers:

- `go run ./examples -mode subscriber1`
- `go run ./examples -mode subscriber2`
