# Middleware Publish/Subscribe (Go)

Middleware para troca de mensagens entre processos distribuidos utilizando arquitetura Pub/Sub.

## Estrutura do Projeto

```
Projeto---Middleware-Publish-Subscribe/
├── broker/            # Servidor broker (Go)
├── client/            # Biblioteca cliente (Go)
├── common/            # Tipos compartilhados e logger
├── examples/          # Aplicacoes de exemplo
├── .env               # Variaveis locais (opcional)
├── .env.example       # Exemplo de variaveis
├── go.mod             # Modulo Go
└── README.md          # Este arquivo
```

## Protocolo de Comunicacao

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

## Garantia de Entrega (At-Least-Once)

- O broker exige `delivery_ack` do subscriber para cada mensagem.
- Se o ACK nao chegar, o broker reenvia periodicamente.
- Isso garante entrega **ao menos uma vez**, podendo gerar **duplicatas** (o consumidor deve tolerar isso).

## Variaveis de Ambiente

- `BROKER_ADDR`: lista de brokers (ex: `localhost:9000,localhost:9001`).
- Se nao for definido, o padrao e usar `localhost:9000` ate `localhost:9001`.
- O arquivo `.env` e carregado automaticamente pelas aplicacoes em `examples/`.

## Balanceamento de Carga

O cliente aceita varios brokers no parametro `brokerAddr`.
O topico e roteado para um broker usando hash do nome do topico, garantindo que o mesmo topico
sempre va para a mesma instancia.

## Cenario 2 — IoT Industrial

Topicos:

- `temperatura_maquina`
- `pressao`
- `falha_motor`
- `consumo_energia`

Subscribers:

- Painel industrial (role `painel`) assina 3 topicos
- Alertas de manutencao (role `alertas`) assina todos os topicos

## Como Executar (Terminais Separados)

No diretorio do projeto:

1. Inicie um broker:
	- `go run ./broker -addr :9000`
2. (Opcional) Inicie mais brokers para balanceamento:
	- `go run ./broker -addr :9001`
	- `go run ./broker -addr :9002`
3. Em outro terminal, rode o publisher:
	- `set BROKER_ADDR=localhost:9000,localhost:9001` (Windows CMD)
	- `go run ./examples -mode publisher`
4. Rode os subscribers:
	- `go run ./examples -mode subscriber -role painel`
	- `go run ./examples -mode subscriber -role alertas`

## Teste de Multi-Broker

1. Suba brokers nas portas 9000 e 9001 (ou outra combinacao ativa).
2. Defina `BROKER_ADDR` com a lista de portas usadas.
3. Rode publisher/subscribers e observe os logs: cada topico sempre vai para o mesmo broker.

## Transparencia e Rebalanceamento

- Se um broker cair, o cliente tenta outro broker disponivel automaticamente.
- Quando o broker preferido volta, as assinaturas sao reequilibradas para ele.

## Logs Coloridos

Os logs sao coloridos por componente (BROKER, PUB-1, SUB-1, etc).
Para desativar cores, defina `NO_COLOR=1` ou `LOG_NO_COLOR=1`.
