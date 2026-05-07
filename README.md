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

(A definir)

## Cenário de Teste

(A definir)

## Como Executar

(A definir)
