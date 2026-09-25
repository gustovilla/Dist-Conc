# Monitoramento de Sensores de uma Fábrica Inteligente

Trabalho 1 de INF1304 – Distribuição e Concorrência (PUC-Rio, 2026.2).
Grupo: Pedro e Enzo.

Balanceamento de carga, elasticidade e failover com **Apache Kafka** em um
cluster **Docker Compose**. Sensores simulados publicam leituras em um tópico
particionado e replicado em 3 brokers. Um grupo de processadores divide as
partições entre si, detecta anomalias e grava leituras e alertas em
**SQLite**.

---

## 1. Arquitetura

```
             ┌──────────────── rede interna int-network ─────────────────┐
             │                                                            │
 sensor-producao ──┐        ┌──────────── Cluster Kafka (KRaft) ───────┐  │
 sensor-refrigeracao ┼─────▶│ kafka-1   kafka-2   kafka-3              │  │
 sensor-empacotamento┘      │ (broker + controller em cada nó)         │  │
   produtores               │ tópico dados-sensores                    │  │
   chave = máquina          │   6 partições × 3 réplicas, min ISR = 2  │  │
                            └───────────────┬──────────────────────────┘  │
                                            │ grupo "processadores"       │
                        ┌───────────────────┼───────────────────┐         │
                        ▼                   ▼                   ▼         │
                 processador-1       processador-2       processador-3    │
                 partições 0,1       partições 2,3       partições 4,5    │
                        └───────────────────┼───────────────────┘         │
                                            ▼                             │
                               volume dados-db: /data/fabrica.db (SQLite) │
             └────────────────────────────────────────────────────────────┘
```

| Componente | Imagem / código | Papel |
|---|---|---|
| `kafka-1..3` | `apache/kafka` (modo KRaft, sem Zookeeper) | Brokers e controllers. Cada um tem seu próprio volume. |
| `kafka-init` | `apache/kafka` | Cria o tópico `dados-sensores` com as partições e a replicação definidas e depois termina |
| `sensor-*` | [sensor/sensor.py](sensor/sensor.py) | Produtores. Cada container simula `SENSOR_MAQUINAS` máquinas de um setor. |
| `processador` | [processador/processador.py](processador/processador.py) | Consumidores do grupo `processadores`. Detectam anomalias e gravam no SQLite. |
| banco | [processador/armazenamento.py](processador/armazenamento.py) | Tabelas `leituras`, `alertas` e `eventos_grupo` |

### Como cada requisito é atendido

| Requisito | Mecanismo |
|---|---|
| **Balanceamento de carga** | O tópico tem 6 partições. A chave de cada mensagem é o ID da máquina, e o hash da chave espalha as máquinas pelas partições. Os processadores estão no mesmo *consumer group*, e o coordenador entrega cada partição a um único membro. |
| **Tolerância a falhas (broker)** | Replicação 3 e `min.insync.replicas=2`. Se um broker cai, as partições das quais ele era líder elegem um novo líder entre as réplicas em sincronia. Os produtores usam `acks=all` e continuam gravando com 2 réplicas. O quórum KRaft (3 controllers) tolera a perda de 1 nó. |
| **Tolerância a falhas (consumidor)** | Um processador que para de enviar heartbeats por `SESSION_TIMEOUT_MS` sai do grupo. O coordenador faz um **rebalanço** e as partições dele passam para os outros membros, que continuam do último offset confirmado. |
| **Sem perda nem duplicação** | O offset só é marcado depois da gravação no banco (entrega *at-least-once*). A chave primária `(tópico, partição, offset)` descarta mensagens reentregues após um rebalanço. |
| **Elasticidade** | `docker compose up --scale processador=N` adiciona ou remove membros do grupo sem parar os demais, e o rebalanço redistribui as partições. Com mais processadores que partições, os excedentes ficam ociosos, porque o paralelismo máximo é igual ao número de partições. |
| **Configuração** | Nada fica fixo no código: tudo está no [.env](.env) e chega aos containers pelo [docker-compose.yaml](docker-compose.yaml). |

---

## 2. Instalação

### Requisitos

- Linux, ou Windows com **WSL 2** (Ubuntu). Todos os comandos abaixo são executados no terminal Linux.
- Docker Engine com o plugin Compose v2, `make` e `bash`.
- Cerca de 4 GB de RAM livres (3 brokers com até 1,2 GB cada).

### Instalar o Docker no Ubuntu/WSL

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 make
sudo usermod -aG docker $USER      # usar docker sem sudo
```

Feche e reabra o terminal. No WSL, rode `wsl --shutdown` no PowerShell e abra
o Ubuntu de novo. Depois teste:

```bash
docker run --rm hello-world
docker compose version
```

### Obter o projeto

```bash
git clone https://github.com/enzofarina/INF1304---Pedro-e-Enzo.git
cd INF1304---Pedro-e-Enzo
```

No WSL o projeto também pode ser usado direto do disco do Windows, por
exemplo `cd /mnt/c/Users/<usuario>/Desktop/trabdc/INF1304---Pedro-e-Enzo`.

---

## 3. Operação

Rode `make` sem argumentos para ver todos os comandos.

### Subir e acompanhar

```bash
make up                  # constrói as imagens e sobe tudo (~1 min na 1ª vez)
make status              # containers, líderes/ISR do tópico, partições por processador
make logs-processadores  # acompanha os processadores (Ctrl+C para sair)
make consultar           # relatório do banco: carga por processador, rebalanços, alertas
```

Linhas importantes nos logs:

| Linha | Significado |
|---|---|
| `REBALANCO processador=<id> partições atribuidas: [2, 3] -> responsável por [2, 3]` | O processador recebeu (ou perdeu) partições |
| `ESTATISTICA processador=<id> partições=[2, 3] processadas=60 (6.0 msg/s) ...` | Carga tratada nos últimos `INTERVALO_ESTATISTICAS_S` segundos |
| `ALERTA maquina=... temperatura_c=97.31 > limite 85.00 ...` | Anomalia detectada |
| `ESTATISTICA sensor=<id> entregues=30 ... por_particao={1: 10, 4: 20}` | Leituras publicadas por um container de sensor |

O `<id>` é o hostname do container. É o mesmo valor que aparece na coluna
`CLIENT-ID` de `make status`.

### Simular falhas

```bash
make falha-broker                     # derruba um broker que NÃO é o controller; o sistema continua
make falha-broker BROKER=controller   # derruba o controller do quórum KRaft (caso mais severo)
make falha-broker BROKER=kafka-2      # derruba um broker específico
make recupera-broker                  # religa os brokers parados; eles voltam ao ISR
make falha-consumidor                 # SIGKILL em um processador -> rebalanço após o session timeout
make falha-consumidor MODO=stop       # saída ordenada -> rebalanço imediato
```

### Elasticidade e carga

```bash
make escala-processadores N=6   # 6 processadores: 1 partição cada
make escala-processadores N=8   # 8 > 6 partições: 2 ficam ociosos
make escala-processadores N=2   # volta a 2: cada um fica com 3 partições
make escala-sensores N=3        # 3 containers por setor: triplica a carga
```

### Roteiro completo e evidências

```bash
make demo          # executa todos os testes acima em sequência -> logs/demo-<data>.log
make salvar-logs   # logs completos de cada serviço -> logs/completo-<data>/
```

### Encerrar

```bash
make down     # para os containers (dados preservados nos volumes)
make limpar   # para e apaga os volumes (Kafka e SQLite do zero)
```

---

## 4. Configuração

Todos os parâmetros ficam em [.env](.env). Os principais:

| Variável | Padrão | Efeito |
|---|---|---|
| `TOPICO_PARTICOES` | 6 | Paralelismo máximo do grupo de processadores |
| `TOPICO_REPLICACAO` / `TOPICO_MIN_ISR` | 3 / 2 | Cópias de cada partição e mínimo de cópias para aceitar uma escrita |
| `PROCESSADOR_REPLICAS` | 3 | Processadores iniciais |
| `SENSOR_MAQUINAS`, `SENSOR_INTERVALO_S` | 8, 1.0 | Carga gerada por container de sensor (8 máquinas × 1 leitura/s) |
| `SENSOR_PARTICIONADOR` | murmur2_random | Hash chave → partição. O padrão da librdkafka (crc32) concentrava as máquinas em poucas partições. |
| `SENSOR_PROB_ANOMALIA` | 0.03 | Frequência das anomalias simuladas |
| `LIMITE_TEMPERATURA_C`, `LIMITE_VIBRACAO_MM_S`, `LIMITE_ENERGIA_KW` | 85, 7.1, 45 | Limites de alerta |
| `ESTRATEGIA_ATRIBUICAO` | cooperative-sticky | Estratégia de distribuição de partições (`range`, `roundrobin` ou `cooperative-sticky`) |
| `SESSION_TIMEOUT_MS` | 10000 | Tempo até um processador silencioso ser considerado morto |
| `KAFKA_BROKER_SESSION_TIMEOUT_MS` | 4000 | Tempo até o controller considerar um broker morto e tirá-lo do ISR (padrão do Kafka: 9000) |
| `ESPERA_REACAO_S` | 20 | Espera dos scripts após cada falha ou escala |

Os valores base de cada setor (temperatura, vibração e energia) estão no
próprio `docker-compose.yaml`, nos serviços `sensor-*`.

---

## 5. Estrutura do projeto

```
.
├── .env                      # toda a configuração
├── docker-compose.yaml       # brokers, criação do tópico, sensores, processadores
├── Makefile                  # atalhos de operação
├── sensor/                   # produtor (Python + confluent-kafka)
├── processador/              # consumidor, persistência SQLite e relatório
├── scripts/                  # simulação de falhas, escala, status, demo
├── logs/                     # evidências geradas por make demo / make salvar-logs
└── RELATORIO.md              # relatório do trabalho
```

---

## 6. Problemas comuns

| Sintoma | Causa provável e solução |
|---|---|
| Todos os containers aparecem como `Exited (255)` | O WSL desliga a máquina virtual quando nenhum terminal do Ubuntu fica aberto, e o Docker cai junto. Mantenha um terminal do Ubuntu aberto durante o uso e rode `make up` de novo (os dados ficam preservados nos volumes). |
| `permission denied ... docker.sock` | O usuário não está no grupo `docker`. Rode `sudo usermod -aG docker $USER` e reabra o terminal (`wsl --shutdown` no WSL). |
| `kafka-init` termina com erro | Algum broker não subiu. Veja `docker compose logs kafka-1`. Se os dados ficaram inconsistentes, rode `make limpar && make up`. |
| Container de broker reiniciando por falta de memória | Reduza `KAFKA_HEAP_OPTS` no `.env` ou aumente a memória do WSL (`.wslconfig`). |
| `$'\r': command not found` ao rodar um script | O arquivo foi salvo com fim de linha do Windows (CRLF). O `.gitattributes` força LF. Rode `git add --renormalize .` ou converta com `dos2unix`. |
