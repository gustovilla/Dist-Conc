# Relatório – Trabalho 1 de Distribuição e Concorrência

**Tema:** Balanceamento de carga, elasticidade e failover com Kafka em clusters Docker
**Mini-mundo:** Sistema de monitoramento de sensores em uma fábrica inteligente
**Grupo:** Pedro e Enzo — INF1304, PUC-Rio, 2026.2

> A documentação de instalação e uso está no [README.md](README.md).

---

## 1. O que foi implementado

| Objetivo do enunciado | Implementação | Onde |
|---|---|---|
| Cluster Kafka com múltiplos brokers, tópico com partições e replicação | 3 brokers `apache/kafka` em modo KRaft (cada um é broker e controller). Tópico `dados-sensores` com 6 partições, fator de replicação 3 e `min.insync.replicas=2`, criado pelo serviço `kafka-init` | `docker-compose.yaml` |
| Sensores como produtores em containers distintos | 3 serviços (produção, refrigeração, empacotamento). Cada container simula N máquinas e publica JSON com temperatura, vibração e energia. Podem ser escalados. | `sensor/` |
| Múltiplos consumidores com balanceamento automático | Serviço `processador` com N réplicas no mesmo grupo de consumo. Detecta anomalias e grava em SQLite. | `processador/` |
| Falha de broker | `make falha-broker` / `scripts/falha_broker.sh` | `scripts/` |
| Falha de consumidor e rebalanço | `make falha-consumidor` / `scripts/falha_consumidor.sh` | `scripts/` |
| Elasticidade | `make escala-processadores N=…`, `make escala-sensores N=…` | `scripts/escala.sh` |
| Demonstração via logs | Linhas `REBALANCO`, `ESTATISTICA` e `ALERTA`, tabela `eventos_grupo` no banco, `make status` e `make demo` | `logs/` |
| Banco de dados / logger | SQLite em volume compartilhado, com as tabelas `leituras`, `alertas` e `eventos_grupo` | `processador/armazenamento.py` |
| Sem constantes no código | Tudo em `.env`, repassado como variáveis de ambiente pelo YAML | `.env` |
| Documentação do código | Docstrings (estilo Google) em todos os módulos, classes e funções | `*.py` |

---

## 2. Arquitetura

```
 sensores (3 serviços, N containers)            processadores (N réplicas, 1 grupo)
 ┌───────────────────┐   produce(key=máquina)   ┌──────────────┐   ┌─────────────┐
 │ sensor-producao   │─┐   acks=all             │ processador  │──▶│  SQLite     │
 │ sensor-refrigeracao├─┼──▶ dados-sensores ────▶│ processador  │──▶│ (volume     │
 │ sensor-empacotam. │─┘   6 partições × 3 rép. │ processador  │──▶│  dados-db)  │
 └───────────────────┘   kafka-1/2/3 (KRaft)    └──────────────┘   └─────────────┘
```

### 2.1 Fluxo de uma leitura

1. O sensor gera a leitura de uma máquina e chama `produce(topico, key=maquina_id, value=json)`.
2. O cliente Kafka calcula `partição = hash(maquina_id) mod 6`. A mesma máquina cai sempre na mesma partição, o que preserva a ordem das leituras dela.
3. O líder da partição grava a mensagem e espera as réplicas em sincronia (`acks=all`). Com `min.insync.replicas=2`, a escrita é confirmada quando pelo menos 2 dos 3 brokers têm a mensagem.
4. O processador dono daquela partição recebe a mensagem em `poll()`, compara as métricas com os limites, grava a leitura (e os alertas) no SQLite e só então marca o offset com `store_offsets()`. A biblioteca confirma os offsets marcados periodicamente.

### 2.2 Decisões de projeto

- **KRaft em vez de Zookeeper:** o Zookeeper foi removido do Kafka 4.0. Cada nó é broker e controller, e o quórum de 3 controllers tolera a queda de 1.
- **Um volume por broker:** cada broker precisa do seu diretório de dados. Compartilhar um único volume corromperia os logs e os metadados do cluster.
- **Replicação 3 com min ISR 2:** com um broker fora, as escritas continuam. Com dois fora, os produtores passam a receber erro (`NOT_ENOUGH_REPLICAS`), porque o sistema prefere recusar escritas a arriscar perder dados.
- **6 partições:** é o limite de paralelismo do grupo. 6 é divisível por 1, 2, 3 e 6, o que deixa a divisão visivelmente equilibrada nas demonstrações.
- **`cooperative-sticky`:** no rebalanço, só as partições que precisam mudar de dono são movidas. Os outros processadores não param, ao contrário da estratégia `range`, que revoga tudo de todos.
- **Commit após gravação + chave primária no banco:** o Kafka garante entrega *pelo menos uma vez*. Se um processador cai depois de gravar e antes de confirmar o offset, a mensagem é reentregue a outro processador. O `INSERT OR IGNORE` na chave `(tópico, partição, offset)` descarta a duplicata, e o banco fica com *exatamente uma* cópia de cada leitura.
- **SQLite compartilhado:** vários containers escrevem no mesmo arquivo, em modo WAL e com `timeout` de espera pelo lock. É adequado para o volume do trabalho (dezenas de mensagens/s). Em produção seria usado um banco servidor.
- **Rede interna (`internal: true`):** os containers só se comunicam entre si. Nenhuma porta do Kafka é exposta ao host.
- **Particionador `murmur2_random`:** é o mesmo algoritmo do cliente Java do Kafka. O padrão da `librdkafka` (CRC32) é linear e, com chaves parecidas (`producao-588251-m1`, `-m2`...), colocou todas as máquinas nas partições 0 e 2 (ver seção 5).
- **`broker.session.timeout.ms` = 4 s (padrão 9 s):** é o tempo que o controller leva para declarar um broker morto e retirá-lo do ISR. Enquanto o broker morto está no ISR, as escritas com `acks=all` ficam esperando por ele. Com 9 s, essa espera se aproximava do `session.timeout.ms` de 10 s dos processadores e provocava rebalanços desnecessários (ver seção 5).

### 2.3 Concorrência

- **Sensores:** o callback de entrega (`ao_entregar`) roda dentro de `producer.poll()`, na mesma thread do laço principal. Por isso os contadores não precisam de lock. O envio em rede é feito por threads internas da `librdkafka`.
- **Processadores:** cada réplica é um processo independente. A concorrência entre elas é resolvida pelo Kafka, que garante uma partição por consumidor, e pelo SQLite (lock de escrita + WAL). Os callbacks de rebalanço também rodam dentro de `consumer.poll()`.
- **Encerramento:** `SIGTERM` (`docker stop`, escala para baixo) muda uma flag. O laço termina e `consumer.close()` confirma os offsets e sai do grupo de forma ordenada, o que dispara um rebalanço imediato.

---

## 3. Testes de falha e elasticidade

Todos os testes podem ser reproduzidos individualmente (`make falha-broker`
etc.) ou em sequência com `make demo`, que grava toda a saída em
`logs/demo-<data>.log`.

Os resultados abaixo vêm da execução registrada em
[logs/demo-20260925-194937.log](logs/demo-20260925-194937.log): rodada limpa
(`make limpar && make up && make demo`), com 3 brokers, 3 containers de
sensor (8 máquinas cada, 24 leituras/s no total) e 3 processadores.

### 3.1 Falha de broker

**Procedimento:** `make falha-broker` executa `docker compose kill` (queda abrupta, SIGKILL) em um broker. O script aceita:
- `BROKER=nao-controller` (padrão): um broker comum;
- `BROKER=controller`: o controller ativo, líder do quórum KRaft, que é o caso mais severo;
- `BROKER=kafka-N`: um broker específico.

**Esperado:** as partições lideradas pelo broker morto ganham um novo líder, o `Isr` cai de 3 para 2 réplicas e o sistema continua sem perder mensagens.

**Resultado obtido (a) — broker comum (`kafka-2`, com o controller no `kafka-1`):**

```
Antes:  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
        Partition: 5  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Depois: Partition: 0  Leader: 3  Replicas: 2,3,1  Isr: 3,1
        Partition: 5  Leader: 3  Replicas: 2,3,1  Isr: 3,1
```

- As partições 0 e 5, lideradas pelo `kafka-2`, passaram para o `kafka-3`. Todas as partições ficaram com `Isr` de 2 réplicas.
- Os sensores seguiram a 8,0 msg/s cada, sempre com `falhas=0`.
- Os processadores seguiram com as mesmas partições e a mesma taxa. **Não houve rebalanço.**

**Resultado obtido (b) — broker controller (`kafka-1`):**

- O quórum elegeu o `kafka-3` como novo controller ("controller ativo agora: kafka-3").
- As partições 2 e 4, lideradas pelo `kafka-1`, passaram para o `kafka-2`.
- Os sensores continuaram com `falhas=0`. Um deles mostrou 10,8 msg/s no período seguinte, porque as mensagens retidas durante a eleição foram entregues de uma vez.
- **Não houve rebalanço** do grupo de processadores.

**Recuperação:** `make recupera-broker` religa o broker. Em menos de 20 s ele voltou ao `Isr` de todas as partições (`Isr: 3,2,1`). A liderança não volta imediatamente ao broker "preferido": o Kafka só faz isso na verificação periódica de desequilíbrio de líderes.

### 3.2 Falha de consumidor e rebalanço

**Procedimento:** `make falha-consumidor` executa `docker kill` (SIGKILL) em um processador.

**Comportamento esperado:**
- durante até `SESSION_TIMEOUT_MS` (10 s), as partições do processador morto ficam sem consumo, e o `LAG` delas cresce em `kafka-consumer-groups --describe`;
- o coordenador então remove o membro e os sobreviventes registram `REBALANCO ... atribuidas: [...]`;
- o atraso acumulado é consumido e as leituras reentregues aparecem como `duplicadas` nas estatísticas, sem duplicar registros no banco.

Com `MODO=stop` (SIGTERM) a saída é ordenada e o rebalanço é imediato.

**Resultado obtido:** o `fabrica-processador-1` (id `3b4721499e1b`), responsável pelas partições 0 e 2, foi morto às 22:52:32.

```
22:52:44 REBALANCO processador=03ebfdfe1e89 partições atribuidas: [2] -> responsável por [1, 2, 3]
22:52:44 REBALANCO processador=2ff87803d62b partições atribuidas: [0] -> responsável por [0, 4, 5]
22:52:51 ESTATISTICA processador=2ff87803d62b partições=[0, 4, 5] processadas=152 ... duplicadas=9
22:52:51 ESTATISTICA processador=03ebfdfe1e89 partições=[1, 2, 3] processadas=196 ... duplicadas=14
```

- O coordenador detectou a queda **12 s depois do `kill`**: o `session.timeout.ms` de 10 s mais o ciclo de heartbeat.
- Com a estratégia `cooperative-sticky`, só as partições órfãs (0 e 2) mudaram de dono. Os outros processadores mantiveram as suas.
- As 23 leituras que o processador morto já tinha gravado, mas cujo offset ainda não tinha sido confirmado, foram reentregues e **descartadas pelo banco** (`duplicadas=9` e `duplicadas=14`).
- O atraso (`LAG`) acumulado durante a detecção foi consumido em seguida, com os processadores trabalhando a 15–20 msg/s em vez de 7–10.

### 3.3 Elasticidade

| Processadores | Partições por processador | Esperado |
|---|---|---|
| 3 (inicial) | 2 | carga dividida por 3 |
| 6 | 1 | cada um com 1/6 da carga |
| 8 | 1 ou 0 | 2 processadores ociosos (`responsável por []`): o paralelismo é limitado pelo número de partições |
| 2 | 3 | cada um com metade da carga |

Aumentar os sensores (`make escala-sensores N=3`) triplica as mensagens por
segundo, o que aparece no campo `msg/s` das linhas `ESTATISTICA processador`.

**Resultado obtido:**

| Passo | O que os logs mostraram |
|---|---|
| 3 → 6 processadores | Três containers entraram e o grupo se reorganizou em rodadas incrementais (22:52:56, :59 e 22:53:02). Ao final, cada processador ficou com exatamente 1 partição, sem nenhum processador parar por completo. |
| 6 → 8 processadores | Os dois novos membros receberam `atribuidas: [] -> responsável por []` e registraram `ESTATISTICA ... partições=[] processadas=0`: ficaram **ociosos**, porque o paralelismo máximo é igual ao número de partições (6). |
| 8 → 2 processadores | Os 6 containers removidos receberam SIGTERM e saíram do grupo de forma ordenada (`revogadas: [4] -> []`). Os 2 restantes assumiram 3 partições cada (`[0, 1, 4]` e `[2, 3, 5]`) em menos de 10 s. |
| Sensores 1 → 3 por setor | A carga subiu de ~12 para **~36 msg/s por processador**. O `LAG` subiu momentaneamente para 40–60 mensagens por partição e foi absorvido: na etapa seguinte havia voltado para perto de zero. |

**Relatório do banco (`make consultar`) ao final:**

```
processador   leituras  particoes
2ff87803d62b  2055      5,3,0,4,1,2     <- inicial; depois removido na redução para 2
3b4721499e1b  3381      0,2,1,4         <- inicial; morto e depois religado pela escala
03ebfdfe1e89  3173      1,3,2,5         <- inicial
5d5e5f56bd76  189       0               <- criado na escala para 6
96f00a0bfaf1  177       4               <- criado na escala para 6
f1d14369fdfd  265       2               <- criado na escala para 6

Anomalias: leituras=9240  simuladas=287  leituras_com_alerta=264
```

**Verificação de integridade** (consulta ao SQLite depois da demonstração):

| Verificação | Resultado |
|---|---|
| Offsets gravados por partição = maior offset − menor offset + 1 | **Sim, nas 6 partições** (0 a 1913, 0 a 1472, ...). Nenhuma mensagem perdida e nenhuma gravada em dobro, mesmo com 2 quedas de broker, 1 queda de processador e 5 mudanças de escala. |
| Alertas em leituras sem anomalia simulada (falsos positivos) | **0** |
| Anomalias simuladas sem alerta | 28, todas do setor de empacotamento: o valor base desse setor é mais baixo (55 °C, 20 kW), e um acréscimo de 75% a 125% do valor configurado nem sempre ultrapassa os limites globais (85 °C, 45 kW). A detecção está correta; é a simulação que nem sempre gera um valor anômalo. |

---

## 4. Exibição dos resultados

- **Logs:** `make logs-processadores`, `make rebalancos`, `make alertas`.
- **Estado do cluster:** `make status` mostra os líderes e o ISR por partição e o `CLIENT-ID` do processador dono de cada partição, com o `LAG`.
- **Banco:** `make consultar` mostra as leituras por processador (balanceamento), as leituras por partição, o histórico de rebalanços e os alertas, além da comparação entre anomalias simuladas e detectadas.

---

## 5. O que funcionou e o que não funcionou

### Funcionou

Todos os objetivos do enunciado foram demonstrados na execução registrada em `logs/`:
- o cluster de 3 brokers, com o tópico em 6 partições replicadas 3 vezes;
- sensores em containers distintos e escaláveis;
- balanceamento automático entre os processadores;
- continuidade com a queda de um broker, inclusive do controller;
- rebalanço com a queda de um processador;
- elasticidade para cima e para baixo;
- nenhuma perda nem duplicação no banco.

### Problemas encontrados durante o desenvolvimento (e como foram resolvidos)

| Problema observado | Causa | Solução |
|---|---|---|
| Os três serviços de sensor falhavam no build com `image "fabrica-sensor:latest": already exists` | Os três serviços declaravam o mesmo nome de imagem e o Compose os construía em paralelo | Removido o nome fixo; cada serviço gera sua própria imagem, reaproveitando as camadas do cache |
| Cada processador recebia ~400 msg/s em vez das ~9 esperadas | `producer.poll(t)` retorna assim que atende um callback de entrega, e o sensor não esperava o intervalo completo | Laço que chama `poll()` até completar o intervalo |
| Todas as 9 máquinas iniciais caíam só nas partições 0 e 2, deixando dois processadores ociosos | O particionador padrão da `librdkafka` (CRC32) é linear, e para chaves parecidas `hash mod 6` saía quase sempre par | `partitioner=murmur2_random`. Além disso, 8 máquinas por container: com poucas chaves a distribuição por hash é naturalmente irregular. |
| Na queda de **qualquer** broker, a escrita parava ~10 s e **todos os processadores perdiam suas partições** (`partições perdidas`), gerando um rebalanço completo | O controller levava `broker.session.timeout.ms` (9 s) para declarar o broker morto. Até lá ele continuava no ISR, e as escritas com `acks=all` e as gravações do coordenador do grupo esperavam por ele, esgotando o `session.timeout` (10 s) dos processadores. | `KAFKA_BROKER_SESSION_TIMEOUT_MS=4000` e heartbeat de 1 s. Depois disso, a queda de um broker comum e a do controller passaram sem nenhum rebalanço (seção 3.1). |
| Durante os testes, todos os containers terminaram de uma vez com código 255 | O WSL desliga a máquina virtual quando não há nenhum terminal do Ubuntu aberto, e o Docker cai junto | Documentado no README: manter um terminal aberto. Ao rodar `make up` de novo, o sistema voltou com os dados preservados nos volumes, sem lacunas de offset. |

Numa queda de broker, ainda pode haver rebalanço do grupo se o broker morto
for o **coordenador do grupo** (o broker que lidera a partição de
`__consumer_offsets` do grupo). Isso foi observado em um teste exploratório: os
processadores precisaram localizar o novo coordenador, dois deles perderam as
partições por cerca de 10 s e as mensagens reentregues foram descartadas como
duplicadas. Não houve perda de dados. É o comportamento esperado do protocolo,
e a demonstração final não caiu nesse caso.

### Limitações conhecidas

- Com **dois brokers fora** ao mesmo tempo, o cluster perde o quórum KRaft e as partições ficam sem o mínimo de réplicas. O sistema para de aceitar escritas até um broker voltar. Isso é consequência da configuração escolhida, não um defeito.
- O **SQLite** é um ponto único: fica em um volume local do host Docker. Ele não seria adequado a um cluster com várias máquinas.
- A detecção de anomalias usa limites fixos por métrica, iguais para todos os setores.
