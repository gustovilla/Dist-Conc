#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Simula a queda de um broker Kafka e mostra que o sistema continua:
#  - as partições lideradas pelo broker ganham novo líder;
#  - o broker some da lista de réplicas em sincronia (Isr);
#  - sensores continuam entregando e processadores continuam processando,
#    pois ainda há 2 réplicas (= min.insync.replicas) de cada partição.
#
# Uso: bash scripts/falha_broker.sh [alvo]
#   alvo = nao-controller (padrão) | controller | kafka-1 | kafka-2 | kafka-3
#   "controller" derruba o líder do quórum KRaft, o caso mais severo:
#   além dos líderes de partição, o cluster precisa eleger outro controller.
# ---------------------------------------------------------------------
source "$(dirname "$0")/lib.sh"

controller="kafka-$(controller_ativo)"
BROKER=$(resolver_broker "${1:-nao-controller}")

titulo "Antes da falha: líderes e réplicas (controller ativo: $controller)"
descrever_topico

titulo "Derrubando $BROKER (docker compose kill = queda abrupta)"
dc kill "$BROKER"
esperar "$ESPERA_REACAO_S" "para o cluster eleger novos líderes"

titulo "Depois da falha: containers"
listar_containers

titulo "Depois da falha: líderes e réplicas ($BROKER não aparece mais no Isr)"
echo "controller ativo agora: kafka-$(controller_ativo)"
descrever_topico

titulo "Depois da falha: grupo de consumo continua atendido"
descrever_grupo

titulo "Sensores e processadores desde a queda (continuam funcionando)"
logs_filtrados "$(( ESPERA_REACAO_S + 15 ))" 'ESTATISTICA|falha|erro|ERROR|REBALANCO' "${SENSORES[@]}" processador
