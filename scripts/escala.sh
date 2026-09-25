#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Elasticidade: muda o número de réplicas de um ou mais serviços sem
# reiniciar os containers existentes, e mostra o efeito.
#
#   processador: novos membros entram no grupo -> rebalanço -> partições
#                redistribuídas. Com mais processadores que partições, os
#                excedentes ficam ociosos (nenhuma partição).
#   sensores:    mais máquinas publicando -> mais carga por processador.
#
# Uso: bash scripts/escala.sh <N> <serviço> [serviço...]
#   ex.: bash scripts/escala.sh 6 processador
#        bash scripts/escala.sh 3 sensor-producao sensor-refrigeracao sensor-empacotamento
# ---------------------------------------------------------------------
source "$(dirname "$0")/lib.sh"

if [ $# -lt 2 ]; then
    echo "uso: $0 <N> <serviço> [serviço...]" >&2
    exit 1
fi
N=$1; shift

escalas=()
for servico in "$@"; do escalas+=(--scale "$servico=$N"); done

titulo "Escalando $* para $N réplica(s)"
inicio=$(date +%s)
# --no-deps: não mexe nos brokers; --no-recreate: mantém os containers atuais
dc up -d --no-deps --no-recreate "${escalas[@]}" "$@"
esperar "$ESPERA_REACAO_S" "para o rebalanço e novas estatísticas"

titulo "Containers"
listar_containers

titulo "Distribuição das partições entre os processadores"
descrever_grupo

titulo "Rebalanços e carga por processador desde a mudança"
logs_filtrados "$(( $(date +%s) - inicio + 5 ))" 'REBALANCO|ESTATISTICA processador' processador
