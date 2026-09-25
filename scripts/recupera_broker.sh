#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Religa broker(s) derrubado(s) e mostra que eles voltam a ser réplicas
# em sincronia (Isr) depois de copiar o que perderam dos outros brokers.
# Uso: bash scripts/recupera_broker.sh [broker]
#   sem argumento: religa todos os brokers que não estão em execução
# ---------------------------------------------------------------------
source "$(dirname "$0")/lib.sh"

if [ $# -ge 1 ] && [ -n "$1" ]; then
    parados=("$1")
else
    parados=()
    for b in "${BROKERS[@]}"; do
        if [ -z "$(dc ps -q --status running "$b")" ]; then parados+=("$b"); fi
    done
fi
if [ ${#parados[@]} -eq 0 ]; then
    echo "todos os brokers já estão em execução"
    exit 0
fi

titulo "Religando ${parados[*]}"
dc start "${parados[@]}"
esperar "$ESPERA_REACAO_S" "para o(s) broker(s) sincronizar(em) as réplicas"

titulo "Depois da recuperação: ${parados[*]} de volta ao Isr"
listar_containers
descrever_topico
