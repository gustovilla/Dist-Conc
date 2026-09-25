#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Funções comuns aos scripts de operação e de simulação de falhas.
# Uso: source "$(dirname "$0")/lib.sh"
# ---------------------------------------------------------------------
set -euo pipefail

# Executa sempre a partir da raiz do projeto e carrega a configuração
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RAIZ"
set -a
# shellcheck source=../.env
source .env
set +a

SENSORES=(sensor-producao sensor-refrigeracao sensor-empacotamento)
BROKERS=(kafka-1 kafka-2 kafka-3)

# Atalho para o docker compose
dc() { docker compose "$@"; }

# Cabeçalho com horário, para os logs de evidência
titulo() { printf '\n========== [%s] %s ==========\n' "$(date '+%H:%M:%S')" "$*"; }

# Espera N segundos avisando o motivo
esperar() { echo "... aguardando ${1}s ${2:-}"; sleep "$1"; }

# Nome do primeiro broker em execução (para rodar as ferramentas do Kafka)
broker_ativo() {
    local b
    for b in "${BROKERS[@]}"; do
        if [ -n "$(dc ps -q --status running "$b")" ]; then echo "$b"; return 0; fi
    done
    echo "nenhum broker em execução" >&2
    return 1
}

# Executa uma ferramenta do Kafka (kafka-topics.sh, ...) em um broker ativo
kafka_cmd() {
    local ferramenta=$1; shift
    # < /dev/null: a ferramenta não lê do terminal (evita consumir a entrada do script)
    dc exec -T -e KAFKA_HEAP_OPTS="$KAFKA_FERRAMENTAS_HEAP_OPTS" "$(broker_ativo)" \
        "/opt/kafka/bin/$ferramenta" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@" < /dev/null
}

# Id do controller ativo (líder do quórum KRaft), ex.: 3
controller_ativo() {
    kafka_cmd kafka-metadata-quorum.sh describe --status 2>/dev/null | awk '/LeaderId/ {print $2}'
}

# Traduz "controller" / "nao-controller" para o nome de um broker; outros
# valores (ex.: kafka-2) são devolvidos sem alteração
resolver_broker() {
    local c
    case "$1" in
        controller)     echo "kafka-$(controller_ativo)" ;;
        nao-controller) c=$(controller_ativo); echo "kafka-$(( c % ${#BROKERS[@]} + 1 ))" ;;
        *)              echo "$1" ;;
    esac
}

# Líder, réplicas e réplicas em sincronia (ISR) de cada partição do tópico
descrever_topico() { kafka_cmd kafka-topics.sh --describe --topic "$TOPICO"; }

# Qual processador (CLIENT-ID) lê cada partição, offsets e atraso (LAG)
descrever_grupo() { kafka_cmd kafka-consumer-groups.sh --describe --group "$GRUPO_CONSUMO" || true; }

# Estado dos containers
listar_containers() { dc ps -a --format 'table {{.Name}}\t{{.State}}\t{{.Status}}'; }

# Linhas relevantes dos logs dos últimos N segundos
# Uso: logs_filtrados <segundos> <regex> <serviços...>
logs_filtrados() {
    local desde=$1 filtro=$2; shift 2
    dc logs --no-color --since "${desde}s" "$@" | grep -E "$filtro" || true
}
