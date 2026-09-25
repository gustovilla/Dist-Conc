#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Derruba um processador e mostra o rebalanço: as partições dele são
# redistribuídas entre os processadores restantes.
#
# Modos:
#   kill  - SIGKILL, queda sem aviso. O coordenador só percebe depois de
#           SESSION_TIMEOUT_MS sem heartbeat.
#   stop  - SIGTERM, saída ordenada. O processador sai do grupo e o
#           rebalanço é imediato.
# Uso: bash scripts/falha_consumidor.sh [kill|stop]   (padrão: kill)
# ---------------------------------------------------------------------
source "$(dirname "$0")/lib.sh"
MODO=${1:-kill}

alvo=$(dc ps -q --status running processador | head -n 1)
if [ -z "$alvo" ]; then
    echo "nenhum processador em execução" >&2
    exit 1
fi
nome=$(docker inspect -f '{{.Name}}' "$alvo" | sed 's#^/##')
hostname=$(docker inspect -f '{{.Config.Hostname}}' "$alvo")

titulo "Antes da falha: distribuição das partições"
descrever_grupo

titulo "Derrubando $nome (id nos logs: $hostname) com docker $MODO"
inicio=$(date +%s)
docker "$MODO" "$alvo" > /dev/null
if [ "$MODO" = "kill" ]; then
    echo "O coordenador do grupo só detecta a queda após session.timeout.ms=${SESSION_TIMEOUT_MS}ms"
fi
esperar "$ESPERA_REACAO_S" "para o rebalanço"

titulo "Depois da falha: partições de $hostname assumidas pelos outros processadores"
descrever_grupo

titulo "Eventos de rebalanço registrados nos logs"
logs_filtrados "$(( $(date +%s) - inicio + 5 ))" 'REBALANCO' processador
