#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Roteiro completo de demonstração. Executa, em sequência, todos os testes
# pedidos no enunciado e grava toda a saída em logs/demo-<data>.log:
#
#   1. estado inicial
#   2. falha de um broker comum        -> sistema continua sem interrupção
#   3. recuperação do broker
#   4. falha do broker controller      -> novo controller eleito; sistema continua
#   5. recuperação do broker
#   6. falha de um processador         -> rebalanço
#   7. elasticidade: 6 processadores (1 partição cada)
#   8. 8 processadores (> partições: 2 ficam ociosos)
#   9. redução para 2 processadores
#  10. aumento de carga: 3 containers de sensor por setor
#  11. relatório do banco
#  12. volta à configuração inicial
#
# Pré-requisito: sistema no ar (make up).
# Uso: bash scripts/demo.sh
# ---------------------------------------------------------------------
source "$(dirname "$0")/lib.sh"

if [ -z "$(dc ps -q --status running processador)" ]; then
    echo "O sistema não está no ar. Rode 'make up' antes." >&2
    exit 1
fi

mkdir -p logs
ARQUIVO="logs/demo-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$ARQUIVO") 2>&1
echo "Demonstração iniciada em $(date). Saída gravada em $ARQUIVO"

titulo "1. ESTADO INICIAL"
bash scripts/status.sh
esperar "$ESPERA_REACAO_S" "para acumular estatísticas"
logs_filtrados "$ESPERA_REACAO_S" 'ESTATISTICA' "${SENSORES[@]}" processador

titulo "2. FALHA DE BROKER COMUM (NÃO CONTROLLER)"
bash scripts/falha_broker.sh nao-controller

titulo "3. RECUPERAÇÃO DO BROKER"
bash scripts/recupera_broker.sh

titulo "4. FALHA DO BROKER CONTROLLER"
bash scripts/falha_broker.sh controller

titulo "5. RECUPERAÇÃO DO BROKER"
bash scripts/recupera_broker.sh

titulo "6. FALHA DE PROCESSADOR (queda abrupta)"
bash scripts/falha_consumidor.sh kill

titulo "7. ELASTICIDADE: 6 PROCESSADORES"
bash scripts/escala.sh 6 processador

titulo "8. ELASTICIDADE: 8 PROCESSADORES (MAIS QUE PARTIÇÕES)"
bash scripts/escala.sh 8 processador

titulo "9. ELASTICIDADE: REDUÇÃO PARA 2 PROCESSADORES"
bash scripts/escala.sh 2 processador

titulo "10. CARGA: 3 CONTAINERS DE SENSOR POR SETOR"
bash scripts/escala.sh 3 "${SENSORES[@]}"

titulo "11. RELATÓRIO DO BANCO"
bash scripts/consultar.sh

titulo "12. VOLTA À CONFIGURAÇÃO INICIAL"
bash scripts/escala.sh 1 "${SENSORES[@]}"
bash scripts/escala.sh "$PROCESSADOR_REPLICAS" processador

echo
echo "Demonstração concluída. Saída completa em $ARQUIVO"
