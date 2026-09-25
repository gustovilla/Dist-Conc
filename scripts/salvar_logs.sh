#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Salva os logs completos de todos os serviços em logs/, um arquivo por
# serviço, com data e hora no nome.
# Uso: bash scripts/salvar_logs.sh
# ---------------------------------------------------------------------
source "$(dirname "$0")/lib.sh"

destino="logs/completo-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$destino"
for servico in $(dc config --services); do
    dc logs --no-color --timestamps "$servico" > "$destino/$servico.log" 2>&1 || true
done
echo "logs salvos em $destino/"
ls -l "$destino"
