#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Relatório do banco SQLite (leituras por processador/partição,
# rebalanços e alertas), executado em um container avulso que monta o
# mesmo volume dos processadores.
# Uso: bash scripts/consultar.sh [linhas]
# ---------------------------------------------------------------------
source "$(dirname "$0")/lib.sh"

dc run --rm --no-deps processador python consultar.py "$@"
