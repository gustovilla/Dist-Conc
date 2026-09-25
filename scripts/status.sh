#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Mostra o estado do sistema: containers, tópico (líderes/ISR) e grupo de
# consumo (qual processador lê cada partição e o atraso de cada uma).
# Uso: bash scripts/status.sh
# ---------------------------------------------------------------------
source "$(dirname "$0")/lib.sh"

titulo "Containers"
listar_containers

titulo "Tópico $TOPICO: líder e réplicas em sincronia (Isr) por partição"
descrever_topico

titulo "Grupo $GRUPO_CONSUMO: processador (CLIENT-ID) responsável por cada partição"
descrever_grupo
