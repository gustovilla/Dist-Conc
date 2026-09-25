# =====================================================================
# Atalhos para operar o sistema de monitoramento da fábrica.
# Executar em Linux/WSL, na raiz do projeto. "make" sem argumentos lista
# os comandos. Parâmetros: N (réplicas), BROKER (nao-controller, controller
# ou kafka-1..3), MODO (kill|stop).
# =====================================================================
COMPOSE = docker compose
SENSORES = sensor-producao sensor-refrigeracao sensor-empacotamento
N ?= 3
BROKER ?=
MODO ?= kill

.DEFAULT_GOAL := ajuda
.PHONY: ajuda up down limpar ps logs logs-processadores logs-sensores rebalancos \
        alertas status falha-broker recupera-broker falha-consumidor \
        escala-processadores escala-sensores consultar demo salvar-logs relatorio

ajuda: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  make %-22s %s\n", $$1, $$2}'

# ---------------- Ciclo de vida ----------------
up: ## Constrói as imagens e sobe todo o sistema
	$(COMPOSE) up -d --build
	@echo "Sistema no ar. Use 'make status' ou 'make logs'."

down: ## Para e remove os containers (mantém os dados nos volumes)
	$(COMPOSE) down

limpar: ## Para tudo e apaga os volumes (dados do Kafka e do SQLite)
	$(COMPOSE) down -v

ps: ## Lista os containers
	$(COMPOSE) ps -a

# ---------------- Observação ----------------
logs: ## Acompanha os logs de todos os serviços
	$(COMPOSE) logs -f

logs-processadores: ## Acompanha os logs dos processadores
	$(COMPOSE) logs -f processador

logs-sensores: ## Acompanha os logs dos sensores
	$(COMPOSE) logs -f $(SENSORES)

rebalancos: ## Mostra os eventos de rebalanço registrados nos logs
	@$(COMPOSE) logs --no-color processador | grep REBALANCO || true

alertas: ## Mostra os alertas de anomalia registrados nos logs
	@$(COMPOSE) logs --no-color processador | grep ALERTA || true

status: ## Containers, líderes/réplicas do tópico e distribuição das partições
	@bash scripts/status.sh

consultar: ## Relatório do banco SQLite (carga por processador, rebalanços, alertas)
	@bash scripts/consultar.sh

# ---------------- Falhas e elasticidade ----------------
falha-broker: ## Derruba um broker (BROKER=nao-controller|controller|kafka-N)
	@bash scripts/falha_broker.sh $(BROKER)

recupera-broker: ## Religa os brokers parados (ou BROKER=kafka-N)
	@bash scripts/recupera_broker.sh $(BROKER)

falha-consumidor: ## Derruba um processador (MODO=kill|stop)
	@bash scripts/falha_consumidor.sh $(MODO)

escala-processadores: ## Muda o número de processadores (N=6)
	@bash scripts/escala.sh $(N) processador

escala-sensores: ## Muda o número de containers de sensor por setor (N=3)
	@bash scripts/escala.sh $(N) $(SENSORES)

# ---------------- Evidências ----------------
demo: ## Roteiro completo de testes; saída gravada em logs/demo-<data>.log
	@bash scripts/demo.sh

salvar-logs: ## Salva os logs completos de cada serviço em logs/
	@bash scripts/salvar_logs.sh

# ---------------- Relatório ----------------
TEXLIVE = texlive/texlive:latest

relatorio: ## Compila relatorio/relatorio.tex em PDF (usa a imagem Docker do TeX Live)
	docker run --rm -v "$(CURDIR)/relatorio:/doc" -w /doc $(TEXLIVE) \
		sh -c "pdflatex -interaction=nonstopmode relatorio.tex && pdflatex -interaction=nonstopmode relatorio.tex"
