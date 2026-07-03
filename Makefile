COMPOSE  := docker compose -f infra/docker-compose.yml
COMPOSE_LOCAL := docker compose -f infra/docker-compose.yml -f infra/docker-compose.local.yml

.PHONY: start start-local stop restart restart-local clean help

start:
	$(COMPOSE) build exchange
	$(COMPOSE) up -d exchange

start-local:
	$(COMPOSE_LOCAL) build exchange
	$(COMPOSE_LOCAL) up -d exchange

stop:
	$(COMPOSE) down

restart: stop start

restart-local: stop start-local

clean: ## Remove all containers + volumes
	$(COMPOSE) down -v
