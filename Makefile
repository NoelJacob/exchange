SHELL := /bin/bash
PASS := admin123
API := http://localhost:8080
ROOT := $(shell pwd)

.PHONY: infra build build-wrong runner build-local start start-local stop restart \
        admin-create admin-config submit status leaderboard events logs e2e e2e-full clean help \
        build-panic build-slow

start:        ## Build + start everything: infra services + API + ingester
	docker compose -f infra/docker-compose.yml build platform-api telemetry-ingester bot-worker runner
	docker compose -f infra/docker-compose.yml up -d questdb valkey redpanda minio platform-api telemetry-ingester

start-local:  ## Build binaries locally, run everything in Docker via Dockerfile.local
	scripts/build-local.sh
	docker compose -f infra/docker-compose.yml -f infra/docker-compose.local.yml build platform-api telemetry-ingester bot-worker runner
	docker compose -f infra/docker-compose.yml up -d questdb valkey redpanda minio platform-api telemetry-ingester

stop:        ## Stop all services
	docker compose -f infra/docker-compose.yml down

restart: stop start

admin-create: ## Create contestant: make admin-create NAME="Alice"
	curl -s -X POST "$(API)/api/admin/register" \
		-H "X-Admin-Password: $(PASS)" \
		-H "Content-Type: application/json" \
		-d '{"name":"$(NAME)"}' | tee /tmp/jwt-$(NAME).json

admin-config: ## Get admin config
	curl -s "$(API)/api/admin/config" | jq .

submit:      ## Submit binary: make submit NAME="Alice" FILE="/path/to/binary"
	JWT=$$(jq -r .jwt /tmp/jwt-$(NAME).json); \
	curl -s -X POST "$(API)/api/contestant/submit" \
		-H "Authorization: Bearer $$JWT" \
		-F "binary=@$(FILE)" | tee /tmp/run-$(NAME).json

status:      ## Check status: make status NAME="Alice"
	JWT=$$(jq -r .jwt /tmp/jwt-$(NAME).json); \
	curl -s "$(API)/api/contestant/status" \
		-H "Authorization: Bearer $$JWT" | jq .

leaderboard: ## Show leaderboard
	curl -s "$(API)/api/leaderboard" | jq .

events:      ## Watch SSE events (Ctrl+C to stop)
	timeout 15 curl -sN "$(API)/api/events" || true

logs:        ## View platform-api logs
	docker compose -f infra/docker-compose.yml logs --tail=50 -f platform-api

e2e:         ## Run full e2e (requires API running + infra)
	@echo "=== Building contestant variants ==="
	scripts/build-local.sh
	@echo "=== Running Rust E2E test ==="
	cd platform-api && cargo test --test e2e_full -- --nocapture 2>&1
	@echo "=== e2e complete ==="

e2e-full:     ## Build → validate (run `make clean` first if needed)
	$(MAKE) start-local
	$(MAKE) e2e

clean:       ## Remove all containers + volumes
	docker compose -f infra/docker-compose.yml down -v

help:        ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
