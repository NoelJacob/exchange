SHELL := /bin/bash
PASS := admin123
API := http://localhost:8080
ROOT := $(shell pwd)

.PHONY: infra build build-wrong build-all runner start stop clean restart \
        admin-create admin-config submit status leaderboard events logs e2e

infra:       ## Start infrastructure (QuestDB, Redis, Redpanda, MinIO)
	docker compose -f infra/docker-compose.yml up -d questdb valkey redpanda minio

build:       ## Build correct contestant binary
	cargo build --release -p contestant-sample

build-wrong: ## Build prefilled (wrong) contestant binary to /tmp/contestant-sample-wrong
	cargo build --release -p contestant-sample --features prefilled && \
	cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-wrong

build-all:   ## Build all services + both contestant variants
	cargo build --release -p contestant-sample
	cargo build --release -p contestant-sample --features prefilled && \
	cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-wrong
	cargo build --release -p telemetry-ingester
	cargo build --release -p bot-worker

runner:      ## Build runner Docker image
	docker compose -f infra/docker-compose.yml build runner

start:       ## Build + start everything: infra services + API + ingester
	docker compose -f infra/docker-compose.yml build platform-api telemetry-ingester
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

e2e:         ## Run full e2e pipeline (requires Docker, all builds done)
	@echo "=== Starting e2e ==="
	$(MAKE) infra
	@sleep 15
	$(MAKE) runner
	$(MAKE) start
	@sleep 10
	$(MAKE) submit NAME="Correct" FILE="contestant-sample/target/release/contestant-sample" RPS=30 DURATION=25
	$(MAKE) submit NAME="Wrong" FILE="/tmp/contestant-sample-wrong" RPS=30 DURATION=25
	@echo "=== Waiting for tests to complete (90s) ==="
	@sleep 90
	$(MAKE) leaderboard
	@echo ""
	@echo "=== Correctness Gap Check ==="
	@C=$$(curl -s "$(API)/api/contestant/status" -H "Authorization: Bearer $$(jq -r .jwt /tmp/jwt-Correct.json)" | jq '.metrics.correctness_pct'); \
	W=$$(curl -s "$(API)/api/contestant/status" -H "Authorization: Bearer $$(jq -r .jwt /tmp/jwt-Wrong.json)" | jq '.metrics.correctness_pct'); \
	: $${C:=0} $${W:=0}; \
	BETTER=$$(echo "if ($$C > $$W) 1 else 0" | bc 2>/dev/null || python3 -c "print(1 if $$C > $$W else 0)"); \
	if [ "$$BETTER" = "1" ]; then \
		echo "PASS: Correct contestant has higher correctness_pct ($$C% > $$W%)"; \
	else \
		echo "FAIL: Expected Correct > Wrong, got $$C% vs $$W%"; \
		exit 1; \
	fi
	@echo ""
	@echo "=== Composite Score Check ==="
	@LB=$$(curl -sf "$(API)/api/leaderboard" | jq -c '.leaderboard // []'); \
	ALICE=$$(echo "$$LB" | jq ".[] | select(.name==\"Correct\")"); \
	if [ -n "$$ALICE" ]; then \
		A_COMPOSITE=$$(echo "$$ALICE" | jq '.composite // -1'); \
		A_TPS=$$(echo "$$ALICE" | jq '.current_tps // 0'); \
		echo "Correct: composite=$$A_COMPOSITE tps=$$A_TPS"; \
		if [ "$$(echo "$$A_COMPOSITE > 0" | bc 2>/dev/null || python3 -c "print(1 if $$A_COMPOSITE > 0 else 0)")" = "1" ]; then \
			echo "PASS: composite > 0"; \
		else \
			echo "FAIL: composite <= 0"; \
			exit 1; \
		fi; \
		if [ "$$(echo "$$A_TPS > 0" | bc 2>/dev/null || python3 -c "print(1 if $$A_TPS > 0 else 0)")" = "1" ]; then \
			echo "PASS: TPS > 0"; \
		else \
			echo "FAIL: TPS <= 0"; \
			exit 1; \
		fi; \
	else \
		echo "FAIL: Correct contestant not in leaderboard"; \
		exit 1; \
	fi

clean:       ## Remove all containers + volumes
	docker compose -f infra/docker-compose.yml down -v

help:        ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
