.PHONY: up down build swarm-deploy swarm-ps swarm-rm swarm-logs logs clean contestant bot-spawn

# ── Local (Docker Compose) ──────────────────────────────
up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build bot-worker telemetry-ingester submission-api portal

logs:
	docker compose logs -f

# ── Cloud (Docker Swarm) ────────────────────────────────
swarm-deploy:
	docker stack deploy -c docker-compose.yml platform

swarm-rm:
	docker stack rm platform

swarm-ps:
	docker stack ps platform

swarm-logs:
	docker service logs --follow platform_bot-worker

# ── Operations ──────────────────────────────────────────
contestant:
	curl -s -X POST http://localhost:3000/api/admin/contestant \
		-H "Content-Type: application/json" \
		-d '{"name":"$(NAME)"}' | jq .

bot-spawn:
	docker compose run -d --rm bot-worker \
		-e BOT_ID=$(ID) \
		-e CONTESTANT_HOST=$(TARGET) \
		-e TARGET_RPS=$(RPS)

clean:
	docker compose down -v
	docker system prune -f
