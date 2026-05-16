SHELL := /bin/bash

.PHONY: help build up down restart logs ps shell health stats watch-health \
	tail-app tail-events backup-session restore-session \
	test lint format ci dev smoke inspector ci-status

.DEFAULT_GOAL := help

PORT ?= 3003
HOST ?= http://localhost:$(PORT)
SESSIONS_VOLUME := whatsapp-mcp-stream_whatsapp-sessions

help: ## Show available targets
	@awk 'BEGIN{FS=":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# --- Container lifecycle (named volumes preserved) ---

build: ## docker compose build
	docker compose build

up: ## docker compose up -d
	docker compose up -d

down: ## docker compose down (keeps whatsapp-sessions / logs / media volumes)
	docker compose down

restart: ## Rebuild image and restart container (auth survives)
	docker compose build
	docker compose up -d

logs: ## Follow combined container logs (stdout + stderr)
	docker compose logs -f mcp-whatsapp

ps: ## Show container status
	docker compose ps

shell: ## Open a shell inside the running container
	docker compose exec mcp-whatsapp sh

# --- Diagnostics ---

health: ## Probe /healthz on the host-published port
	@curl -s $(HOST)/healthz | (command -v python3 >/dev/null && python3 -m json.tool || cat)

stats: ## Probe /api/status (auth + recovery + sync stats)
	@curl -s $(HOST)/api/status | (command -v python3 >/dev/null && python3 -m json.tool || cat)

watch-health: ## Poll /healthz every 5s (Ctrl-C to stop)
	@while true; do clear; date; curl -s $(HOST)/healthz | (command -v python3 >/dev/null && python3 -m json.tool || cat); sleep 5; done

tail-app: ## Tail /app/logs/mcp-whatsapp.log inside the container
	docker compose exec mcp-whatsapp tail -F /app/logs/mcp-whatsapp.log

tail-events: ## Tail /app/logs/wa-events.log (requires WA_EVENT_STREAM=1)
	docker compose exec mcp-whatsapp tail -F /app/logs/wa-events.log

# --- Session backup / restore ---
# Backup: tars the whatsapp-sessions volume to a host file.
# Restore: untars FILE=... back into the same volume. WIPES current session data.

backup-session: ## Tar the auth session volume to session-YYYYMMDD-HHMMSS.tar.gz
	@OUT=session-$$(date +%Y%m%d-%H%M%S).tar.gz; \
	docker run --rm \
	  -v $(SESSIONS_VOLUME):/data:ro \
	  -v $$PWD:/backup \
	  alpine sh -c "cd /data && tar czf /backup/$$OUT ." && \
	echo "Wrote $$OUT ($$(du -h $$OUT | cut -f1))"

restore-session: ## Restore session from FILE=path/to/session.tar.gz (stops/starts container)
	@if [ -z "$$FILE" ]; then echo "Usage: make restore-session FILE=session-...tar.gz"; exit 1; fi
	@if [ ! -f "$$FILE" ]; then echo "Not found: $$FILE"; exit 1; fi
	@echo "About to wipe $(SESSIONS_VOLUME) and restore from $$FILE"
	@read -p "Type 'yes' to confirm: " ans && [ "$$ans" = "yes" ] || (echo aborted; exit 1)
	docker compose down
	docker run --rm \
	  -v $(SESSIONS_VOLUME):/data \
	  -v $$PWD:/backup \
	  alpine sh -c "rm -rf /data/* /data/..?* /data/.[!.]* 2>/dev/null; tar xzf /backup/$$FILE -C /data"
	docker compose up -d

# --- Local dev helpers ---

test: ## Run vitest suite
	npm test

lint: ## Run eslint
	npm run lint

format: ## Run prettier --write on src
	npm run format

ci: ## Mirror CI locally: clean install + build + lint + format:check + test
	npm ci
	npm run build
	npm run lint
	npm run format:check
	npm test

dev: ## Local dev server (tsc -w + node --watch)
	npm run dev

inspector: ## Launch MCP inspector against the local dist build
	npm run debug

smoke: ## Smoke test MCP against $(HOST)
	MCP_BASE_URL=$(HOST) npm run smoke:mcp

# --- GitHub ---

ci-status: ## Show conclusion of the latest CI run on main
	@curl -s "https://api.github.com/repos/loglux/whatsapp-mcp-stream/actions/runs?per_page=1" | \
	  python3 -c "import json,sys; r=json.load(sys.stdin)['workflow_runs'][0]; print(f\"{r['display_title'][:60]:60} | {r['status']:10} | {r['conclusion'] or '-'} | {r['head_sha'][:8]}\")"
