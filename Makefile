DEV_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.development.yml
PROD_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.production.yml

.PHONY: dev-up dev-down dev-logs dev-psql dev-redis-cli dev-redis-logs prod-up prod-down prod-logs prod-redis-logs verify-timescale verify-redis flush-redis-dev

dev-up:
	$(DEV_COMPOSE) up -d --build

dev-down:
	$(DEV_COMPOSE) down

dev-logs:
	$(DEV_COMPOSE) logs -f postgres

dev-psql:
	# Opens psql inside the dev container using the dedicated app credentials.
	$(DEV_COMPOSE) exec postgres sh -lc 'PGPASSWORD="$$APP_DB_PASSWORD" psql -U "$$APP_DB_USER" -d "$$POSTGRES_DB"'

dev-redis-cli:
	# Opens redis-cli inside the dev container and authenticates with local env credentials.
	$(DEV_COMPOSE) exec redis sh -lc 'redis-cli -a "$$REDIS_PASSWORD"'

dev-redis-logs:
	$(DEV_COMPOSE) logs -f redis

prod-up:
	$(PROD_COMPOSE) up -d --build

prod-down:
	$(PROD_COMPOSE) down

prod-logs:
	$(PROD_COMPOSE) logs -f postgres

prod-redis-logs:
	$(PROD_COMPOSE) logs -f redis

verify-timescale:
	# Confirms the extension is installed and prints its version from the running container.
	$(DEV_COMPOSE) exec postgres sh -lc 'PGPASSWORD="$$APP_DB_PASSWORD" psql -U "$$APP_DB_USER" -d "$$POSTGRES_DB" -tAc "SELECT extname || '' '' || extversion FROM pg_extension WHERE extname = ''timescaledb'';"'

verify-redis:
	# Confirms Redis responds to PING, prints version, and shows memory summary.
	$(DEV_COMPOSE) exec redis sh -lc 'redis-cli -a "$$REDIS_PASSWORD" PING && redis-cli -a "$$REDIS_PASSWORD" INFO server | grep redis_version && redis-cli -a "$$REDIS_PASSWORD" INFO memory'

flush-redis-dev:
	@read -p "This will run FLUSHALL on development Redis. Continue? [y/N] " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		$(DEV_COMPOSE) exec redis sh -lc 'redis-cli -a "$$REDIS_PASSWORD" FLUSHALL'; \
		echo "Development Redis flushed."; \
	else \
		echo "Aborted."; \
	fi
