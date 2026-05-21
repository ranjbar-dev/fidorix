DEV_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.development.yml
PROD_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.production.yml

.PHONY: dev-up dev-down dev-logs dev-psql prod-up prod-down prod-logs verify-timescale

dev-up:
	$(DEV_COMPOSE) up -d --build

dev-down:
	$(DEV_COMPOSE) down

dev-logs:
	$(DEV_COMPOSE) logs -f postgres

dev-psql:
	# Opens psql inside the dev container using the dedicated app credentials.
	$(DEV_COMPOSE) exec postgres sh -lc 'PGPASSWORD="$$APP_DB_PASSWORD" psql -U "$$APP_DB_USER" -d "$$POSTGRES_DB"'

prod-up:
	$(PROD_COMPOSE) up -d --build

prod-down:
	$(PROD_COMPOSE) down

prod-logs:
	$(PROD_COMPOSE) logs -f postgres

verify-timescale:
	# Confirms the extension is installed and prints its version from the running container.
	$(DEV_COMPOSE) exec postgres sh -lc 'PGPASSWORD="$$APP_DB_PASSWORD" psql -U "$$APP_DB_USER" -d "$$POSTGRES_DB" -tAc "SELECT extname || '' '' || extversion FROM pg_extension WHERE extname = ''timescaledb'';"'
