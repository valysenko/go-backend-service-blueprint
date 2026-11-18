PROJECT_NAME=go-backend-service-blueprint
APP_CONTAINER_NAME=blueprint-app
BASE_COMPOSE_PATH=./deployments/docker-compose.yml
COMPOSE=docker compose -p $(PROJECT_NAME) -f $(BASE_COMPOSE_PATH)

.PHONY: copy-env build up down exec logs


copy-env:
	@cp -n deployments/.env.dist deployments/.env 2>/dev/null || true


# golang app
build: copy-env
	$(COMPOSE) build --no-cache --force-rm
	$(COMPOSE) up --remove-orphans --force-recreate -d

up: copy-env
	$(COMPOSE) up --no-recreate -d

down:
	$(COMPOSE) down --remove-orphans

exec:
	docker exec -it $(APP_CONTAINER_NAME) sh

logs:
	docker logs -f $(APP_CONTAINER_NAME)
