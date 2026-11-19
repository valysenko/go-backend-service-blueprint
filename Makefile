PROJECT_NAME=go-backend-service-blueprint
APP_CONTAINER_NAME=blueprint-app
DLV=dlv debug --headless --listen=:40000 --api-version=2 --log --accept-multiclient
BASE_COMPOSE_PATH=./deployments/docker-compose.yml
MONITORING_COMPOSE_PATH=./deployments/docker-compose-monitoring.yml
COMPOSE=docker compose -p $(PROJECT_NAME) -f $(BASE_COMPOSE_PATH)

.PHONY: copy-env build up down exec logs cron up-monitoring lint arch-lint mocks swag test test-verbose test-integration debug-http up-kafka bootstrap-kafka-topics register-user-schema logs-kafka-user logs-kafka-events debug-kafka-user debug-kafka-events


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

cron:
	docker exec -i $(APP_CONTAINER_NAME) sh -c "cd /src && go run ./cmd/operations-cron"

debug-http:
	$(COMPOSE) stop app
	$(COMPOSE) run --rm --service-ports app sh -c "cd /src && $(DLV) ./cmd/http"
	$(COMPOSE) start app

# monitoring
up-monitoring: copy-env
	docker compose -p $(PROJECT_NAME) -f $(BASE_COMPOSE_PATH) -f $(MONITORING_COMPOSE_PATH) up -d

# quality gates
lint:
	docker exec -i $(APP_CONTAINER_NAME) golangci-lint run

arch-lint:
	docker exec -i $(APP_CONTAINER_NAME) go run github.com/fe3dback/go-arch-lint@v1.17.0 check

# The app container has no docker socket, so the suite runs there with -short:
# integration tests spin up their own postgres and therefore run on the host.
test:
	docker exec -i $(APP_CONTAINER_NAME) go test ./... -count=1 -short

test-verbose:
	docker exec -i $(APP_CONTAINER_NAME) go test -v ./... -count=1 -short

test-integration:
	go test ./... -count=1

mocks:
	docker exec -i $(APP_CONTAINER_NAME) sh -c "cd /src && GOTOOLCHAIN=go1.26.2 go run github.com/vektra/mockery/v3@v3.7.2"

swag:
	docker exec -i $(APP_CONTAINER_NAME) go run github.com/swaggo/swag/cmd/swag@v1.16.6 init -g cmd/http/main.go --parseInternal -o docs

KAFKA_COMPOSE=docker compose -p $(PROJECT_NAME) -f $(BASE_COMPOSE_PATH) -f ./deployments/docker-compose-kafka.yml -f ./deployments/docker-compose-kafka-consumers.yml

up-kafka: copy-env
	$(KAFKA_COMPOSE) up -d

bootstrap-kafka-topics:
	./deployments/bootstrap-kafka-topics.sh

register-user-schema:
	python3 ./deployments/kafka/register-user-schema.py

logs-kafka-user:
	$(KAFKA_COMPOSE) logs -f kafka-user

logs-kafka-events:
	$(KAFKA_COMPOSE) logs -f kafka-events

debug-kafka-user:
	$(KAFKA_COMPOSE) stop kafka-user
	$(KAFKA_COMPOSE) run --rm --service-ports kafka-user sh -c "cd /src && $(DLV) ./cmd/kafka-user"
	$(KAFKA_COMPOSE) start kafka-user

debug-kafka-events:
	$(KAFKA_COMPOSE) stop kafka-events
	$(KAFKA_COMPOSE) run --rm --service-ports kafka-events sh -c "cd /src && $(DLV) ./cmd/kafka-events"
	$(KAFKA_COMPOSE) start kafka-events
