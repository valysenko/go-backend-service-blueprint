# Go Backend Service Blueprint

A reference Go project demonstrating layered architecture and common backend patterns:

- **HTTP** — Fiber API, PostgreSQL, Redis, Swagger and a user example.
- **Kafka** — user and event consumers, schema validation, retries and dead-letter topics.
- **RabbitMQ** — multiple queues and workers, retries, dead-letter queues and delayed messages.
- **Cron** — scheduled operations with keyset pagination and transactional row locking.

## Structure

```text
cmd/                      Application entry points
config/                   Environment configuration
internal/domain/          Entities and domain interfaces
internal/app/             Use cases and ports
internal/infrastructure/  Database, messaging and monitoring adapters
internal/presentation/    HTTP and message handlers
internal/testutils/       Testcontainers helpers
pkg/                      Reusable infrastructure packages
deployments/              Docker Compose and service configuration
```

Handlers call use cases; use cases depend on ports implemented by infrastructure adapters. `.go-arch-lint.yml` checks the dependency boundaries.

## Run locally

Requires Docker Compose; Python 3 is needed for Kafka schema registration. `make up` creates `deployments/.env` from `.env.dist` if absent.

```sh
make up                     # HTTP, PostgreSQL and Redis
make up-kafka               # Kafka, Schema Registry and consumers
make bootstrap-kafka-topics  # run after Kafka starts
make register-user-schema   # run after Schema Registry starts
make up-rabbitmq            # RabbitMQ and consumer
make up-monitoring          # Prometheus and Pushgateway
make cron                   # run the operations cron once
```

| Service | Address |
| --- | --- |
| HTTP API / Swagger | http://localhost:8096 / http://localhost:8096/swagger/index.html |
| Kafka user / events monitoring | http://localhost:8097 / http://localhost:8098 |
| RabbitMQ consumer monitoring | http://localhost:8099 |
| Kafka console / Schema Registry | http://localhost:18080 / http://localhost:18081 |
| RabbitMQ management | http://localhost:15696 (`blueprint` / `blueprint`) |
| Prometheus / Pushgateway | http://localhost:9090 / http://localhost:9091 |

The HTTP, Kafka and RabbitMQ apps expose `/healthcheck` and `/metrics`. Request IDs propagate through context and message headers; the cron pushes its latest run metrics to Pushgateway. OpenTelemetry tracing is not configured.

The RabbitMQ app uses Redis to deduplicate messages by queue and `MessageId` for 24 hours; producers must supply a stable, non-empty `MessageId`. Kafka uses its `id` header, falling back to a hash of the key and body, for deduplication. Both examples derive stable IDs for outgoing events so downstream consumers can deduplicate retries. Concurrent duplicates may still be processed simultaneously.


## Tests and development

```sh
make test               # short suite inside the app container
make test-integration   # full suite on the host with Testcontainers
make lint
make arch-lint
make mocks
```

Host tests require Go 1.26.2 and Docker. Kafka E2E tests use a schema-registry HTTP stub serving the committed schema.
