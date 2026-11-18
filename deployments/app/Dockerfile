FROM golang:1.26.2-alpine3.23 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
RUN go mod verify

COPY . .

# production env - 1 stage
FROM builder AS prod
RUN apk add --no-cache ca-certificates
RUN CGO_ENABLED=0 GOOS=linux go build -o /src/bin/ ./cmd/...

# development env - 1 stage
FROM builder AS dev
RUN go install github.com/air-verse/air@v1.65.1
RUN go install github.com/go-delve/delve/cmd/dlv@v1.26.1
RUN set -eux; \
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4; \
    golangci-lint --version

# production env - 2 stage
FROM scratch AS final-prod
COPY --from=prod /src/bin/ /
# Migrations ship with the image
COPY --from=prod /src/internal/infrastructure/postgres/migrations /migrations
COPY --from=prod /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# no CMD — the entrypoint is chosen per deployment (one image, several binaries)

# development env - 2 stage
FROM golang:1.26.2-alpine3.23 AS final-dev
RUN apk add --no-cache gcc musl-dev curl
WORKDIR /app
COPY --from=dev /src /app
COPY --from=dev /go/bin/air /usr/local/bin/air
COPY --from=dev /go/bin/dlv /usr/local/bin/dlv
COPY --from=dev /go/bin/golangci-lint /usr/local/bin/golangci-lint
CMD ["air"]
