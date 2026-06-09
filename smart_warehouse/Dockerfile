FROM golang:1.25-bookworm AS build

ARG APP
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -o /out/service ./cmd/${APP}

FROM debian:bookworm-slim

WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/service /app/service
COPY schema /app/schema
COPY examples /app/examples

ENTRYPOINT ["/app/service"]
