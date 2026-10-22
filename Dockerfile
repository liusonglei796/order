# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/order ./cmd/order

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S appgroup && adduser -S appuser -G appgroup

USER appuser
WORKDIR /app

COPY --from=builder /bin/order /bin/order
COPY configs/config.yaml /app/configs/config.yaml

EXPOSE 50054

ENTRYPOINT ["/bin/order", "-conf", "/app/configs/config.yaml"]
