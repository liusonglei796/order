.PHONY: all help build wire test clean run

all: build

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

wire: ## Generate Google Wire dependency injection code
	go run github.com/google/wire/cmd/wire ./cmd/order

build: ## Compile the order microservice into bin/
	@if not exist bin mkdir bin
	go build -trimpath -o bin/order.exe ./cmd/order

test: ## Run all tests across the repository
	go test -v ./...

run: ## Run order service with default configuration
	go run ./cmd/order -conf configs/config.yaml

clean: ## Remove compiled binaries
	powershell -Command "Remove-Item -Force -Recurse bin\*.exe -ErrorAction SilentlyContinue"
