SHELL := /bin/sh

.PHONY: fmt fmt-check lint test build proto proto-check

fmt:
	go fmt ./...

fmt-check:
	./scripts/check-gofmt.sh

lint:
	golangci-lint run ./...

test:
	go test ./...

build:
	go build ./...

proto:
	./scripts/proto.sh generate

proto-check:
	./scripts/proto.sh check
