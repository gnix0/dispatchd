FROM golang:1.25.7-bookworm

ARG BUF_VERSION=1.57.0

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates git protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

RUN GOBIN=/usr/local/bin go install github.com/bufbuild/buf/cmd/buf@v${BUF_VERSION} \
  && GOBIN=/usr/local/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6 \
  && GOBIN=/usr/local/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

WORKDIR /workspace
