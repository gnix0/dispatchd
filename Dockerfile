FROM golang:1.26-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN test -n "${SERVICE}"
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
  go build -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder /out/app /app/task-orchestrator

USER nonroot:nonroot

ENTRYPOINT ["/app/task-orchestrator"]
