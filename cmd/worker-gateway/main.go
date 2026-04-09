package main

import (
	"log/slog"
	"os"

	"github.com/gnix0/task-orchestrator/internal/platform/config"
	runtimeapp "github.com/gnix0/task-orchestrator/internal/platform/runtime"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := config.Load("worker-gateway")

	runtimeapp.Run(logger, serviceConfig)
}
