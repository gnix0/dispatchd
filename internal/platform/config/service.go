package config

import (
	"os"
	"strconv"
	"time"
)

const defaultShutdownTimeout = 10 * time.Second

type Service struct {
	Name            string
	Environment     string
	LogLevel        string
	GRPCPort        int
	ShutdownTimeout time.Duration
}

func Load(serviceName string) Service {
	return Service{
		Name:            serviceName,
		Environment:     getEnv("APP_ENV", "development"),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		GRPCPort:        getIntEnv("GRPC_PORT", 8080),
		ShutdownTimeout: getDurationEnv("SHUTDOWN_TIMEOUT_SECONDS", defaultShutdownTimeout),
	}
}

func (s Service) GRPCAddress() string {
	return ":" + strconv.Itoa(s.GRPCPort)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	rawValue := os.Getenv(key)
	if rawValue == "" {
		return fallback
	}

	seconds, err := strconv.Atoi(rawValue)
	if err != nil || seconds <= 0 {
		return fallback
	}

	return time.Duration(seconds) * time.Second
}

func getIntEnv(key string, fallback int) int {
	rawValue := os.Getenv(key)
	if rawValue == "" {
		return fallback
	}

	value, err := strconv.Atoi(rawValue)
	if err != nil || value <= 0 {
		return fallback
	}

	return value
}
