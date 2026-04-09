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
	ShutdownTimeout time.Duration
}

func Load(serviceName string) Service {
	return Service{
		Name:            serviceName,
		Environment:     getEnv("APP_ENV", "development"),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		ShutdownTimeout: getDurationEnv("SHUTDOWN_TIMEOUT_SECONDS", defaultShutdownTimeout),
	}
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
