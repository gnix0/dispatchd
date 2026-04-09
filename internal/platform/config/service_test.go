package config

import (
	"testing"
	"time"
)

func TestLoadUsesDefaultsWhenEnvVarsAreUnset(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("LOG_LEVEL", "")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "")

	got := Load("control-plane")

	if got.Name != "control-plane" {
		t.Fatalf("expected service name control-plane, got %q", got.Name)
	}

	if got.Environment != "development" {
		t.Fatalf("expected default environment development, got %q", got.Environment)
	}

	if got.LogLevel != "info" {
		t.Fatalf("expected default log level info, got %q", got.LogLevel)
	}

	if got.GRPCPort != 8080 {
		t.Fatalf("expected default gRPC port 8080, got %d", got.GRPCPort)
	}

	if got.ShutdownTimeout != 10*time.Second {
		t.Fatalf("expected default shutdown timeout 10s, got %s", got.ShutdownTimeout)
	}
}

func TestLoadUsesEnvironmentOverrides(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("GRPC_PORT", "9090")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "45")

	got := Load("scheduler")

	if got.Environment != "production" {
		t.Fatalf("expected environment production, got %q", got.Environment)
	}

	if got.LogLevel != "debug" {
		t.Fatalf("expected log level debug, got %q", got.LogLevel)
	}

	if got.GRPCPort != 9090 {
		t.Fatalf("expected gRPC port 9090, got %d", got.GRPCPort)
	}

	if got.ShutdownTimeout != 45*time.Second {
		t.Fatalf("expected shutdown timeout 45s, got %s", got.ShutdownTimeout)
	}
}
