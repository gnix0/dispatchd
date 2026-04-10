package config

import (
	"testing"
	"time"
)

func TestLoadUsesDefaultsWhenEnvVarsAreUnset(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("LOG_LEVEL", "")
	t.Setenv("METRICS_PORT", "")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_ADDRESS", "")
	t.Setenv("INSTANCE_ID", "")
	t.Setenv("HOSTNAME", "")
	t.Setenv("SCHEDULER_POLL_INTERVAL", "")
	t.Setenv("LEASE_DURATION", "")
	t.Setenv("WORKER_HEARTBEAT_TTL", "")
	t.Setenv("READY_QUEUE_PREFIX", "")
	t.Setenv("SCHEDULER_LEADER_KEY", "")
	t.Setenv("AUTH_ENABLED", "")
	t.Setenv("AUTH_JWT_ISSUER", "")
	t.Setenv("AUTH_JWT_AUDIENCE", "")
	t.Setenv("AUTH_JWT_SHARED_SECRET", "")
	t.Setenv("AUTH_JWT_PUBLIC_KEY_PEM", "")
	t.Setenv("AUDIT_LOG_ENABLED", "")
	t.Setenv("TLS_ENABLED", "")
	t.Setenv("TLS_CERT_FILE", "")
	t.Setenv("TLS_KEY_FILE", "")
	t.Setenv("TLS_CLIENT_CA_FILE", "")
	t.Setenv("TLS_REQUIRE_CLIENT_CERT", "")
	t.Setenv("TRACING_ENABLED", "")
	t.Setenv("OTLP_ENDPOINT", "")
	t.Setenv("OTLP_INSECURE", "")
	t.Setenv("TRACE_SAMPLE_RATE", "")

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

	if got.MetricsPort != 9100 {
		t.Fatalf("expected default metrics port 9100, got %d", got.MetricsPort)
	}

	if got.ShutdownTimeout != 10*time.Second {
		t.Fatalf("expected default shutdown timeout 10s, got %s", got.ShutdownTimeout)
	}

	if got.DatabaseURL != "postgres://postgres:postgres@postgres:5432/task_orchestrator?sslmode=disable" {
		t.Fatalf("unexpected default database url: %q", got.DatabaseURL)
	}

	if got.RedisAddress != "redis:6379" {
		t.Fatalf("expected default redis address redis:6379, got %q", got.RedisAddress)
	}

	if got.InstanceID != "control-plane" {
		t.Fatalf("expected fallback instance id control-plane, got %q", got.InstanceID)
	}

	if got.SchedulerPollInterval != 2*time.Second {
		t.Fatalf("expected default scheduler poll interval 2s, got %s", got.SchedulerPollInterval)
	}

	if got.LeaseDuration != 30*time.Second {
		t.Fatalf("expected default lease duration 30s, got %s", got.LeaseDuration)
	}

	if got.WorkerHeartbeatTTL != 45*time.Second {
		t.Fatalf("expected default heartbeat ttl 45s, got %s", got.WorkerHeartbeatTTL)
	}

	if got.AuthEnabled {
		t.Fatal("expected auth to be disabled by default")
	}

	if got.AuthJWTIssuer != "task-orchestrator" {
		t.Fatalf("expected default auth issuer task-orchestrator, got %q", got.AuthJWTIssuer)
	}

	if got.AuthJWTAudience != "task-orchestrator-clients" {
		t.Fatalf("expected default auth audience task-orchestrator-clients, got %q", got.AuthJWTAudience)
	}

	if !got.AuditLogEnabled {
		t.Fatal("expected audit logging to be enabled by default")
	}

	if got.TLSEnabled {
		t.Fatal("expected tls to be disabled by default")
	}

	if got.TracingEnabled {
		t.Fatal("expected tracing to be disabled by default")
	}

	if got.OTLPEndpoint != "jaeger:4317" {
		t.Fatalf("expected default otlp endpoint jaeger:4317, got %q", got.OTLPEndpoint)
	}

	if !got.OTLPInsecure {
		t.Fatal("expected otlp exporter to default to insecure in compose/dev")
	}

	if got.TraceSampleRate != 1.0 {
		t.Fatalf("expected default trace sample rate 1.0, got %f", got.TraceSampleRate)
	}
}

func TestLoadUsesEnvironmentOverrides(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("GRPC_PORT", "9090")
	t.Setenv("METRICS_PORT", "9191")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "45")
	t.Setenv("DATABASE_URL", "postgres://custom")
	t.Setenv("REDIS_ADDRESS", "redis.example:6380")
	t.Setenv("REDIS_DB", "5")
	t.Setenv("INSTANCE_ID", "scheduler-1")
	t.Setenv("SCHEDULER_POLL_INTERVAL", "5s")
	t.Setenv("LEASE_DURATION", "90s")
	t.Setenv("WORKER_HEARTBEAT_TTL", "2m")
	t.Setenv("READY_QUEUE_PREFIX", "custom:ready")
	t.Setenv("SCHEDULER_LEADER_KEY", "custom:leader")
	t.Setenv("AUTH_ENABLED", "true")
	t.Setenv("AUTH_JWT_ISSUER", "issuer.example")
	t.Setenv("AUTH_JWT_AUDIENCE", "aud.example")
	t.Setenv("AUTH_JWT_SHARED_SECRET", "secret-value")
	t.Setenv("AUTH_JWT_PUBLIC_KEY_PEM", "pem-value")
	t.Setenv("AUDIT_LOG_ENABLED", "false")
	t.Setenv("TLS_ENABLED", "true")
	t.Setenv("TLS_CERT_FILE", "/tls/server.crt")
	t.Setenv("TLS_KEY_FILE", "/tls/server.key")
	t.Setenv("TLS_CLIENT_CA_FILE", "/tls/ca.crt")
	t.Setenv("TLS_REQUIRE_CLIENT_CERT", "true")
	t.Setenv("TRACING_ENABLED", "true")
	t.Setenv("OTLP_ENDPOINT", "otel-collector:4317")
	t.Setenv("OTLP_INSECURE", "false")
	t.Setenv("TRACE_SAMPLE_RATE", "0.25")

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

	if got.MetricsPort != 9191 {
		t.Fatalf("expected metrics port 9191, got %d", got.MetricsPort)
	}

	if got.ShutdownTimeout != 45*time.Second {
		t.Fatalf("expected shutdown timeout 45s, got %s", got.ShutdownTimeout)
	}

	if got.DatabaseURL != "postgres://custom" {
		t.Fatalf("expected overridden database url, got %q", got.DatabaseURL)
	}

	if got.RedisAddress != "redis.example:6380" {
		t.Fatalf("expected overridden redis address, got %q", got.RedisAddress)
	}

	if got.RedisDB != 5 {
		t.Fatalf("expected redis db 5, got %d", got.RedisDB)
	}

	if got.InstanceID != "scheduler-1" {
		t.Fatalf("expected instance id scheduler-1, got %q", got.InstanceID)
	}

	if got.SchedulerPollInterval != 5*time.Second {
		t.Fatalf("expected scheduler poll interval 5s, got %s", got.SchedulerPollInterval)
	}

	if got.LeaseDuration != 90*time.Second {
		t.Fatalf("expected lease duration 90s, got %s", got.LeaseDuration)
	}

	if got.WorkerHeartbeatTTL != 2*time.Minute {
		t.Fatalf("expected worker heartbeat ttl 2m, got %s", got.WorkerHeartbeatTTL)
	}

	if got.ReadyQueuePrefix != "custom:ready" {
		t.Fatalf("expected ready queue prefix custom:ready, got %q", got.ReadyQueuePrefix)
	}

	if got.SchedulerLeaderKey != "custom:leader" {
		t.Fatalf("expected scheduler leader key custom:leader, got %q", got.SchedulerLeaderKey)
	}

	if !got.AuthEnabled {
		t.Fatal("expected auth to be enabled")
	}

	if got.AuthJWTIssuer != "issuer.example" {
		t.Fatalf("expected auth issuer issuer.example, got %q", got.AuthJWTIssuer)
	}

	if got.AuthJWTAudience != "aud.example" {
		t.Fatalf("expected auth audience aud.example, got %q", got.AuthJWTAudience)
	}

	if got.AuthJWTSharedSecret != "secret-value" {
		t.Fatalf("expected auth shared secret override, got %q", got.AuthJWTSharedSecret)
	}

	if got.AuthJWTPublicKeyPEM != "pem-value" {
		t.Fatalf("expected auth public key override, got %q", got.AuthJWTPublicKeyPEM)
	}

	if got.AuditLogEnabled {
		t.Fatal("expected audit logging override to false")
	}

	if !got.TLSEnabled {
		t.Fatal("expected tls to be enabled")
	}

	if got.TLSCertFile != "/tls/server.crt" || got.TLSKeyFile != "/tls/server.key" || got.TLSClientCAFile != "/tls/ca.crt" {
		t.Fatalf("unexpected tls file configuration: cert=%q key=%q ca=%q", got.TLSCertFile, got.TLSKeyFile, got.TLSClientCAFile)
	}

	if !got.TLSRequireClientCert {
		t.Fatal("expected tls client cert requirement to be enabled")
	}

	if !got.TracingEnabled {
		t.Fatal("expected tracing to be enabled")
	}

	if got.OTLPEndpoint != "otel-collector:4317" {
		t.Fatalf("expected otlp endpoint override, got %q", got.OTLPEndpoint)
	}

	if got.OTLPInsecure {
		t.Fatal("expected otlp insecure override to false")
	}

	if got.TraceSampleRate != 0.25 {
		t.Fatalf("expected trace sample rate 0.25, got %f", got.TraceSampleRate)
	}
}
