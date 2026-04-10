package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultShutdownTimeout = 10 * time.Second

type Service struct {
	Name                  string
	Environment           string
	LogLevel              string
	GRPCPort              int
	ShutdownTimeout       time.Duration
	DatabaseURL           string
	RedisAddress          string
	RedisPassword         string
	RedisDB               int
	InstanceID            string
	SchedulerPollInterval time.Duration
	LeaseDuration         time.Duration
	WorkerHeartbeatTTL    time.Duration
	SchedulerLeaderKey    string
	ReadyQueuePrefix      string
	AuthEnabled           bool
	AuthJWTIssuer         string
	AuthJWTAudience       string
	AuthJWTSharedSecret   string
	AuthJWTPublicKeyPEM   string
	AuditLogEnabled       bool
	TLSEnabled            bool
	TLSCertFile           string
	TLSKeyFile            string
	TLSClientCAFile       string
	TLSRequireClientCert  bool
}

func Load(serviceName string) Service {
	instanceID := strings.TrimSpace(os.Getenv("INSTANCE_ID"))
	if instanceID == "" {
		instanceID = strings.TrimSpace(os.Getenv("HOSTNAME"))
	}
	if instanceID == "" {
		instanceID = serviceName
	}

	return Service{
		Name:                  serviceName,
		Environment:           getEnv("APP_ENV", "development"),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
		GRPCPort:              getIntEnv("GRPC_PORT", 8080),
		ShutdownTimeout:       getDurationEnv("SHUTDOWN_TIMEOUT_SECONDS", defaultShutdownTimeout),
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/task_orchestrator?sslmode=disable"),
		RedisAddress:          getEnv("REDIS_ADDRESS", "redis:6379"),
		RedisPassword:         os.Getenv("REDIS_PASSWORD"),
		RedisDB:               getIntEnv("REDIS_DB", 0),
		InstanceID:            instanceID,
		SchedulerPollInterval: getDurationValueEnv("SCHEDULER_POLL_INTERVAL", 2*time.Second),
		LeaseDuration:         getDurationValueEnv("LEASE_DURATION", 30*time.Second),
		WorkerHeartbeatTTL:    getDurationValueEnv("WORKER_HEARTBEAT_TTL", 45*time.Second),
		SchedulerLeaderKey:    getEnv("SCHEDULER_LEADER_KEY", "task-orchestrator:scheduler:leader"),
		ReadyQueuePrefix:      getEnv("READY_QUEUE_PREFIX", "task-orchestrator:ready"),
		AuthEnabled:           getBoolEnv("AUTH_ENABLED", false),
		AuthJWTIssuer:         getEnv("AUTH_JWT_ISSUER", "task-orchestrator"),
		AuthJWTAudience:       getEnv("AUTH_JWT_AUDIENCE", "task-orchestrator-clients"),
		AuthJWTSharedSecret:   os.Getenv("AUTH_JWT_SHARED_SECRET"),
		AuthJWTPublicKeyPEM:   os.Getenv("AUTH_JWT_PUBLIC_KEY_PEM"),
		AuditLogEnabled:       getBoolEnv("AUDIT_LOG_ENABLED", true),
		TLSEnabled:            getBoolEnv("TLS_ENABLED", false),
		TLSCertFile:           getEnv("TLS_CERT_FILE", "/var/run/task-orchestrator/tls/tls.crt"),
		TLSKeyFile:            getEnv("TLS_KEY_FILE", "/var/run/task-orchestrator/tls/tls.key"),
		TLSClientCAFile:       getEnv("TLS_CLIENT_CA_FILE", "/var/run/task-orchestrator/tls/ca.crt"),
		TLSRequireClientCert:  getBoolEnv("TLS_REQUIRE_CLIENT_CERT", false),
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

func getDurationValueEnv(key string, fallback time.Duration) time.Duration {
	rawValue := strings.TrimSpace(os.Getenv(key))
	if rawValue == "" {
		return fallback
	}

	value, err := time.ParseDuration(rawValue)
	if err != nil || value <= 0 {
		return fallback
	}

	return value
}

func getBoolEnv(key string, fallback bool) bool {
	rawValue := strings.TrimSpace(os.Getenv(key))
	if rawValue == "" {
		return fallback
	}

	value, err := strconv.ParseBool(rawValue)
	if err != nil {
		return fallback
	}

	return value
}
