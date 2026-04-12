package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultShutdownTimeout = 10 * time.Second

type Service struct {
	Name                    string
	Environment             string
	LogLevel                string
	GRPCPort                int
	MetricsPort             int
	ShutdownTimeout         time.Duration
	DatabaseURL             string
	RedisAddress            string
	RedisPassword           string
	RedisDB                 int
	InstanceID              string
	SchedulerPollInterval   time.Duration
	LeaseDuration           time.Duration
	WorkerHeartbeatTTL      time.Duration
	SchedulerLeaderKey      string
	ReadyQueuePrefix        string
	AuthEnabled             bool
	AuthJWTIssuer           string
	AuthJWTAudience         string
	AuthJWTSharedSecret     string
	AuthJWTSharedSecretFile string
	AuthJWTPublicKeyPEM     string
	AuthJWTPublicKeyPEMFile string
	AuditLogEnabled         bool
	TLSEnabled              bool
	TLSCertFile             string
	TLSKeyFile              string
	TLSClientCAFile         string
	TLSRequireClientCert    bool
	TracingEnabled          bool
	OTLPEndpoint            string
	OTLPInsecure            bool
	TraceSampleRate         float64
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
		Name:                    serviceName,
		Environment:             getEnv("APP_ENV", "development"),
		LogLevel:                getEnv("LOG_LEVEL", "info"),
		GRPCPort:                getIntEnv("GRPC_PORT", 8080),
		MetricsPort:             getIntEnv("METRICS_PORT", defaultMetricsPort(serviceName)),
		ShutdownTimeout:         getDurationEnv("SHUTDOWN_TIMEOUT_SECONDS", defaultShutdownTimeout),
		DatabaseURL:             getEnv("DATABASE_URL", "postgres://postgres:postgres@postgres:5432/dispatchd?sslmode=disable"),
		RedisAddress:            getEnv("REDIS_ADDRESS", "redis:6379"),
		RedisPassword:           os.Getenv("REDIS_PASSWORD"),
		RedisDB:                 getIntEnv("REDIS_DB", 0),
		InstanceID:              instanceID,
		SchedulerPollInterval:   getDurationValueEnv("SCHEDULER_POLL_INTERVAL", 2*time.Second),
		LeaseDuration:           getDurationValueEnv("LEASE_DURATION", 30*time.Second),
		WorkerHeartbeatTTL:      getDurationValueEnv("WORKER_HEARTBEAT_TTL", 45*time.Second),
		SchedulerLeaderKey:      getEnv("SCHEDULER_LEADER_KEY", "dispatchd:scheduler:leader"),
		ReadyQueuePrefix:        getEnv("READY_QUEUE_PREFIX", "dispatchd:ready"),
		AuthEnabled:             getBoolEnv("AUTH_ENABLED", false),
		AuthJWTIssuer:           getEnv("AUTH_JWT_ISSUER", "dispatchd"),
		AuthJWTAudience:         getEnv("AUTH_JWT_AUDIENCE", "dispatchd-clients"),
		AuthJWTSharedSecret:     os.Getenv("AUTH_JWT_SHARED_SECRET"),
		AuthJWTSharedSecretFile: strings.TrimSpace(os.Getenv("AUTH_JWT_SHARED_SECRET_FILE")),
		AuthJWTPublicKeyPEM:     os.Getenv("AUTH_JWT_PUBLIC_KEY_PEM"),
		AuthJWTPublicKeyPEMFile: strings.TrimSpace(os.Getenv("AUTH_JWT_PUBLIC_KEY_PEM_FILE")),
		AuditLogEnabled:         getBoolEnv("AUDIT_LOG_ENABLED", true),
		TLSEnabled:              getBoolEnv("TLS_ENABLED", false),
		TLSCertFile:             getEnv("TLS_CERT_FILE", "/var/run/dispatchd/tls/tls.crt"),
		TLSKeyFile:              getEnv("TLS_KEY_FILE", "/var/run/dispatchd/tls/tls.key"),
		TLSClientCAFile:         getEnv("TLS_CLIENT_CA_FILE", "/var/run/dispatchd/tls/ca.crt"),
		TLSRequireClientCert:    getBoolEnv("TLS_REQUIRE_CLIENT_CERT", false),
		TracingEnabled:          getBoolEnv("TRACING_ENABLED", false),
		OTLPEndpoint:            getEnv("OTLP_ENDPOINT", "jaeger:4317"),
		OTLPInsecure:            getBoolEnv("OTLP_INSECURE", true),
		TraceSampleRate:         getFloatEnv("TRACE_SAMPLE_RATE", 1.0),
	}
}

func (s Service) Validate() error {
	if s.AuthEnabled {
		if strings.TrimSpace(s.AuthJWTIssuer) == "" {
			return fmt.Errorf("auth_jwt_issuer is required when auth is enabled")
		}
		if strings.TrimSpace(s.AuthJWTAudience) == "" {
			return fmt.Errorf("auth_jwt_audience is required when auth is enabled")
		}
		if strings.TrimSpace(s.AuthJWTSharedSecret) == "" &&
			strings.TrimSpace(s.AuthJWTSharedSecretFile) == "" &&
			strings.TrimSpace(s.AuthJWTPublicKeyPEM) == "" &&
			strings.TrimSpace(s.AuthJWTPublicKeyPEMFile) == "" {
			return fmt.Errorf("auth requires a shared secret or public key configuration")
		}
	}

	if s.TLSEnabled {
		if strings.TrimSpace(s.TLSCertFile) == "" {
			return fmt.Errorf("tls_cert_file is required when tls is enabled")
		}
		if strings.TrimSpace(s.TLSKeyFile) == "" {
			return fmt.Errorf("tls_key_file is required when tls is enabled")
		}
	}

	if s.TLSRequireClientCert && strings.TrimSpace(s.TLSClientCAFile) == "" {
		return fmt.Errorf("tls_client_ca_file is required when client certificates are enforced")
	}

	if !s.enforcesNonDevSecurity() {
		return nil
	}

	if !s.AuthEnabled {
		return fmt.Errorf("auth must be enabled in %s", s.Environment)
	}
	if !s.TLSEnabled {
		return fmt.Errorf("tls must be enabled in %s", s.Environment)
	}
	if !s.TLSRequireClientCert {
		return fmt.Errorf("tls client certificate verification must be enabled in %s", s.Environment)
	}
	if !s.AuditLogEnabled {
		return fmt.Errorf("audit logging must be enabled in %s", s.Environment)
	}
	if s.Name != "scheduler" &&
		strings.TrimSpace(s.AuthJWTSharedSecret) == "" &&
		strings.TrimSpace(s.AuthJWTSharedSecretFile) == "" &&
		strings.TrimSpace(s.AuthJWTPublicKeyPEM) == "" &&
		strings.TrimSpace(s.AuthJWTPublicKeyPEMFile) == "" {
		return fmt.Errorf("non-development services require jwt verification material")
	}

	return nil
}

func (s Service) GRPCAddress() string {
	return ":" + strconv.Itoa(s.GRPCPort)
}

func (s Service) MetricsAddress() string {
	return ":" + strconv.Itoa(s.MetricsPort)
}

func defaultMetricsPort(serviceName string) int {
	switch strings.TrimSpace(serviceName) {
	case "control-plane":
		return 9100
	case "scheduler":
		return 9101
	case "worker-gateway":
		return 9102
	default:
		return 9100
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

func getFloatEnv(key string, fallback float64) float64 {
	rawValue := strings.TrimSpace(os.Getenv(key))
	if rawValue == "" {
		return fallback
	}

	value, err := strconv.ParseFloat(rawValue, 64)
	if err != nil || value < 0 || value > 1 {
		return fallback
	}

	return value
}

func (s Service) enforcesNonDevSecurity() bool {
	switch strings.ToLower(strings.TrimSpace(s.Environment)) {
	case "", "development", "development-kind", "dev", "local", "kubernetes", "test":
		return false
	default:
		return true
	}
}
