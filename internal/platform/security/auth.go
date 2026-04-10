package security

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Principal struct {
	Subject  string
	WorkerID string
	Roles    map[string]struct{}
}

type Authenticator struct {
	enabled      bool
	issuer       string
	audience     string
	sharedSecret []byte
	publicKey    *rsa.PublicKey
}

type contextKey string

const principalKey contextKey = "principal"

func NewAuthenticator(cfg config.Service) (*Authenticator, error) {
	authenticator := &Authenticator{
		enabled:  cfg.AuthEnabled,
		issuer:   cfg.AuthJWTIssuer,
		audience: cfg.AuthJWTAudience,
	}
	if !cfg.AuthEnabled {
		return authenticator, nil
	}

	if cfg.AuthJWTSharedSecret != "" {
		authenticator.sharedSecret = []byte(cfg.AuthJWTSharedSecret)
		return authenticator, nil
	}

	if strings.TrimSpace(cfg.AuthJWTPublicKeyPEM) != "" {
		publicKey, err := parseRSAPublicKey(cfg.AuthJWTPublicKeyPEM)
		if err != nil {
			return nil, err
		}
		authenticator.publicKey = publicKey
		return authenticator, nil
	}

	return nil, errors.New("auth is enabled but no jwt shared secret or public key is configured")
}

func (a *Authenticator) Enabled() bool {
	return a != nil && a.enabled
}

func (a *Authenticator) Authenticate(ctx context.Context) (context.Context, *Principal, error) {
	if a == nil || !a.enabled {
		return ctx, nil, nil
	}

	tokenString, err := bearerTokenFromContext(ctx)
	if err != nil {
		return ctx, nil, status.Error(codes.Unauthenticated, err.Error())
	}

	token, err := jwt.Parse(tokenString, a.keyFunc(), a.parseOptions()...)
	if err != nil {
		return ctx, nil, status.Error(codes.Unauthenticated, "invalid bearer token")
	}

	mapClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return ctx, nil, status.Error(codes.Unauthenticated, "invalid bearer token claims")
	}

	subject, _ := mapClaims["sub"].(string)
	if strings.TrimSpace(subject) == "" {
		return ctx, nil, status.Error(codes.Unauthenticated, "jwt subject is required")
	}

	principal := &Principal{
		Subject:  subject,
		WorkerID: stringClaim(mapClaims, "worker_id"),
		Roles:    rolesFromClaims(mapClaims),
	}

	return WithPrincipal(ctx, principal), principal, nil
}

func WithPrincipal(ctx context.Context, principal *Principal) context.Context {
	if principal == nil {
		return ctx
	}
	return context.WithValue(ctx, principalKey, principal)
}

func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	principal, ok := ctx.Value(principalKey).(*Principal)
	return principal, ok && principal != nil
}

func ValidateWorkerIdentity(ctx context.Context, workerID string) error {
	principal, ok := PrincipalFromContext(ctx)
	if !ok {
		return nil
	}

	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return status.Error(codes.InvalidArgument, "worker_id is required")
	}

	if principal.WorkerID != "" && principal.WorkerID != workerID {
		return status.Error(codes.PermissionDenied, "worker identity does not match token")
	}

	if HasRole(principal, "worker") && principal.Subject != workerID {
		return status.Error(codes.PermissionDenied, "worker subject does not match worker_id")
	}

	return nil
}

func HasAnyRole(principal *Principal, roles ...string) bool {
	if principal == nil {
		return false
	}
	for _, role := range roles {
		if HasRole(principal, role) {
			return true
		}
	}
	return false
}

func HasRole(principal *Principal, role string) bool {
	if principal == nil {
		return false
	}
	_, ok := principal.Roles[strings.TrimSpace(role)]
	return ok
}

func (a *Authenticator) keyFunc() jwt.Keyfunc {
	return func(token *jwt.Token) (any, error) {
		switch {
		case a.sharedSecret != nil:
			return a.sharedSecret, nil
		case a.publicKey != nil:
			return a.publicKey, nil
		default:
			return nil, errors.New("no jwt verification key configured")
		}
	}
}

func (a *Authenticator) parseOptions() []jwt.ParserOption {
	options := []jwt.ParserOption{}
	if a.sharedSecret != nil {
		options = append(options, jwt.WithValidMethods([]string{"HS256", "HS384", "HS512"}))
	}
	if a.publicKey != nil {
		options = append(options, jwt.WithValidMethods([]string{"RS256", "RS384", "RS512"}))
	}
	if a.issuer != "" {
		options = append(options, jwt.WithIssuer(a.issuer))
	}
	if a.audience != "" {
		options = append(options, jwt.WithAudience(a.audience))
	}
	return options
}

func bearerTokenFromContext(ctx context.Context) (string, error) {
	metadataValues, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("authorization metadata is required")
	}

	values := metadataValues.Get("authorization")
	if len(values) == 0 {
		return "", errors.New("authorization metadata is required")
	}

	parts := strings.SplitN(values[0], " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", errors.New("authorization metadata must use bearer scheme")
	}

	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", errors.New("bearer token is required")
	}

	return token, nil
}

func parseRSAPublicKey(rawPEM string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(rawPEM))
	if block == nil {
		return nil, errors.New("failed to decode jwt public key pem")
	}

	if block.Type == "CERTIFICATE" {
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse jwt public certificate: %w", err)
		}
		publicKey, ok := certificate.PublicKey.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("jwt certificate does not contain an rsa public key")
		}
		return publicKey, nil
	}

	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse jwt public key: %w", err)
	}

	publicKey, ok := key.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("jwt public key is not rsa")
	}
	return publicKey, nil
}

func rolesFromClaims(claims jwt.MapClaims) map[string]struct{} {
	roles := make(map[string]struct{})

	addRole := func(role string) {
		role = strings.TrimSpace(role)
		if role == "" {
			return
		}
		roles[role] = struct{}{}
	}

	for _, role := range stringSliceClaim(claims, "roles") {
		addRole(role)
	}
	for _, role := range strings.Fields(stringClaim(claims, "scope")) {
		addRole(role)
	}
	if realmAccess, ok := claims["realm_access"].(map[string]any); ok {
		for _, role := range stringSliceAny(realmAccess["roles"]) {
			addRole(role)
		}
	}

	return roles
}

func stringClaim(claims jwt.MapClaims, key string) string {
	value, _ := claims[key].(string)
	return strings.TrimSpace(value)
}

func stringSliceClaim(claims jwt.MapClaims, key string) []string {
	return stringSliceAny(claims[key])
}

func stringSliceAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			if stringValue, ok := item.(string); ok {
				result = append(result, stringValue)
			}
		}
		return result
	case string:
		if typed == "" {
			return nil
		}
		return []string{typed}
	default:
		return nil
	}
}
