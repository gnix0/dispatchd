package security

import (
	"context"
	"os"
	"testing"

	"github.com/gnix0/dispatchd/internal/platform/config"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthenticateValidBearerToken(t *testing.T) {
	authenticator, err := NewAuthenticator(config.Service{
		AuthEnabled:         true,
		AuthJWTIssuer:       "dispatchd",
		AuthJWTAudience:     "dispatchd-clients",
		AuthJWTSharedSecret: "super-secret",
	})
	if err != nil {
		t.Fatalf("expected authenticator to build, got %v", err)
	}

	tokenString := signToken(t, jwt.MapClaims{
		"sub":   "user-1",
		"iss":   "dispatchd",
		"aud":   []string{"dispatchd-clients"},
		"roles": []string{"submitter", "viewer"},
	}, "super-secret")

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+tokenString))
	nextCtx, principal, err := authenticator.Authenticate(ctx)
	if err != nil {
		t.Fatalf("expected token to authenticate, got %v", err)
	}
	if principal.Subject != "user-1" {
		t.Fatalf("expected subject user-1, got %q", principal.Subject)
	}
	if !HasAnyRole(principal, "submitter", "viewer") {
		t.Fatalf("expected principal roles to be populated, got %#v", principal.Roles)
	}
	if _, ok := PrincipalFromContext(nextCtx); !ok {
		t.Fatal("expected principal to be stored in context")
	}
}

func TestAuthenticateRejectsMissingToken(t *testing.T) {
	authenticator, err := NewAuthenticator(config.Service{
		AuthEnabled:         true,
		AuthJWTIssuer:       "dispatchd",
		AuthJWTAudience:     "dispatchd-clients",
		AuthJWTSharedSecret: "super-secret",
	})
	if err != nil {
		t.Fatalf("expected authenticator to build, got %v", err)
	}

	_, _, err = authenticator.Authenticate(context.Background())
	if status.Code(err).String() != "Unauthenticated" {
		t.Fatalf("expected unauthenticated error, got %v", err)
	}
}

func TestValidateWorkerIdentityRejectsMismatch(t *testing.T) {
	ctx := WithPrincipal(context.Background(), &Principal{
		Subject:  "worker-1",
		WorkerID: "worker-1",
		Roles: map[string]struct{}{
			"worker": {},
		},
	})

	err := ValidateWorkerIdentity(ctx, "worker-2")
	if status.Code(err).String() != "PermissionDenied" {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestIsAuthorizedUsesMethodPolicies(t *testing.T) {
	principal := &Principal{
		Subject: "user-1",
		Roles: map[string]struct{}{
			"submitter": {},
			"viewer":    {},
		},
	}

	if !isAuthorized("/dispatchd.v1.JobService/SubmitJob", principal) {
		t.Fatal("expected submitter to be allowed to submit")
	}
	if isAuthorized("/dispatchd.v1.JobService/CancelJob", principal) {
		t.Fatal("expected submitter/viewer to be denied cancel")
	}
}

func TestAuthenticatorLoadsSharedSecretFromFile(t *testing.T) {
	secretFile, err := os.CreateTemp(t.TempDir(), "jwt-secret-*")
	if err != nil {
		t.Fatalf("create temp secret file: %v", err)
	}
	if _, err := secretFile.WriteString("super-secret\n"); err != nil {
		t.Fatalf("write temp secret file: %v", err)
	}
	if err := secretFile.Close(); err != nil {
		t.Fatalf("close temp secret file: %v", err)
	}

	authenticator, err := NewAuthenticator(config.Service{
		AuthEnabled:             true,
		AuthJWTIssuer:           "dispatchd",
		AuthJWTAudience:         "dispatchd-clients",
		AuthJWTSharedSecretFile: secretFile.Name(),
	})
	if err != nil {
		t.Fatalf("expected authenticator to build from file, got %v", err)
	}

	tokenString := signToken(t, jwt.MapClaims{
		"sub": "user-1",
		"iss": "dispatchd",
		"aud": []string{"dispatchd-clients"},
	}, "super-secret")

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+tokenString))
	if _, _, err := authenticator.Authenticate(ctx); err != nil {
		t.Fatalf("expected token signed with file secret to authenticate, got %v", err)
	}
}

func signToken(t *testing.T, claims jwt.MapClaims, secret string) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}
