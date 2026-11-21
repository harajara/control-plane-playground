// bootstrap validates the environment once, before the application services
// start: key material parses and forms a real pair, the tenant resource server
// answers, and the secret store root is writable. There is nothing to create in
// a key service, which is the point - a container restart can no longer lose an
// alias and invalidate every message already on the queue.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"tenant-control-plane/internal/applog"
	"tenant-control-plane/internal/secretstore"
	"tenant-control-plane/internal/secure"
)

// stage names the failing step without repeating its error text: connection
// errors from the tenant server can contain the administrator credential.
type stage string

const (
	stageSigner  stage = "message-signing-key"
	stageSealer  stage = "seal-public-key"
	stageOpener  stage = "seal-key-pair"
	stageTenant  stage = "tenant-database"
	stageSecrets stage = "secret-store"
)

func main() {
	applog.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		os.Exit(1)
	}
	slog.Info("environment and secret store ready")
}

func run(ctx context.Context) error {
	if _, err := secure.SignerFromEnv(); err != nil {
		return fail(stageSigner, err)
	}
	if _, err := secure.SealerFromEnv(); err != nil {
		return fail(stageSealer, err)
	}
	if _, err := secure.OpenerFromEnv(); err != nil {
		return fail(stageOpener, err)
	}
	admin := os.Getenv("TENANT_DB_ADMIN_URL")
	if admin == "" {
		return fail(stageTenant, errors.New("TENANT_DB_ADMIN_URL is required; run task setup"))
	}
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		return fail(stageTenant, err)
	}
	defer conn.Close(ctx)
	if err = conn.Ping(ctx); err != nil {
		return fail(stageTenant, err)
	}
	if _, err = secretstore.FromEnv(); err != nil {
		return fail(stageSecrets, err)
	}
	return nil
}

func fail(s stage, err error) error {
	slog.Error("bootstrap failed", "stage", string(s), "error_type", fmt.Sprintf("%T", err))
	return fmt.Errorf("bootstrap failed at %s", s)
}
