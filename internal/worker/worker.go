package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	randv2 "math/rand/v2"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/messaging"
	"tenant-control-plane/internal/secretstore"
	"tenant-control-plane/internal/tenant"
)

type Infra struct {
	SchemaVersion int               `json:"schema_version"`
	TenantID      string            `json:"tenant_id"`
	Status        string            `json:"status"`
	Env           map[string]string `json:"env"`
	Managed       *Managed          `json:"managed,omitempty"`
}
type Managed struct {
	Database string `json:"database"`
	Role     string `json:"role"`
}

// Secrets is the tenant infrastructure document store. Opener opens an
// imported connection string that the control plane sealed for this worker.
type Secrets interface {
	Get(context.Context, string, any) error
	Put(context.Context, string, string, any) error
	Delete(context.Context, string) error
}
type Opener interface {
	Open(context.Context, string) (string, error)
}
type Worker struct {
	ID, AdminURL       string
	Pool               *pgxpool.Pool
	Secrets            Secrets
	Opener             Opener
	Broker             *messaging.Broker
	FailRate           float64
	MinDelay, MaxDelay time.Duration
}

const journalSchema = `CREATE TABLE IF NOT EXISTS provisioning_receipts(task_id uuid PRIMARY KEY,tenant_id uuid NOT NULL,result jsonb NOT NULL,completed_at timestamptz NOT NULL DEFAULT now())`

func (w *Worker) Init(ctx context.Context) error {
	var err error
	w.Pool, err = pgxpool.New(ctx, w.AdminURL)
	if err != nil {
		return err
	}
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(764422)")
	if err == nil {
		_, err = tx.Exec(ctx, journalSchema)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (w *Worker) Handle(ctx context.Context, e messaging.Event) error {
	var c domain.Command
	if json.Unmarshal(e.Data, &c) != nil {
		return messaging.ErrPoison
	}
	if _, err := uuid.Parse(c.Task.ID); err != nil {
		return messaging.ErrPoison
	}
	if _, err := uuid.Parse(c.Task.TenantID); err != nil {
		return messaging.ErrPoison
	}
	if e.ID != c.Task.ID || e.TenantID != c.Task.TenantID || e.Type != "task."+c.Task.Type || c.Task.Status != "accepted" || tenant.ValidateSlug(c.Slug) != nil || c.SecretID != tenant.InfraSecretID(c.Slug) {
		return messaging.ErrPoison
	}
	if c.Task.Type != "deploy" && c.Task.Type != "update" && c.Task.Type != "destroy" {
		return messaging.ErrPoison
	}
	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	// This lock is on the tenant resource server, never the control-plane DB.
	// It serializes side effects even during Kafka consumer-group rebalances.
	_, err = conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1,0))", c.Task.TenantID)
	if err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock(hashtextextended($1,0))", c.Task.TenantID); err != nil {
			_ = conn.Conn().Close(unlockCtx)
		}
	}()
	r := domain.Result{TaskID: c.Task.ID, TenantID: c.Task.TenantID, Type: c.Task.Type, Status: "in_progress"}
	if err = w.Broker.Result(ctx, r, w.ID); err != nil {
		return err
	}
	var receipt []byte
	err = conn.QueryRow(ctx, "SELECT result FROM provisioning_receipts WHERE task_id=$1", c.Task.ID).Scan(&receipt)
	if err == nil {
		if err = json.Unmarshal(receipt, &r); err != nil {
			return err
		}
		return w.Broker.Result(ctx, r, w.ID)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	delay := w.MinDelay
	if w.MaxDelay > w.MinDelay {
		delay += time.Duration(randv2.Int64N(int64(w.MaxDelay - w.MinDelay)))
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	r.Status = "done"
	if randv2.Float64() < w.FailRate {
		r.Status = "failed"
		r.ErrorCode = "injected_failure"
	} else {
		err = w.provision(ctx, conn.Conn(), c)
		var permanent Permanent
		if errors.As(err, &permanent) {
			r.Status = "failed"
			r.ErrorCode = string(permanent)
		} else if err != nil {
			return err
		}
	}
	// One line per provisioning outcome. Codes only: never a credential, a
	// connection string or a generated password.
	slog.Info("provisioning finished", "worker", w.ID, "task_type", c.Task.Type, "status", r.Status, "code", r.ErrorCode)
	receipt, _ = json.Marshal(r)
	_, err = conn.Exec(ctx, "INSERT INTO provisioning_receipts(task_id,tenant_id,result) VALUES($1,$2,$3)", c.Task.ID, c.Task.TenantID, receipt)
	if err != nil {
		return err
	}
	return w.Broker.Result(ctx, r, w.ID)
}

type Permanent string

func (p Permanent) Error() string { return string(p) }
func version(task, stage string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(task+"|"+stage)).String()
}
func randomPassword() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func envFor(raw string) (map[string]string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User == nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nil, Permanent("invalid_database_url")
	}
	password, _ := u.User.Password()
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	return map[string]string{"DATABASE_URL": raw, "DB_HOST": u.Hostname(), "DB_PORT": port, "DB_NAME": strings.TrimPrefix(u.Path, "/"), "DB_USER": u.User.Username(), "DB_PASSWORD": password}, nil
}
func (w *Worker) provision(ctx context.Context, conn *pgx.Conn, c domain.Command) error {
	var infra Infra
	err := w.Secrets.Get(ctx, c.SecretID, &infra)
	if err != nil && !errors.Is(err, secretstore.ErrMissing) {
		return err
	}
	if err == nil && infra.TenantID != c.Task.TenantID {
		return Permanent("resource_ownership_conflict")
	}
	switch c.Task.Type {
	case "deploy":
		if errors.Is(err, secretstore.ErrMissing) {
			name := "tenant_" + strings.ReplaceAll(c.Slug, "-", "_")
			u, err := url.Parse(w.AdminURL)
			if err != nil {
				return err
			}
			u.User = url.UserPassword(name, randomPassword())
			u.Path = "/" + name
			env, err := envFor(u.String())
			if err != nil {
				return err
			}
			infra = Infra{SchemaVersion: 1, TenantID: c.Task.TenantID, Status: "provisioning", Env: env, Managed: &Managed{name, name}}
			// Persist the generated credential BEFORE CREATE ROLE. A crash/retry reuses it.
			if err = w.Secrets.Put(ctx, c.SecretID, version(c.Task.ID, "intent"), infra); err != nil {
				return err
			}
		}
		if infra.Managed == nil {
			return Permanent("resource_ownership_conflict")
		}
		var exists bool
		if err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", infra.Managed.Role).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			password := strings.ReplaceAll(infra.Env["DB_PASSWORD"], "'", "''")
			_, err = conn.Exec(ctx, "CREATE ROLE "+pgx.Identifier{infra.Managed.Role}.Sanitize()+" LOGIN PASSWORD '"+password+"'")
			if err != nil {
				return err
			}
		}
		if err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", infra.Managed.Database).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{infra.Managed.Database}.Sanitize()+" OWNER "+pgx.Identifier{infra.Managed.Role}.Sanitize())
			if err != nil {
				return err
			}
		}
		infra.Status = "active"
		return w.Secrets.Put(ctx, c.SecretID, version(c.Task.ID, "done"), infra)
	case "update":
		if errors.Is(err, secretstore.ErrMissing) {
			return Permanent("tenant_infrastructure_missing")
		}
		if c.SealedURL == "" {
			return nil
		} // name-only PATCH
		raw, err := w.Opener.Open(ctx, c.SealedURL)
		if err != nil {
			return Permanent("sealed_url_unreadable")
		}
		env, err := envFor(raw)
		if err != nil {
			return err
		}
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		db, err := pgx.Connect(checkCtx, raw)
		if err != nil {
			return Permanent("database_connection_failed")
		}
		defer db.Close(ctx)
		var one int
		if err = db.QueryRow(checkCtx, "SELECT 1").Scan(&one); err != nil {
			return Permanent("database_connection_failed")
		}
		// Preserve unrelated environment variables and original managed resources.
		if infra.Env == nil {
			infra.Env = map[string]string{}
		}
		for k, v := range env {
			infra.Env[k] = v
		}
		infra.Status = "active"
		return w.Secrets.Put(ctx, c.SecretID, version(c.Task.ID, "done"), infra)
	case "destroy":
		if errors.Is(err, secretstore.ErrMissing) {
			return nil
		}
		if infra.Managed != nil {
			if _, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{infra.Managed.Database}.Sanitize()+" WITH (FORCE)"); err != nil {
				return err
			}
			if _, err = conn.Exec(ctx, "DROP ROLE IF EXISTS "+pgx.Identifier{infra.Managed.Role}.Sanitize()); err != nil {
				return err
			}
		}
		return w.Secrets.Delete(ctx, c.SecretID)
	}
	return fmt.Errorf("unknown task type")
}
