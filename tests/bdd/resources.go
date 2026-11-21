//go:build bdd

package bdd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tenant-control-plane/internal/secretstore"
	"tenant-control-plane/internal/worker"
)

// Resource-server and secret-store reads. The suite looks at the tenant
// Postgres with the same administrator credential the workers get from their
// environment, and the infrastructure documents through the same package the
// workers write them with.

func tenantAdminURL() string { return os.Getenv("TENANT_DB_ADMIN_URL") }

func withAdmin(ctx context.Context, fn func(*pgx.Conn) error) error {
	url := tenantAdminURL()
	if url == "" {
		return fmt.Errorf("TENANT_DB_ADMIN_URL is required")
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		// Connection errors can echo the credential, so only the type of
		// failure is reported.
		return fmt.Errorf("tenant resource server unreachable (%T)", err)
	}
	defer conn.Close(ctx)
	return fn(conn)
}

// importedDBName turns the feature-file placeholder into a real database name
// that cannot collide with a control-plane managed database.
func importedDBName(placeholder, suffix string) string {
	base := strings.ReplaceAll(strings.ToLower(placeholder), "-", "_")
	base = strings.Trim(base, "_")
	if base == "" {
		base = "imported"
	}
	name := "outside_" + base + "_" + suffix
	if len(name) > 56 {
		name = name[:56]
	}
	return strings.Trim(name, "_")
}

// managedDBName is the name the worker derives from a slug: tenant_ with the
// slug's dashes replaced, which is the identifier destroy must own.
func managedDBName(slug string) string { return "tenant_" + strings.ReplaceAll(slug, "-", "_") }

// createOutsideDatabase makes a database the control plane did not create and
// returns the URL the scenario imports it with.
func createOutsideDatabase(name string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	err := withAdmin(ctx, func(conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
		return err
	})
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return existingDatabaseURL(name), nil
		}
		return "", fmt.Errorf("create fixture database %s: %w", name, err)
	}
	return existingDatabaseURL(name), nil
}

// existingDatabaseURL points at a database on the tenant server using the
// administrator credential from the environment.
func existingDatabaseURL(name string) string {
	u := tenantAdminURL()
	i := strings.LastIndex(u, "/")
	if i < 0 {
		return u
	}
	return u[:i] + "/" + name
}

func dropDatabase(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	return withAdmin(ctx, func(conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		return err
	})
}

func databaseExists(name string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var found bool
	err := withAdmin(ctx, func(conn *pgx.Conn) error {
		return conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", name).Scan(&found)
	})
	return found, err
}

// countDatabasesWithPrefix counts databases whose name starts with a prefix.
// The redelivery scenario uses it to prove one replay did not create a second
// copy of the tenant's database.
func countDatabasesWithPrefix(prefix string) (int, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var names []string
	err := withAdmin(ctx, func(conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, "SELECT datname FROM pg_database WHERE datname LIKE $1 ORDER BY datname", prefix+"%")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var n string
			if err = rows.Scan(&n); err != nil {
				return err
			}
			names = append(names, n)
		}
		return rows.Err()
	})
	return len(names), names, err
}

func roleExists(name string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var found bool
	err := withAdmin(ctx, func(conn *pgx.Conn) error {
		return conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name).Scan(&found)
	})
	return found, err
}

// connectWith proves a stored credential actually works. The URL is used and
// discarded here: it never reaches an error message or a log line.
func connectWith(databaseURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("credential is not a usable URL (%T)", err)
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return fmt.Errorf("credential does not connect (%T)", err)
	}
	var one int
	if err = pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("credential cannot run a query (%T)", err)
	}
	return nil
}

// infraID is the documented secret id for a slug.
func infraID(slug string) string { return "control-plane/tenants/" + slug + "/infra" }

// readInfra loads one tenant's infrastructure document.
func readInfra(slug string) (*worker.Infra, error) {
	store, err := secretstore.FromEnv()
	if err != nil {
		return nil, err
	}
	var v worker.Infra
	err = store.Get(context.Background(), infraID(slug), &v)
	if errors.Is(err, secretstore.ErrMissing) {
		return nil, fmt.Errorf("no infrastructure document at %s", infraID(slug))
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// infraURL returns the runtime connection string for a tenant, for internal
// use only (connectivity checks and import assertions).
func infraURL(v *worker.Infra) string {
	if v == nil || v.Env == nil {
		return ""
	}
	return v.Env["DATABASE_URL"]
}
