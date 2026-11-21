package store

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/tenant"
)

//go:embed schema.sql
var schema string

type Store struct{ DB *pgxpool.Pool }
type Problem string

func (p Problem) Error() string { return string(p) }

const (
	NotFound     Problem = "tenant_not_found"
	TaskNotFound Problem = "task_not_found"
	Exists       Problem = "tenant_already_exists"
	Conflict     Problem = "tenant_version_conflict"
	NotAllowed   Problem = "tenant_update_not_allowed"
)

func Open(ctx context.Context, url string) (*Store, error) {
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	s := &Store{p}
	tx, err := p.Begin(ctx)
	if err != nil {
		p.Close()
		return nil, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(8975123)")
	if err == nil {
		_, err = tx.Exec(ctx, schema)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		p.Close()
		return nil, err
	}
	return s, nil
}

const tenantColumns = "id::text,slug,name,status,version,infra_secret_id,created_at,updated_at"
const taskColumns = "id::text,type,tenant_id::text,status,created_at,updated_at"

func scanTenant(row pgx.Row) (v domain.Tenant, err error) {
	err = row.Scan(&v.ID, &v.Slug, &v.Name, &v.Status, &v.Version, &v.InfraSecretID, &v.CreatedAt.Time, &v.UpdatedAt.Time)
	return
}
func scanTask(row pgx.Row) (v domain.Task, err error) {
	err = row.Scan(&v.ID, &v.Type, &v.TenantID, &v.Status, &v.CreatedAt.Time, &v.UpdatedAt.Time)
	return
}
func newTask(ctx context.Context, tx pgx.Tx, t domain.Tenant, kind string, name *string, sealed string) (domain.Task, error) {
	id := uuid.NewString()
	now := domain.Now()
	v := domain.Task{ID: id, Type: kind, TenantID: t.ID, Status: "accepted", CreatedAt: now, UpdatedAt: now}
	cmd := domain.Command{Task: v, Slug: t.Slug, SecretID: t.InfraSecretID, PendingName: name, SealedURL: sealed}
	b, err := json.Marshal(cmd)
	if err != nil {
		return v, err
	}
	_, err = tx.Exec(ctx, "INSERT INTO tasks(id,type,tenant_id,status,command,created_at,updated_at) VALUES($1,$2,$3,'accepted',$4,$5,$5)", id, kind, t.ID, b, now.Time)
	return v, err
}
func (s *Store) Create(ctx context.Context, slug, name string) (domain.Tenant, domain.Task, error) {
	var task domain.Task
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return domain.Tenant{}, task, err
	}
	defer tx.Rollback(ctx)
	v, err := scanTenant(tx.QueryRow(ctx, "INSERT INTO tenants(id,slug,name,status,infra_secret_id) VALUES($1,$2,$3,'provisioning',$4) RETURNING "+tenantColumns, uuid.NewString(), slug, name, tenant.InfraSecretID(slug)))
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		return v, task, Exists
	}
	if err != nil {
		return v, task, err
	}
	task, err = newTask(ctx, tx, v, "deploy", nil, "")
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err == nil {
		slog.Info("tenant created", "tenant_id", v.ID, "slug", v.Slug, "status", v.Status, "task_id", task.ID, "task_type", task.Type)
	}
	return v, task, err
}
func (s *Store) Mutate(ctx context.Context, id, kind string, version int64, name *string, sealed string) (domain.Tenant, domain.Task, error) {
	var task domain.Task
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return domain.Tenant{}, task, err
	}
	defer tx.Rollback(ctx)
	v, err := scanTenant(tx.QueryRow(ctx, "SELECT "+tenantColumns+" FROM tenants WHERE id=$1 FOR UPDATE", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return v, task, NotFound
	}
	if err != nil {
		return v, task, err
	}
	var next tenant.Status
	if kind == "update" {
		if v.Version != version {
			return v, task, Conflict
		}
		next, err = tenant.BeginUpdate(tenant.Status(v.Status))
	} else {
		next, err = tenant.BeginDestroy(tenant.Status(v.Status))
	}
	if err != nil {
		return v, task, NotAllowed
	}
	v, err = scanTenant(tx.QueryRow(ctx, "UPDATE tenants SET status=$2,version=version+1,updated_at=now() WHERE id=$1 RETURNING "+tenantColumns, id, string(next)))
	if err == nil {
		task, err = newTask(ctx, tx, v, kind, name, sealed)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err == nil {
		slog.Info("tenant mutation accepted", "tenant_id", v.ID, "slug", v.Slug, "status", v.Status, "version", v.Version, "task_id", task.ID, "task_type", kind, "imported_database", sealed != "")
	}
	return v, task, err
}
func (s *Store) Tenant(ctx context.Context, id string) (domain.Tenant, error) {
	v, err := scanTenant(s.DB.QueryRow(ctx, "SELECT "+tenantColumns+" FROM tenants WHERE id=$1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		err = NotFound
	}
	return v, err
}
func (s *Store) Task(ctx context.Context, id string) (domain.Task, error) {
	v, err := scanTask(s.DB.QueryRow(ctx, "SELECT "+taskColumns+" FROM tasks WHERE id=$1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		err = TaskNotFound
	}
	return v, err
}

type Filter struct {
	Limit                          int
	Cursor, Status, Type, TenantID string
}
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func cursor(t domain.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}
func decodeCursor(c string) (time.Time, string, error) {
	if c == "" {
		return time.Time{}, "00000000-0000-0000-0000-000000000000", nil
	}
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, "", Problem("validation_failed")
	}
	parts := strings.Split(string(b), "|")
	if len(parts) != 2 {
		return time.Time{}, "", Problem("validation_failed")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return t, "", Problem("validation_failed")
	}
	if _, err = uuid.Parse(parts[1]); err != nil {
		return t, "", Problem("validation_failed")
	}
	return t, parts[1], nil
}
func (s *Store) Tenants(ctx context.Context, f Filter) (Page[domain.Tenant], error) {
	p := Page[domain.Tenant]{Items: []domain.Tenant{}}
	t, id, err := decodeCursor(f.Cursor)
	if err != nil {
		return p, err
	}
	rows, err := s.DB.Query(ctx, "SELECT "+tenantColumns+" FROM tenants WHERE (created_at,id)>($1,$2) AND ($3='' OR status=$3) ORDER BY created_at,id LIMIT $4", t, id, f.Status, f.Limit+1)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		v, err := scanTenant(rows)
		if err != nil {
			return p, err
		}
		p.Items = append(p.Items, v)
	}
	if len(p.Items) > f.Limit {
		p.Items = p.Items[:f.Limit]
		v := p.Items[len(p.Items)-1]
		p.NextCursor = cursor(v.CreatedAt, v.ID)
	}
	return p, rows.Err()
}
func (s *Store) Tasks(ctx context.Context, f Filter) (Page[domain.Task], error) {
	p := Page[domain.Task]{Items: []domain.Task{}}
	t, id, err := decodeCursor(f.Cursor)
	if err != nil {
		return p, err
	}
	rows, err := s.DB.Query(ctx, "SELECT "+taskColumns+" FROM tasks WHERE (created_at,id)>($1,$2) AND ($3='' OR status=$3) AND ($4='' OR type=$4) AND ($5='' OR tenant_id::text=$5) ORDER BY created_at,id LIMIT $6", t, id, f.Status, f.Type, f.TenantID, f.Limit+1)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		v, err := scanTask(rows)
		if err != nil {
			return p, err
		}
		p.Items = append(p.Items, v)
	}
	if len(p.Items) > f.Limit {
		p.Items = p.Items[:f.Limit]
		v := p.Items[len(p.Items)-1]
		p.NextCursor = cursor(v.CreatedAt, v.ID)
	}
	return p, rows.Err()
}

// PublishOne holds a short row lock across the bounded broker send. All replicas
// use this path, including the fast path, so relay attempts cannot race locally.
// A crash after broker acknowledgement can still duplicate the stable task ID.
func (s *Store) PublishOne(ctx context.Context, id string, send func(context.Context, domain.Command) error) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var b []byte
	var taskID string
	err = tx.QueryRow(ctx, `SELECT id::text,command FROM tasks WHERE NOT published AND (($1<>'' AND id::text=$1) OR ($1='' AND created_at<now()-interval '5 seconds')) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`, id).Scan(&taskID, &b)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var cmd domain.Command
	if err = json.Unmarshal(b, &cmd); err != nil {
		return false, err
	}
	if err = send(ctx, cmd); err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, "UPDATE tasks SET published=true WHERE id=$1", taskID)
	if err == nil {
		err = tx.Commit(ctx)
	}
	return true, err
}

// Apply preserves early terminal messages in the inbox, then applies them after
// in_progress. Dedupe and both state changes are committed atomically.
func (s *Store) Apply(ctx context.Context, eventID string, r domain.Result) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	t, err := scanTenant(tx.QueryRow(ctx, "SELECT "+tenantColumns+" FROM tenants WHERE id=$1 FOR UPDATE", r.TenantID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Problem("invalid_event")
	}
	if err != nil {
		return err
	}
	k, err := scanTask(tx.QueryRow(ctx, "SELECT "+taskColumns+" FROM tasks WHERE id=$1 FOR UPDATE", r.TaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Problem("invalid_event")
	}
	if err != nil {
		return err
	}
	if k.TenantID != r.TenantID || k.Type != r.Type {
		return Problem("invalid_event")
	}
	b, _ := json.Marshal(r)
	tag, err := tx.Exec(ctx, "INSERT INTO inbox(id,task_id,result) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", eventID, k.ID, b)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		slog.Debug("duplicate result ignored", "task_id", k.ID, "status", r.Status)
		return tx.Commit(ctx)
	}
	if k.Status == "done" || k.Status == "failed" {
		_, err = tx.Exec(ctx, "UPDATE inbox SET applied=true WHERE id=$1", eventID)
		if err != nil {
			return err
		}
		slog.Debug("stale result ignored", "task_id", k.ID, "status", r.Status, "terminal", k.Status)
		return tx.Commit(ctx)
	}
	if k.Status == "accepted" && r.Status != "in_progress" {
		// A terminal result that outran its progress report is durable in the
		// inbox; it is applied the moment in_progress arrives.
		slog.Info("terminal result buffered ahead of progress", "task_id", k.ID, "task_type", k.Type, "status", r.Status)
		return tx.Commit(ctx)
	}
	if r.Status == "in_progress" && k.Status == "accepted" {
		k.Status = "in_progress"
		k.UpdatedAt = domain.Now()
		_, err = tx.Exec(ctx, "UPDATE tasks SET status='in_progress',updated_at=now() WHERE id=$1", k.ID)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, "UPDATE inbox SET applied=true WHERE task_id=$1 AND result->>'status'='in_progress'", k.ID)
	if err != nil {
		return err
	}
	var terminal []byte
	err = tx.QueryRow(ctx, "SELECT result FROM inbox WHERE task_id=$1 AND NOT applied AND result->>'status' IN ('done','failed') ORDER BY received_at,id LIMIT 1", k.ID).Scan(&terminal)
	if errors.Is(err, pgx.ErrNoRows) {
		if r.Status == "in_progress" {
			slog.Info("task transition", "task_id", k.ID, "task_type", k.Type, "status", "in_progress")
		}
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	if err = json.Unmarshal(terminal, &r); err != nil {
		return err
	}
	next, err := tenant.ApplyTaskOutcome(tenant.Status(t.Status), tenant.TaskType(k.Type), tenant.TaskStatus(r.Status))
	if err != nil {
		return fmt.Errorf("inconsistent tenant/task: %w", err)
	}
	var raw []byte
	if err = tx.QueryRow(ctx, "SELECT command FROM tasks WHERE id=$1", k.ID).Scan(&raw); err != nil {
		return err
	}
	var cmd domain.Command
	if err = json.Unmarshal(raw, &cmd); err != nil {
		return err
	}
	name := t.Name
	if r.Status == "done" && cmd.PendingName != nil {
		name = *cmd.PendingName
	}
	t, err = scanTenant(tx.QueryRow(ctx, "UPDATE tenants SET name=$2,status=$3,version=version+1,updated_at=now() WHERE id=$1 RETURNING "+tenantColumns, t.ID, name, string(next)))
	if err != nil {
		return err
	}
	k, err = scanTask(tx.QueryRow(ctx, "UPDATE tasks SET status=$2,updated_at=now() WHERE id=$1 RETURNING "+taskColumns, k.ID, r.Status))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "UPDATE inbox SET applied=true WHERE task_id=$1", k.ID)
	if err != nil {
		return err
	}
	// One line per visible state change: the task outcome and the tenant
	// status it produced, plus the sanitized worker code when it failed.
	slog.Info("task transition", "task_id", k.ID, "task_type", k.Type, "status", k.Status, "tenant_id", t.ID, "tenant_status", t.Status, "version", t.Version, "code", r.ErrorCode)
	return tx.Commit(ctx)
}
func (s *Store) Heartbeat(ctx context.Context, w domain.Worker) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO workers(id,last_seen,current_task) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen,current_task=excluded.current_task WHERE workers.last_seen<excluded.last_seen`, w.ID, w.LastSeen.Time, w.CurrentTask)
	return err
}
func (s *Store) Workers(ctx context.Context) ([]domain.Worker, error) {
	v := []domain.Worker{}
	rows, err := s.DB.Query(ctx, "SELECT id,last_seen,current_task,last_seen>now()-interval '15 seconds' FROM workers ORDER BY id")
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var w domain.Worker
		if err = rows.Scan(&w.ID, &w.LastSeen.Time, &w.CurrentTask, &w.Online); err != nil {
			return v, err
		}
		v = append(v, w)
	}
	return v, rows.Err()
}
