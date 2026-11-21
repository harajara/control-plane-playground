//go:build integration

package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/messaging"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("TEST_DATABASE_URL required; run task test:integration")
	}
	// Each test gets a separate schema; live API/relay instances cannot consume
	// integration fixtures from the application's public schema.
	admin, err := Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + uuid.NewString()[:8]
	if _, err = admin.DB.Exec(context.Background(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	parsed, err := parseTestURL(url, schema)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), parsed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.DB.Close()
		_, _ = admin.DB.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.DB.Close()
	})
	return s
}
func parseTestURL(raw, schema string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
func newTenant(t *testing.T, s *Store) (domain.Tenant, domain.Task) {
	t.Helper()
	v, k, err := s.Create(context.Background(), "test-"+uuid.NewString()[:12], "Integration tenant")
	if err != nil {
		t.Fatal(err)
	}
	return v, k
}
func apply(t *testing.T, s *Store, k domain.Task, status string) {
	t.Helper()
	if err := s.Apply(context.Background(), messaging.ResultID(k.ID, status), domain.Result{TaskID: k.ID, TenantID: k.TenantID, Type: k.Type, Status: status}); err != nil {
		t.Fatal(err)
	}
}
func TestConcurrentCreateAndPatch(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	slug := "race-" + uuid.NewString()[:12]
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := s.Create(ctx, slug, "Race")
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, Exists) {
				t.Errorf("unexpected create error: %v", err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("create successes=%d", successes.Load())
	}
	v, k := newTenant(t, s)
	apply(t, s, k, "in_progress")
	apply(t, s, k, "done")
	v, err := s.Tenant(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	successes.Store(0)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := "Renamed"
			_, _, err := s.Mutate(ctx, v.ID, "update", v.Version, &name, "")
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, Conflict) {
				t.Errorf("unexpected patch error: %v", err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("patch successes=%d", successes.Load())
	}
}
func TestEarlyTerminalDuplicateAndLateProgress(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	v, k := newTenant(t, s)
	apply(t, s, k, "done")
	pending, err := s.Task(ctx, k.ID)
	if err != nil || pending.Status != "accepted" {
		t.Fatalf("terminal must wait: %+v %v", pending, err)
	}
	apply(t, s, k, "in_progress")
	active, err := s.Tenant(ctx, v.ID)
	if err != nil || active.Status != "active" || active.Version != 2 {
		t.Fatalf("early terminal lost: %+v %v", active, err)
	}
	apply(t, s, k, "done")
	if err = s.Apply(ctx, uuid.NewString(), domain.Result{TaskID: k.ID, TenantID: v.ID, Type: "deploy", Status: "in_progress"}); err != nil {
		t.Fatal(err)
	}
	after, _ := s.Tenant(ctx, v.ID)
	if after.Version != active.Version || after.Status != "active" {
		t.Fatal("duplicate changed state")
	}
}
func TestOutboxCommitRetryAndRollback(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	v, k := newTenant(t, s)
	if _, _, err := s.Create(ctx, v.Slug, "Duplicate"); !errors.Is(err, Exists) {
		t.Fatal(err)
	}
	var count int
	if err := s.DB.QueryRow(ctx, "SELECT count(*) FROM tasks WHERE tenant_id=$1", v.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("rolled back request emitted task: %d %v", count, err)
	}
	_, err := s.PublishOne(ctx, k.ID, func(context.Context, domain.Command) error { return errors.New("broker offline") })
	if err == nil {
		t.Fatal("expected publish failure")
	}
	var published bool
	if err = s.DB.QueryRow(ctx, "SELECT published FROM tasks WHERE id=$1", k.ID).Scan(&published); err != nil || published {
		t.Fatal("failed publish marked published")
	}
	var calls atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.PublishOne(ctx, k.ID, func(_ context.Context, c domain.Command) error {
				if c.Task.ID != k.ID {
					t.Error("changed event identity")
				}
				calls.Add(1)
				time.Sleep(10 * time.Millisecond)
				return nil
			})
			if e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent relays=%d", calls.Load())
	}
}
func TestWorkersArePrunedAfterRetention(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for _, id := range []string{"worker-1", "worker-2", "retired-worker"} {
		if err := s.Heartbeat(ctx, domain.Worker{ID: id, LastSeen: domain.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	// Age one worker past the retention window, exactly as the clock would.
	if _, err := s.DB.Exec(ctx, "UPDATE workers SET last_seen=now()-make_interval(secs=>$2) WHERE id=$1", "retired-worker", int64((WorkerRetention + time.Minute).Seconds())); err != nil {
		t.Fatal(err)
	}
	v, err := s.Workers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 {
		t.Fatalf("workers=%d, want the two live ones: %+v", len(v), v)
	}
	for _, w := range v {
		if w.ID == "retired-worker" {
			t.Fatal("a retired worker is still listed")
		}
		if !w.Online {
			t.Fatalf("fresh worker %s reported offline", w.ID)
		}
	}
	// Reading again must not drop anything that is still beating.
	again, err := s.Workers(ctx)
	if err != nil || len(again) != 2 {
		t.Fatalf("second read: %d workers, %v", len(again), err)
	}
}

func TestDestroyTombstoneAndGuards(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	v, k := newTenant(t, s)
	if _, _, err := s.Mutate(ctx, v.ID, "destroy", 0, nil, ""); !errors.Is(err, NotAllowed) {
		t.Fatal("destroy accepted during provision")
	}
	apply(t, s, k, "in_progress")
	apply(t, s, k, "failed")
	v, k, err := s.Mutate(ctx, v.ID, "destroy", 0, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	apply(t, s, k, "in_progress")
	apply(t, s, k, "done")
	v, err = s.Tenant(ctx, v.ID)
	if err != nil || v.Status != "destroyed" {
		t.Fatal("destroy failed")
	}
	if _, _, err = s.Create(ctx, v.Slug, "Reused"); !errors.Is(err, Exists) {
		t.Fatal("tombstoned slug reused")
	}
}
