package secretstore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type payload struct {
	Env map[string]string `json:"env"`
}

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutGetDeleteRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	id := "control-plane/tenants/acme-prod/infra"
	if err := s.Put(ctx, id, uuid.NewString(), payload{map[string]string{"DB_HOST": "tenant-db"}}); err != nil {
		t.Fatal(err)
	}
	var got payload
	if err := s.Get(ctx, id, &got); err != nil || got.Env["DB_HOST"] != "tenant-db" {
		t.Fatalf("get: %v %+v", err, got)
	}
	// Replacement keeps one document, not a pile of versions.
	if err := s.Put(ctx, id, uuid.NewString(), payload{map[string]string{"DB_HOST": "other"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Get(ctx, id, &got); err != nil || got.Env["DB_HOST"] != "other" {
		t.Fatalf("replace: %v %+v", err, got)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.Get(ctx, id, &got); !errors.Is(err, ErrMissing) {
		t.Fatalf("expected ErrMissing, got %v", err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatalf("delete of a missing secret must be idempotent: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.Root(), "control-plane")); err != nil {
		t.Fatal("secret tree disappeared")
	}
}

func TestMissingAndUnknownIDs(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	var got payload
	if err := s.Get(ctx, "control-plane/tenants/nobody/infra", &got); !errors.Is(err, ErrMissing) {
		t.Fatalf("missing: %v", err)
	}
	if err := s.Get(ctx, "control-plane/tenants", &got); !errors.Is(err, ErrMissing) {
		t.Fatalf("directory as id: %v", err)
	}
}

func TestSecretIdCannotEscapeRoot(t *testing.T) {
	s := open(t)
	var got payload
	for _, id := range []string{"", "/absolute/id", "../escape", "a/../../b", "a//b", "./x", `a\..\b`, "a/./b"} {
		if err := s.Put(context.Background(), id, uuid.NewString(), payload{}); err == nil {
			t.Fatalf("accepted id %q", id)
		}
		if err := s.Get(context.Background(), id, &got); err == nil {
			t.Fatalf("read accepted id %q", id)
		}
		if err := s.Delete(context.Background(), id); err == nil {
			t.Fatalf("delete accepted id %q", id)
		}
	}
}

func TestDocumentsArePrivateAndSelfDescribing(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	id := "control-plane/tenants/acme/infra"
	v := uuid.NewString()
	if err := s.Put(ctx, id, v, payload{map[string]string{"DATABASE_URL": "postgres://u:p@h/db"}}); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(s.Root(), filepath.FromSlash(id)) + ".json"
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var d document
	if err = json.Unmarshal(b, &d); err != nil {
		t.Fatal(err)
	}
	if d.ID != id || d.Version != v || d.Modified.IsZero() {
		t.Fatalf("document metadata: %+v", d)
	}
	if !strings.Contains(string(d.Value), "DATABASE_URL") {
		t.Fatal("value not stored")
	}
	// Windows NTFS ignores the mode bits; the guarantee matters on the Linux
	// runtime the containers use.
	if runtime.GOOS != "windows" {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("secret file permissions: %v %v", fi.Mode().Perm(), err)
		}
	}
	// A truncated document is reported as corrupt, not silently empty.
	if err = os.WriteFile(p, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = s.Get(ctx, id, &payload{}); err == nil || errors.Is(err, ErrMissing) {
		t.Fatalf("corrupt document: %v", err)
	}
}

func TestPutRejectsNonUUIDVersion(t *testing.T) {
	s := open(t)
	if err := s.Put(context.Background(), "control-plane/tenants/acme/infra", "attempt-1", payload{}); err == nil {
		t.Fatal("accepted a non-UUID version label")
	}
}

func TestOpenRequiresWritableRoot(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("accepted an empty root")
	}
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.Root() != dir {
		t.Fatalf("root %q != %q", s.Root(), dir)
	}
}
