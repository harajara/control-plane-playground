// Package secretstore keeps tenant infrastructure documents in the filesystem
// of a Docker volume. It is deliberately small: one JSON document per secret
// id, atomic replacement, no plaintext in the control-plane database and no
// network key service to lose state on restart.
package secretstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ErrMissing means the document does not exist.
var ErrMissing = errors.New("secret not found")

// Environment configuration for the volume-backed store.
const (
	EnvDir     = "SECRET_STORE_DIR"
	DefaultDir = "/data/secrets"
)

// document is what one secret looks like on disk. version carries the caller's
// idempotency label (the same token the task command is keyed by) so operators
// can tell which attempt wrote the document.
type document struct {
	ID       string          `json:"id"`
	Version  string          `json:"version"`
	Modified time.Time       `json:"modified_at"`
	Value    json.RawMessage `json:"value"`
}

// Store reads and writes secret documents below a root directory.
type Store struct{ root string }

// Open prepares the root directory. Permissions are 0700 because documents
// hold tenant credentials.
func Open(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("secret store directory is required")
	}
	if !filepath.IsAbs(root) {
		abs, err := filepath.Abs(root)
		if err != nil {
			return nil, err
		}
		root = abs
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	// Existence is not enough: a volume mounted over a directory the runtime
	// user cannot write would fail the first tenant instead of now.
	probe := filepath.Join(root, ".write-probe")
	if err := os.WriteFile(probe, []byte("ok"), 0o600); err != nil {
		return nil, fmt.Errorf("secret store %s is not writable: %w", root, err)
	}
	if err := os.Remove(probe); err != nil {
		return nil, err
	}
	return &Store{root}, nil
}

// Root reports the directory backing this store.
func (s *Store) Root() string { return s.root }

// FromEnv opens the store at SECRET_STORE_DIR, defaulting to the compose
// volume mount.
func FromEnv() (*Store, error) {
	dir := os.Getenv(EnvDir)
	if dir == "" {
		dir = DefaultDir
	}
	return Open(dir)
}

// path maps a secret id to a file below the root. Ids are hierarchical
// ("control-plane/tenants/<slug>/infra"); anything that could escape the root
// is refused rather than sanitized.
func (s *Store) path(id string) (string, error) {
	if id == "" || strings.HasPrefix(id, "/") || strings.ContainsRune(id, '\\') || filepath.IsAbs(id) {
		return "", fmt.Errorf("invalid secret id %q", id)
	}
	parts := strings.Split(id, "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return "", fmt.Errorf("invalid secret id %q", id)
		}
	}
	p := filepath.Join(append([]string{s.root}, parts...)...) + ".json"
	if p != s.root && !strings.HasPrefix(p, s.root+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid secret id %q", id)
	}
	return p, nil
}

// Get decodes the document's value into v.
func (s *Store) Get(_ context.Context, id string, v any) error {
	p, err := s.path(id)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return ErrMissing
	}
	if err != nil {
		return err
	}
	var d document
	if err = json.Unmarshal(b, &d); err != nil {
		return fmt.Errorf("secret %s is corrupt: %w", id, err)
	}
	return json.Unmarshal(d.Value, v)
}

// Put writes the value atomically. version is stored with the document.
func (s *Store) Put(_ context.Context, id, version string, v any) error {
	if _, err := uuid.Parse(version); err != nil {
		return fmt.Errorf("secret version must be a UUID, got %q", version)
	}
	value, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b, err := json.Marshal(document{ID: id, Version: version, Modified: time.Now().UTC(), Value: value})
	if err != nil {
		return err
	}
	p, err := s.path(id)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	tmp := p + ".tmp-" + version
	if err = os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err = os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// Delete removes a secret. A missing document is not an error, so destroy
// tasks stay replayable.
func (s *Store) Delete(_ context.Context, id string) error {
	p, err := s.path(id)
	if err != nil {
		return err
	}
	if err = os.Remove(p); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
