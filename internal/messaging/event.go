package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	"tenant-control-plane/internal/domain"
)

const Commands = "tenant.tasks.v1"
const Results = "tenant.task-updates.v1"
const Heartbeats = "workers.heartbeats.v1"

var ErrPoison = errors.New("invalid event")

type Signer interface {
	MAC(context.Context, []byte) ([]byte, error)
	Verify(context.Context, []byte, []byte) (bool, error)
}
type Event struct {
	SchemaVersion int             `json:"schema_version"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Source        string          `json:"source"`
	Time          domain.Time     `json:"time"`
	TenantID      string          `json:"tenant_id,omitempty"`
	Data          json.RawMessage `json:"data"`
	Token         string          `json:"token,omitempty"`
}

func New(id, kind, source, tenantID string, data any) Event {
	b, _ := json.Marshal(data)
	return Event{SchemaVersion: 1, ID: id, Type: kind, Source: source, Time: domain.Now(), TenantID: tenantID, Data: b}
}
func ResultID(task, status string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(task+"|"+status)).String()
}

type claims struct {
	ID     string `json:"jti"`
	Digest string `json:"sha256"`
}

const header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"

func digest(e Event) string {
	e.Token = ""
	b, _ := json.Marshal(e)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// These JWTs are durable message signatures, not expiring access tokens. Binding
// the complete payload prevents substituting data under an otherwise valid JWT.
func Sign(ctx context.Context, s Signer, e Event) ([]byte, error) {
	b, _ := json.Marshal(claims{e.ID, digest(e)})
	input := header + "." + base64.RawURLEncoding.EncodeToString(b)
	mac, err := s.MAC(ctx, []byte(input))
	if err != nil {
		return nil, err
	}
	e.Token = input + "." + base64.RawURLEncoding.EncodeToString(mac)
	return json.Marshal(e)
}
func Decode(ctx context.Context, s Signer, b []byte) (Event, error) {
	var e Event
	if len(b) > 1<<20 {
		return e, ErrPoison
	}
	if err := json.Unmarshal(b, &e); err != nil {
		return e, ErrPoison
	}
	if e.SchemaVersion != 1 || e.Time.IsZero() {
		return e, ErrPoison
	}
	if _, err := uuid.Parse(e.ID); err != nil {
		return e, ErrPoison
	}
	p := strings.Split(e.Token, ".")
	if len(p) != 3 || p[0] != header {
		return e, ErrPoison
	}
	raw, err := base64.RawURLEncoding.DecodeString(p[1])
	if err != nil {
		return e, ErrPoison
	}
	var c claims
	if json.Unmarshal(raw, &c) != nil || c.ID != e.ID || c.Digest != digest(e) {
		return e, ErrPoison
	}
	mac, err := base64.RawURLEncoding.DecodeString(p[2])
	if err != nil {
		return e, ErrPoison
	}
	ok, err := s.Verify(ctx, []byte(p[0]+"."+p[1]), mac)
	if err != nil {
		return e, err
	}
	if !ok {
		return e, ErrPoison
	}
	return e, nil
}
