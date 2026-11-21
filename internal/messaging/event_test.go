package messaging

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

type localMAC struct{}

func (localMAC) MAC(_ context.Context, b []byte) ([]byte, error) {
	h := hmac.New(sha256.New, []byte("unit-test-only"))
	_, _ = h.Write(b)
	return h.Sum(nil), nil
}
func (m localMAC) Verify(ctx context.Context, b, sig []byte) (bool, error) {
	want, _ := m.MAC(ctx, b)
	return hmac.Equal(want, sig), nil
}
func TestSignatureBindsCompleteMessage(t *testing.T) {
	ctx := context.Background()
	s := localMAC{}
	e := New(uuid.NewString(), "task.deploy", "control-plane", uuid.NewString(), map[string]string{"value": "original"})
	raw, err := Sign(ctx, s, e)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(ctx, s, raw)
	if err != nil || got.ID != e.ID {
		t.Fatalf("round trip: %v", err)
	}
	for _, change := range []func(*Event){func(v *Event) { v.Data = json.RawMessage(`{"value":"changed"}`) }, func(v *Event) { v.TenantID = uuid.NewString() }, func(v *Event) { v.Type = "task.destroy" }, func(v *Event) { v.ID = uuid.NewString() }} {
		v := got
		change(&v)
		b, _ := json.Marshal(v)
		if _, err = Decode(ctx, s, b); !errors.Is(err, ErrPoison) {
			t.Fatal("tampered message accepted")
		}
	}
}
func TestStableProgressIdentity(t *testing.T) {
	id := uuid.NewString()
	if ResultID(id, "done") != ResultID(id, "done") {
		t.Fatal("unstable ID")
	}
	if ResultID(id, "done") == ResultID(id, "failed") {
		t.Fatal("colliding outcomes")
	}
}
