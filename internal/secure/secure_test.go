package secure

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"testing"
)

func newSigner(t *testing.T, mark byte) *Signer {
	t.Helper()
	key := make([]byte, 32)
	key[0] = mark
	s, err := NewSigner(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSignerRejectsBadKeyMaterial(t *testing.T) {
	for _, bad := range []string{"", "not-base64!!", base64.StdEncoding.EncodeToString(make([]byte, 31)), base64.StdEncoding.EncodeToString(make([]byte, 33))} {
		if _, err := NewSigner(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func TestMACRoundTripAndTamperDetection(t *testing.T) {
	ctx := context.Background()
	s := newSigner(t, 1)
	msg := []byte("eyJhbGciOiJIUzI1NiJ9.payload")
	mac, err := s.MAC(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if len(mac) != 32 {
		t.Fatalf("mac length %d", len(mac))
	}
	if ok, err := s.Verify(ctx, msg, mac); err != nil || !ok {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if ok, _ := s.Verify(ctx, []byte("eyJhbGciOiJIUzI1NiJ9.payloaD"), mac); ok {
		t.Fatal("tampered payload accepted")
	}
	if ok, _ := newSigner(t, 2).Verify(ctx, msg, mac); ok {
		t.Fatal("a different key accepted a foreign signature")
	}
	// Determinism is the point: the same key and bytes must sign identically
	// across processes, which a restart-sensitive KMS alias could not promise.
	second, err := s.MAC(ctx, msg)
	if err != nil || !bytes.Equal(mac, second) {
		t.Fatal("signing is not deterministic")
	}
	if ok, _ := s.Verify(ctx, msg, nil); ok {
		t.Fatal("empty signature accepted")
	}
}

func TestSealOpenRoundTrip(t *testing.T) {
	ctx := context.Background()
	pair, err := GenerateSealKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := NewSealer(pair.ControlPlaneOpen)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(pair.WorkerSecret, pair.ControlPlaneOpen)
	if err != nil {
		t.Fatal(err)
	}
	raw := "postgres://user:s3cr3t@tenant-db:5432/imported?sslmode=disable"
	sealed, err := sealer.Seal(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sealed, "s3cr3t") || strings.Contains(sealed, "imported") {
		t.Fatal("sealed blob leaks the connection string")
	}
	opened, err := opener.Open(ctx, sealed)
	if err != nil || opened != raw {
		t.Fatalf("open: %v %q", err, opened)
	}
	// A fresh ephemeral key per call means the same URL never repeats.
	again, err := sealer.Seal(ctx, raw)
	if err != nil || again == sealed {
		t.Fatalf("sealing leaked identity: %v", err)
	}
	if _, err := sealer.Seal(ctx, ""); err == nil {
		t.Fatal("sealed nothing")
	}
}

func TestSealKeyPairMismatchFailsFast(t *testing.T) {
	a, _ := GenerateSealKeyPair()
	b, _ := GenerateSealKeyPair()
	if _, err := NewOpener(a.WorkerSecret, b.ControlPlaneOpen); err == nil {
		t.Fatal("mismatched keypair accepted")
	}
	for _, bad := range []string{"", "zz", strings.Repeat("ab", 31), strings.Repeat("ab", 33)} {
		if _, err := NewSealer(bad); err == nil {
			t.Fatalf("accepted public key %q", bad)
		}
		if _, err := NewOpener(bad, bad); err == nil {
			t.Fatalf("accepted private key %q", bad)
		}
	}
}

func TestOpenRefusesForeignAndCorruptBlobs(t *testing.T) {
	ctx := context.Background()
	a, _ := GenerateSealKeyPair()
	b, _ := GenerateSealKeyPair()
	sealer, err := NewSealer(a.ControlPlaneOpen)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealer.Seal(ctx, "postgres://x:y@h:5432/d")
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewOpener(b.WorkerSecret, b.ControlPlaneOpen)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Open(ctx, sealed); err == nil {
		t.Fatal("opened a blob sealed to another key")
	}
	opener, _ := NewOpener(a.WorkerSecret, a.ControlPlaneOpen)
	for _, bad := range []string{"not base64!!", "", base64.StdEncoding.EncodeToString(make([]byte, 8))} {
		if _, err := opener.Open(ctx, bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	// Flipping a ciphertext byte must fail the authentication tag.
	raw, _ := base64.StdEncoding.DecodeString(sealed)
	raw[len(raw)-1] ^= 1
	if _, err := opener.Open(ctx, base64.StdEncoding.EncodeToString(raw)); err == nil {
		t.Fatal("accepted a tampered blob")
	}
}

func TestEnvHelpersRequireKeys(t *testing.T) {
	t.Setenv(EnvM2MSecret, "")
	if _, err := SignerFromEnv(); err == nil {
		t.Fatal("missing M2M_SECRET accepted")
	}
	t.Setenv(EnvM2MSecret, base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if _, err := SignerFromEnv(); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvControlPlaneReceiveKey, "")
	if _, err := SealerFromEnv(); err == nil {
		t.Fatal("missing seal public key accepted")
	}
	pair, _ := GenerateSealKeyPair()
	t.Setenv(EnvControlPlaneReceiveKey, pair.ControlPlaneOpen)
	if _, err := SealerFromEnv(); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvWorkerSealKey, "")
	if _, err := OpenerFromEnv(); err == nil {
		t.Fatal("missing worker secret accepted")
	}
	t.Setenv(EnvWorkerSealKey, pair.WorkerSecret)
	if o, err := OpenerFromEnv(); err != nil || o == nil {
		t.Fatalf("OpenerFromEnv: %v", err)
	}
}
