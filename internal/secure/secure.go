// Package secure holds the key material the control plane and workers share
// trust through. Everything comes from the environment: one HMAC key signs
// broker envelopes, and one NaCl box keypair seals imported connection
// strings so that only workers can read them.
package secure

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/nacl/box"
)

// Environment names. CONTROL_PLANE_RECEIVE_KEY is the public half of the
// worker keypair: the control plane seals an imported database URL to it, so
// the holder of WORKER_SEAL_KEY is the only party that can open it again.
const (
	EnvM2MSecret              = "M2M_SECRET"
	EnvWorkerSealKey          = "WORKER_SEAL_KEY"
	EnvControlPlaneReceiveKey = "CONTROL_PLANE_RECEIVE_KEY"
)

// keyVersion prefixes every sealed plaintext. Sealed URLs live inside
// immutable task commands, so a blob written under one key must never be
// silently readable after rotation; the tag makes rotation observable.
const keyVersion = "v1"

// ErrKey is returned when configured key material is unusable.
var ErrKey = errors.New("invalid key")

// Signer implements messaging.Signer with HMAC-SHA256 over a shared key. It is
// deterministic across restarts, which is exactly what broke when the queue's
// trust anchor was an emulator-held KMS alias.
type Signer struct{ key []byte }

// NewSigner takes the base64 form stored in the environment.
func NewSigner(secret string) (*Signer, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(secret))
	if err != nil {
		return nil, fmt.Errorf("%s must be base64: %w", EnvM2MSecret, err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("%s must decode to 32 bytes: %w", EnvM2MSecret, ErrKey)
	}
	return &Signer{key}, nil
}

// SignerFromEnv builds the signer from M2M_SECRET.
func SignerFromEnv() (*Signer, error) {
	v, ok := os.LookupEnv(EnvM2MSecret)
	if !ok || v == "" {
		return nil, fmt.Errorf("%s is required; run task setup", EnvM2MSecret)
	}
	return NewSigner(v)
}

// MAC signs the message with HMAC-SHA256.
func (s *Signer) MAC(_ context.Context, b []byte) ([]byte, error) {
	m := hmac.New(sha256.New, s.key)
	if _, err := m.Write(b); err != nil {
		return nil, err
	}
	return m.Sum(nil), nil
}

// Verify reports whether mac is the signature of b. hmac.Equal is
// constant-time for equal-length inputs.
func (s *Signer) Verify(_ context.Context, b, mac []byte) (bool, error) {
	m := hmac.New(sha256.New, s.key)
	if _, err := m.Write(b); err != nil {
		return false, err
	}
	return hmac.Equal(m.Sum(nil), mac), nil
}

// SealKeyPair is a worker seal keypair, hex encoded for .env.
type SealKeyPair struct {
	WorkerSecret     string
	ControlPlaneOpen string
}

// GenerateSealKeyPair makes a fresh NaCl box keypair.
func GenerateSealKeyPair() (SealKeyPair, error) {
	pub, priv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return SealKeyPair{}, err
	}
	return SealKeyPair{hex.EncodeToString(priv[:]), hex.EncodeToString(pub[:])}, nil
}

func keyFromHex(value string) (*[32]byte, error) {
	b, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("must be hexadecimal: %w", ErrKey)
	}
	if len(b) != 32 {
		return nil, fmt.Errorf("must be 32 bytes: %w", ErrKey)
	}
	k := new([32]byte)
	copy(k[:], b)
	return k, nil
}

// Sealer seals an imported database URL so only workers can open it. It
// satisfies httpapi.Sealer.
type Sealer struct{ recipient *[32]byte }

// NewSealer takes the worker public key (hex).
func NewSealer(publicKey string) (*Sealer, error) {
	k, err := keyFromHex(publicKey)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", EnvControlPlaneReceiveKey, err)
	}
	return &Sealer{k}, nil
}

// SealerFromEnv builds the sealer from CONTROL_PLANE_RECEIVE_KEY.
func SealerFromEnv() (*Sealer, error) {
	v, ok := os.LookupEnv(EnvControlPlaneReceiveKey)
	if !ok || v == "" {
		return nil, fmt.Errorf("%s is required; run task setup", EnvControlPlaneReceiveKey)
	}
	return NewSealer(v)
}

// Seal returns a base64 anonymous-sealed box binding plaintext to the worker
// keypair. Each call uses a fresh ephemeral key, so equal URLs never produce
// equal blobs and nothing about a tenant's connection string leaks into logs.
func (s *Sealer) Seal(_ context.Context, plaintext string) (string, error) {
	if plaintext == "" {
		return "", errors.New("nothing to seal")
	}
	ct, err := box.SealAnonymous(nil, []byte(keyVersion+":"+plaintext), s.recipient, rand.Reader)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ct), nil
}

// Opener is the worker half of the seal keypair. It satisfies
// worker.Opener for sealed command URLs.
type Opener struct{ public, secret *[32]byte }

// NewOpener takes the worker private key and its matching public key.
func NewOpener(secret, publicKey string) (*Opener, error) {
	priv, err := keyFromHex(secret)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", EnvWorkerSealKey, err)
	}
	pub, err := keyFromHex(publicKey)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", EnvControlPlaneReceiveKey, err)
	}
	// A generated pair is confirmed by a round trip: sealing to the public key
	// and opening with the private one must return the probe. Misconfigured
	// environments therefore fail at startup, not on a customer's PATCH.
	probe := "pair-check"
	ct, err := box.SealAnonymous(nil, []byte(probe), pub, rand.Reader)
	if err != nil {
		return nil, err
	}
	if plain, ok := box.OpenAnonymous(nil, ct, pub, priv); !ok || string(plain) != probe {
		return nil, errors.New(EnvWorkerSealKey + " and " + EnvControlPlaneReceiveKey + " are not a pair")
	}
	return &Opener{pub, priv}, nil
}

// OpenerFromEnv builds the opener from the two seal key variables.
func OpenerFromEnv() (*Opener, error) {
	secret, ok := os.LookupEnv(EnvWorkerSealKey)
	if !ok || secret == "" {
		return nil, fmt.Errorf("%s is required; run task setup", EnvWorkerSealKey)
	}
	public, ok := os.LookupEnv(EnvControlPlaneReceiveKey)
	if !ok || public == "" {
		return nil, fmt.Errorf("%s is required; run task setup", EnvControlPlaneReceiveKey)
	}
	return NewOpener(secret, public)
}

// Open decrypts a sealed value and returns the original plaintext. A value
// sealed under a different key version is refused rather than returned.
func (o *Opener) Open(_ context.Context, sealed string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(sealed))
	if err != nil {
		return "", errors.New("sealed value is not base64")
	}
	plain, ok := box.OpenAnonymous(nil, b, o.public, o.secret)
	if !ok {
		return "", errors.New("sealed value does not open")
	}
	tag := keyVersion + ":"
	if len(plain) < len(tag) || string(plain[:len(tag)]) != tag {
		return "", errors.New("sealed value was produced under a different key version")
	}
	return string(plain[len(tag):]), nil
}
