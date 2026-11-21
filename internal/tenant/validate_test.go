package tenant

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateSlug(t *testing.T) {
	valid := []string{"acme", "a-b-c", "tenant-01", strings.Repeat("a", 1) + strings.Repeat("b", 26) + "c"}
	for _, s := range valid {
		if err := ValidateSlug(s); err != nil {
			t.Errorf("ValidateSlug(%q) = %v, want nil", s, err)
		}
	}

	invalid := []string{
		"",
		"ab",                                // too short
		strings.Repeat("a", 29),             // too long
		"Acme",                              // uppercase
		"1acme",                             // must start with a letter
		"acme-",                             // must not end with a dash
		"ac_me",                             // underscore not allowed
		"acme ",                             // space
		"a" + strings.Repeat("b", 27) + "c", // 29 chars
	}
	for _, s := range invalid {
		if err := ValidateSlug(s); !errors.Is(err, ErrInvalidSlug) {
			t.Errorf("ValidateSlug(%q) = %v, want ErrInvalidSlug", s, err)
		}
	}
}

func TestValidateName(t *testing.T) {
	if err := ValidateName("Acme Corp"); err != nil {
		t.Errorf("ValidateName(valid) = %v", err)
	}
	for _, n := range []string{"", "   ", "\t\n"} {
		if err := ValidateName(n); !errors.Is(err, ErrInvalidName) {
			t.Errorf("ValidateName(%q) = %v, want ErrInvalidName", n, err)
		}
	}
}

func TestInfraSecretID(t *testing.T) {
	if got, want := InfraSecretID("acme"), "control-plane/tenants/acme/infra"; got != want {
		t.Fatalf("InfraSecretID = %q, want %q", got, want)
	}
}
