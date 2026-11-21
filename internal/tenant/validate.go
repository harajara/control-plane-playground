package tenant

import (
	"errors"
	"regexp"
	"strings"
)

var (
	ErrInvalidSlug = errors.New("slug must be 3-28 chars matching ^[a-z][-a-z0-9]{1,26}[a-z0-9]$")
	ErrInvalidName = errors.New("name must not be empty")
)

var slugPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

func ValidateSlug(slug string) error {
	if !slugPattern.MatchString(slug) {
		return ErrInvalidSlug
	}
	return nil
}

func ValidateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return ErrInvalidName
	}
	return nil
}

// InfraSecretID is the Secrets Manager name that holds all infrastructure
// values for one tenant. The tenants table stores this ID, never the values.
func InfraSecretID(slug string) string {
	return "control-plane/tenants/" + slug + "/infra"
}
