//go:build bdd

package bdd

// The fixtures both feature files share. A scenario asks for a tenant in a state
// and gets one, whether that means provisioning it for real, driving it to
// failure with an injected error rate, or importing a database the control
// plane did not create. Each helper is idempotent within a scenario: asking
// twice for the same tenant reuses the same row.

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ensureActiveTenant creates a tenant and waits until it is active. Repeated
// calls in one scenario reuse the same tenant.
func ensureActiveTenant(s *state, placeholder string) (*tracked, error) {
	t := s.track(placeholder)
	if t.ID != "" {
		v, err := s.api.Tenant(t.ID)
		if err != nil {
			return nil, err
		}
		if v.Status == "active" {
			t.Status, t.Version = v.Status, v.Version
			return t, nil
		}
	}
	res, err := s.api.Post("/v1/tenants", map[string]string{"slug": t.Slug, "name": "Fixture tenant"})
	if err != nil {
		return nil, err
	}
	if res.Status != http.StatusAccepted {
		return nil, fmt.Errorf("fixture create: %s", res.Error())
	}
	var m Mutation
	if err := res.Decode(&m); err != nil {
		return nil, err
	}
	s.adopt(placeholder, &m)
	v, err := s.api.WaitTenant(m.Tenant.ID, 3*time.Minute, "active", "failed")
	if err != nil {
		return nil, err
	}
	if v.Status != "active" {
		return nil, fmt.Errorf("fixture tenant ended %q instead of active", v.Status)
	}
	t.Status, t.Version = v.Status, v.Version
	s.versionBefore = v.Version
	s.secretBefore = v.InfraSecretID
	deploy, err := t.lastTask("deploy")
	if err == nil {
		if tv, err := s.api.Task(deploy.ID); err == nil {
			copied := *tv
			t.Tasks["deploy"] = &copied
		}
	}
	return t, nil
}

// ensureFailedTenant drives a tenant to failed with fail rate 1, then restores
// normal workers: an operator who fixed the outage can destroy the tenant, and
// the scenario that follows needs working workers.
func ensureFailedTenant(s *state, placeholder string) (*tracked, error) {
	if !s.failRate {
		if err := setWorkerFailRate("1"); err != nil {
			return nil, err
		}
	}
	t, err := createTenantNow(s, placeholder, "Failing tenant")
	if err != nil {
		return nil, err
	}
	v, err := s.api.WaitTenant(t.ID, 3*time.Minute, "failed", "active")
	if err != nil {
		return nil, err
	}
	if v.Status != "failed" {
		return nil, fmt.Errorf("expected the injected failure to leave the tenant failed, got %q", v.Status)
	}
	t.Status, t.Version = v.Status, v.Version
	return t, setWorkerFailRate("0")
}

// createTenantNow is the plain POST used by fixtures.
func createTenantNow(s *state, placeholder, name string) (*tracked, error) {
	t := s.track(placeholder)
	res, err := s.api.Post("/v1/tenants", map[string]string{"slug": t.Slug, "name": name})
	if err != nil {
		return nil, err
	}
	if res.Status != http.StatusAccepted {
		return nil, fmt.Errorf("create tenant: %s", res.Error())
	}
	var m Mutation
	if err := res.Decode(&m); err != nil {
		return nil, err
	}
	s.adopt(placeholder, &m)
	s.versionBefore = m.Tenant.Version
	return t, nil
}

// patchDatabaseURL sends the PATCH and keeps the response for the next step.
func patchDatabaseURL(s *state, t *tracked, url string) error {
	res, err := s.api.Patch("/v1/tenants/"+t.ID, map[string]any{"version": t.Version, "database_url": url})
	if err != nil {
		return err
	}
	s.last = res
	if res.Status != http.StatusAccepted {
		return fmt.Errorf("import database url: %s", res.Error())
	}
	var m Mutation
	if err := res.Decode(&m); err != nil {
		return err
	}
	s.adopt(t.Placeholder, &m)
	t.Status, t.Version = m.Tenant.Status, m.Tenant.Version
	return nil
}

// Helpers shared by both feature files.

// currentScene returns the scenario in flight without taking a lock that could
// deadlock an After hook; it is only used once, when the scenario is being
// detached for cleanup.
func currentScene() *state {
	mu.Lock()
	defer mu.Unlock()
	return current
}

func splitList(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func statusLine(v []*Response) string {
	parts := make([]string, 0, len(v))
	for _, r := range v {
		parts = append(parts, fmt.Sprint(r.Status))
	}
	return strings.Join(parts, " ")
}

func lastError(s *state) string {
	if s.last == nil {
		return "no response recorded"
	}
	return s.last.Error()
}

// hasInfraDocument reports whether a tenant secret exists at all.
func hasInfraDocument(slug string) (bool, error) {
	_, err := readInfra(slug)
	if err != nil && strings.Contains(err.Error(), "no infrastructure document") {
		return false, nil
	}
	return err == nil, nil
}
