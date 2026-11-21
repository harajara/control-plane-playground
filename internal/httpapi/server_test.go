package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"tenant-control-plane/internal/domain"
)

func TestTenantResponseContainsOnlyMetadataAndSecretReference(t *testing.T) {
	v := domain.Tenant{ID: "id", InfraSecretID: "control-plane/tenants/acme/infra"}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err = json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 8 {
		t.Fatalf("unexpected fields: %s", b)
	}
	for _, k := range []string{"database_url", "password", "host", "managed", "env", "masked_url"} {
		if _, ok := fields[k]; ok {
			t.Fatalf("infrastructure leaked: %s", k)
		}
	}
}
func TestProblemContract(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/tenants/absent", nil)
	fail(w, r, 404, "tenant_not_found", "Tenant not found.")
	var p Problem
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || w.Header().Get("Content-Type") != "application/problem+json" || p.Code != "tenant_not_found" || p.Status != 404 {
		t.Fatalf("invalid problem: %s", w.Body.String())
	}
}
func TestPasswordHashAndVerification(t *testing.T) {
	a := Password("a sufficiently long test password")
	b := Password("a sufficiently long test password")
	if a == b {
		t.Fatal("password salt reused")
	}
	if !checkPassword("a sufficiently long test password", a) || checkPassword("wrong", a) || checkPassword("anything", "invalid") {
		t.Fatal("password verification failed")
	}
}
