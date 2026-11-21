//go:build bdd

package bdd

// Hand-written implementations of the API-contract steps in features/tenants.feature:
// listing and pagination, the tenant payload shape, the RFC 9457 problem
// contract, optimistic-concurrency races and authentication. The lifecycle
// and provisioning steps live in steps_tenants.go beside this file.

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// iAmNotAuthenticated drops the session so the next call has no credential.
func iAmNotAuthenticated(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	return s.api.Logout()
}

// clientsConcurrentlyCreateTenantNamed races n creates for the same slug.
func clientsConcurrentlyCreateTenantNamed(ctx context.Context, n int, placeholder, name string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t := s.track(placeholder)
	responses, err := s.api.Parallel(http.MethodPost, "/v1/tenants", map[string]string{"slug": t.Slug, "name": name}, n)
	if err != nil {
		return err
	}
	s.concurrent = responses
	// Keep the winner as the scenario's tenant so later steps have a subject.
	for _, r := range responses {
		if r.Status == http.StatusAccepted {
			var m Mutation
			if err := r.Decode(&m); err == nil {
				s.adopt(placeholder, &m)
				s.last = r
				break
			}
		}
	}
	if s.concurrent == nil {
		s.concurrent = responses
	}
	return nil
}

// iFetchTheTenant GETs the tenant in scope and keeps the raw response for the
// field assertions.
func iFetchTheTenant(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	res, err := s.api.Get("/v1/tenants/" + t.ID)
	if err != nil {
		return err
	}
	s.last = res
	return nil
}

// iFetchTenant fetches a tenant by identifier.
func iFetchTenant(ctx context.Context, id string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	res, err := s.api.Get("/v1/tenants/" + id)
	if err != nil {
		return err
	}
	s.last = res
	return nil
}

// iListTenants and its filtered twin list the collection.
func iListTenants(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	res, err := s.api.Get("/v1/tenants")
	if err != nil {
		return err
	}
	s.last = res
	return nil
}

func iListTenantsWithStatus(ctx context.Context, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	page, res, err := s.api.Tenants(status, "", 5)
	if err != nil {
		return err
	}
	s.last = res
	s.lastPage = page
	return nil
}

// tenantsExist creates n active tenants, each with its own slug.
func tenantsExist(ctx context.Context, n int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	s.batch = nil
	for i := 0; i < n; i++ {
		placeholder := fmt.Sprintf("batch-%d", i+1)
		t, err := ensureActiveTenant(s, placeholder)
		if err != nil {
			return err
		}
		s.batch = append(s.batch, t)
	}
	s.primary = ""
	return nil
}

// everyReturnedTenantHasStatus checks the filter really filtered.
func everyReturnedTenantHasStatus(ctx context.Context, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	if s.lastPage == nil || len(s.lastPage.Items) == 0 {
		return fmt.Errorf("the list returned nothing to check")
	}
	for _, v := range s.lastPage.Items {
		if v.Status != status {
			return fmt.Errorf("listed tenant %s has status %q, wanted %q", v.Slug, v.Status, status)
		}
	}
	return nil
}

// iCanFollowACursorToFetchTenantsInCreationOrder walks two pages and checks
// the ordering contract across the boundary.
func iCanFollowACursorToFetchTenantsInCreationOrder(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	first := s.lastPage
	if first == nil {
		return fmt.Errorf("no listing has been made yet")
	}
	if first.NextCursor == "" {
		return fmt.Errorf("the first page offered no cursor to follow")
	}
	second, res, err := s.api.Tenants("active", first.NextCursor, 5)
	if err != nil {
		return err
	}
	if res.Status != http.StatusOK {
		return fmt.Errorf("following the cursor: %s", res.Error())
	}
	if len(second.Items) == 0 {
		return fmt.Errorf("the cursor page was empty")
	}
	seen := map[string]bool{}
	var prev string
	for _, page := range []*TenantPage{first, second} {
		for _, v := range page.Items {
			if seen[v.ID] {
				return fmt.Errorf("tenant %s appeared on two pages", v.Slug)
			}
			seen[v.ID] = true
			if prev != "" && v.CreatedAt < prev {
				return fmt.Errorf("pages are not in creation order: %s after %s", v.CreatedAt, prev)
			}
			prev = v.CreatedAt
		}
	}
	s.cursorTenant = &second.Items[0]
	return nil
}

// taskListingFilteredByThatTenantReturnsItsTasksOldestFirst checks the task
// list of one tenant from the cursor page.
func taskListingFilteredByThatTenantReturnsItsTasksOldestFirst(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	id := ""
	if s.cursorTenant != nil {
		id = s.cursorTenant.ID
	} else if t, err := s.primaryTenant(); err == nil {
		id = t.ID
	}
	if id == "" {
		return fmt.Errorf("no tenant to filter tasks by")
	}
	tasks, err := s.api.TenantTasks(id)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return fmt.Errorf("tenant %s has no tasks to list", id)
	}
	for i := 1; i < len(tasks); i++ {
		if tasks[i].CreatedAt < tasks[i-1].CreatedAt {
			return fmt.Errorf("tasks are not oldest first at position %d", i)
		}
	}
	return nil
}

// theResponseFieldsAreExactly pins the tenant payload contract, which is what
// keeps infrastructure out of the record.
func theResponseFieldsAreExactly(ctx context.Context, fields string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	if s.last == nil {
		return fmt.Errorf("no response to inspect")
	}
	if s.last.Status != http.StatusOK {
		return fmt.Errorf("expected a 200 response, got %s", s.last.Error())
	}
	got, err := s.last.FieldNames()
	if err != nil {
		return err
	}
	want := splitList(fields)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		return fmt.Errorf("response fields are %v, wanted %v", got, want)
	}
	return nil
}

// noFieldInTheResponseContainsAPasswordOrConnectionString scans every leaf
// value, not just the field names.
func noFieldInTheResponseContainsAPasswordOrConnectionString(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	if s.last == nil {
		return fmt.Errorf("no response to inspect")
	}
	leaves, err := s.last.LeafValues()
	if err != nil {
		return err
	}
	markers := []string{"postgres://", "postgresql://", "password", "secret_key", "m2m", "tenant-db:5432", "control-plane-db:5432"}
	for path, value := range leaves {
		lower := strings.ToLower(value)
		for _, m := range markers {
			if strings.Contains(lower, m) {
				return fmt.Errorf("field %q contains %q", path, m)
			}
		}
	}
	if _, err := s.last.Problem(); err == nil {
		return fmt.Errorf("a tenant response decoded as a problem document")
	}
	return nil
}

// theErrorBodyHasFields checks the RFC 9457 shape.
func theErrorBodyHasFields(ctx context.Context, fields string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	if s.last == nil {
		return fmt.Errorf("no error response to inspect")
	}
	got, err := s.last.FieldNames()
	if err != nil {
		return err
	}
	want := splitList(fields)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		return fmt.Errorf("problem body fields are %v, wanted %v", got, want)
	}
	if ct := s.last.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		return fmt.Errorf("error content type is %q, wanted application/problem+json", ct)
	}
	p, err := s.last.Problem()
	if err != nil {
		return err
	}
	if p.Status != s.last.Status {
		return fmt.Errorf("problem status %d does not match HTTP %d", p.Status, s.last.Status)
	}
	return nil
}

// theRequestFailsWithAndCode asserts the recorded response.
func theRequestFailsWithAndCode(ctx context.Context, status int, code string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	if s.last == nil {
		return fmt.Errorf("no request was made")
	}
	if s.last.Status != status {
		return fmt.Errorf("request returned %d, wanted %d (%s)", s.last.Status, status, s.last.Error())
	}
	p, err := s.last.Problem()
	if err != nil {
		return err
	}
	if p.Code != code {
		return fmt.Errorf("problem code is %q, wanted %q", p.Code, code)
	}
	return nil
}

// exactlyRequestSucceedsWith and the two failure counters read the recorded
// concurrent responses.
func exactlyRequestSucceedsWith(ctx context.Context, n, status int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	got := 0
	for _, r := range s.concurrent {
		if r.Status == status {
			got++
		}
	}
	if got != n {
		return fmt.Errorf("%d requests succeeded with %d, wanted %d (statuses: %s)", got, status, n, statusLine(s.concurrent))
	}
	return nil
}

func requestsFailWith(ctx context.Context, n, status int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	got := 0
	for _, r := range s.concurrent {
		if r.Status == status {
			got++
		}
	}
	if got != n {
		return fmt.Errorf("%d requests failed with %d, wanted %d (statuses: %s)", got, status, n, statusLine(s.concurrent))
	}
	return nil
}

func requestsFailWithAndCode(ctx context.Context, n, status int, code string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	got := 0
	for _, r := range s.concurrent {
		if r.Status != status {
			continue
		}
		p, err := r.Problem()
		if err == nil && p.Code == code {
			got++
		}
	}
	if got != n {
		return fmt.Errorf("%d requests failed with %d and code %q, wanted %d (%s)", got, status, code, n, statusLine(s.concurrent))
	}
	return nil
}

// updateTaskExistsForTheTenant counts the update tasks the tenant has, which
// is how a concurrency test proves only one mutation was committed.
func updateTaskExistsForTheTenant(ctx context.Context, n int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	tasks, err := s.api.TenantTasks(t.ID)
	if err != nil {
		return err
	}
	got := 0
	for _, task := range tasks {
		if task.Type == "update" {
			got++
		}
	}
	if got != n {
		return fmt.Errorf("tenant has %d update tasks, wanted %d", got, n)
	}
	return nil
}

// clientsPatchTheTenantNameConcurrentlyWithVersion races n same-version PATCH
// requests.
func clientsPatchTheTenantNameConcurrentlyWithVersion(ctx context.Context, n, version int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	responses, err := s.api.Parallel(http.MethodPatch, "/v1/tenants/"+t.ID, map[string]any{"version": version, "name": fmt.Sprintf("Contender %d", n)}, n)
	if err != nil {
		return err
	}
	s.concurrent = responses
	for _, r := range responses {
		if r.Status == http.StatusAccepted {
			var m Mutation
			if err := r.Decode(&m); err == nil {
				s.adopt(t.Placeholder, &m)
			}
			break
		}
	}
	return nil
}
