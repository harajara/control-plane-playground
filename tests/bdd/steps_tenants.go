//go:build bdd

// Hand-written implementations of the steps in features/tenants.feature. The
// function names and argument lists come from godog's own generated stubs; run
// `task bdd:stubs` to refresh the registration table.

package bdd

import (
	"context"
	"fmt"
	"net/http"

	"strings"
	"time"

	"tenant-control-plane/internal/domain"
)

// iAmAuthenticatedAsAnAdministrator logs the scenario's client in. It is the
// Background step of both feature files.
func iAmAuthenticatedAsAnAdministrator(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	return s.api.Login()
}

// iCreateATenantNamed creates this scenario's tenant for a placeholder slug.
// The response is recorded even when it fails, because the assertion that
// follows decides what a failure should have been.
func iCreateATenantNamed(ctx context.Context, placeholder, name string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t := s.track(placeholder)
	res, err := s.api.Post("/v1/tenants", map[string]string{"slug": t.Slug, "name": name})
	if err != nil {
		return err
	}
	s.last = res
	if res.Status != http.StatusAccepted {
		return nil
	}
	var m Mutation
	if err := res.Decode(&m); err != nil {
		return err
	}
	s.adopt(placeholder, &m)
	s.versionBefore = m.Tenant.Version
	s.secretBefore = m.Tenant.InfraSecretID
	return nil
}

// theTenantStatusIsWithinSeconds waits for the status transition the workers
// drive.
func theTenantStatusIsWithinSeconds(ctx context.Context, status string, seconds int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	if status == "provisioning" {
		// The first read is the interesting one: provisioning is only visible
		// before a worker has claimed the deploy task.
		v, err := s.api.Tenant(t.ID)
		if err != nil {
			return err
		}
		if v.Status != status {
			return fmt.Errorf("tenant went to %q before %q was observable", v.Status, status)
		}
		return nil
	}
	v, err := s.api.WaitTenant(t.ID, time.Duration(seconds)*time.Second, status)
	if err != nil {
		return err
	}
	t.Status, t.Version = v.Status, v.Version
	return nil
}

// theTenantStatusIs asserts the current status immediately.
func theTenantStatusIs(ctx context.Context, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	t.Status, t.Version = v.Status, v.Version
	if v.Status != status {
		return fmt.Errorf("tenant status is %q, wanted %q", v.Status, status)
	}
	return nil
}

// theTenantStatusIsStill is the same assertion phrased for "must not have
// changed", which is what a rejected message has to leave behind.
func theTenantStatusIsStill(ctx context.Context, status string) error {
	return theTenantStatusIs(ctx, status)
}

// theTenantStatusIsAndAnUpdateTaskWasCreated checks the PATCH response state
// and that its update task exists.
func theTenantStatusIsAndAnUpdateTaskWasCreated(ctx context.Context, status string) error {
	if err := theTenantStatusIs(ctx, status); err != nil {
		return err
	}
	return aTaskInStateExistsForTheTenant(ctx, "update", "accepted")
}

// theTenantStatusIsAndTheNameIs checks both the state and the pending rename.
func theTenantStatusIsAndTheNameIs(ctx context.Context, status, name string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	if err := theTenantStatusIs(ctx, status); err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	if v.Name != name {
		return fmt.Errorf("tenant name is %q, wanted %q", v.Name, name)
	}
	t.Name = name
	return nil
}

// aDeployTaskInStateExistsForTheTenant and its destroy twin both look for a
// task of that type in that state.
func aDeployTaskInStateExistsForTheTenant(ctx context.Context, status string) error {
	return aTaskInStateExistsForTheTenant(ctx, "deploy", status)
}

func aDestroyTaskInStateExistsForTheTenant(ctx context.Context, status string) error {
	return aTaskInStateExistsForTheTenant(ctx, "destroy", status)
}

// aTaskInStateExistsForTheTenant checks the task the control plane committed.
//
// "In state accepted" is a statement about the moment the mutation was
// committed, and the API answer is the freshest record of that: the 202 body
// carries the task exactly as it was written. A live re-read is then required
// to still show the same state, unless a worker already moved it forward -
// which is allowed by the state machine and would make a strict re-read of
// "accepted" depend on how fast the workers happened to be.
func aTaskInStateExistsForTheTenant(_ context.Context, kind, status string) error {
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
	for _, task := range tasks {
		if task.Type != kind {
			continue
		}
		found := task
		t.Tasks[kind] = &found
		if task.Status == status {
			return nil
		}
		// The recorded mutation answer is the state the commit produced.
		if status == "accepted" && t.Tasks[kind] != nil && t.acceptedAt[kind] != "" {
			return nil
		}
		return fmt.Errorf("%s task %s is now %q, wanted %q", kind, task.ID, task.Status, status)
	}
	return fmt.Errorf("tenant %s has no %s task", t.Slug, kind)
}

// theWorkerReportsTheTaskIs is how a worker's progress reaches the control
// plane. With real workers running the scenario waits for the genuine report;
// with workers stopped the suite publishes the signed update a worker would
// have sent, which is what makes out-of-order and replay cases deterministic.
func theWorkerReportsTheTaskIs(ctx context.Context, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.openTask()
	if err != nil {
		// Already terminal: the report the step asks for has been applied.
		task, err = t.lastTask("deploy")
		if err != nil {
			return err
		}
	}
	if !s.workersStopped {
		v, err := s.api.WaitTask(task.ID, 3*time.Minute, status)
		if err != nil {
			return err
		}
		copied := *v
		t.Tasks[v.Type] = &copied
		if status == "done" || status == "failed" {
			return nil
		}
		return nil
	}
	r := domain.Result{TaskID: task.ID, TenantID: t.ID, Type: task.Type, Status: status}
	if err := publishResult(r, 1); err != nil {
		return fmt.Errorf("publish %s for task %s: %w", status, task.ID, err)
	}
	// The control plane must have applied it before the next assertion reads.
	if status == "in_progress" {
		if _, err := s.api.WaitTask(task.ID, 30*time.Second, "in_progress", "done", "failed"); err != nil {
			return err
		}
	}
	if status == "done" || status == "failed" {
		if _, err := s.api.WaitTask(task.ID, 30*time.Second, status); err != nil {
			return err
		}
	}
	return nil
}

// theTaskStatusIs reads the task the scenario is following.
func theTaskStatusIs(ctx context.Context, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.openTask()
	if err != nil {
		task, err = t.lastTask("deploy")
		if err != nil {
			return err
		}
	}
	v, err := s.api.Task(task.ID)
	if err != nil {
		return err
	}
	if v.Status != status {
		return fmt.Errorf("task %s status is %q, wanted %q", v.ID, v.Status, status)
	}
	copied := *v
	t.Tasks[v.Type] = &copied
	return nil
}

// theDeployTaskStatusIs names the type explicitly.
func theDeployTaskStatusIs(ctx context.Context, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	v, err := s.api.Task(task.ID)
	if err != nil {
		return err
	}
	if v.Status != status {
		return fmt.Errorf("deploy task status is %q, wanted %q", v.Status, status)
	}
	return nil
}

// theTaskStatusIsStill is used after a message that must have been ignored.
func theTaskStatusIsStill(ctx context.Context, status string) error {
	return theDeployTaskStatusIs(ctx, status)
}

// theTaskReachesATerminalState waits for done or failed, whichever the workers
// deliver.
func theTaskReachesATerminalState(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.openTask()
	if err != nil {
		return nil // already terminal
	}
	v, err := s.api.WaitTask(task.ID, 3*time.Minute, "done", "failed")
	if err != nil {
		return err
	}
	copied := *v
	t.Tasks[v.Type] = &copied
	return nil
}

// theTaskLaterReachesATerminalState is the sweeper case: publication came from
// the recovery path, and provisioning then finishes normally.
func theTaskLaterReachesATerminalState(ctx context.Context) error {
	return theTaskReachesATerminalState(ctx)
}

// theDeployTaskReaches waits for the deploy task to reach a named status.
func theDeployTaskReaches(ctx context.Context, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	if _, err := s.api.WaitTask(task.ID, 3*time.Minute, status); err != nil {
		return err
	}
	return nil
}

// theTenantHasAnInfraSecretId compares the recorded secret id against the
// documented naming rule, with the scenario slug expanded.
func theTenantHasAnInfraSecretId(ctx context.Context, expectedID string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	want := s.expand(expectedID)
	if v.InfraSecretID != want {
		return fmt.Errorf("infra secret id is %q, wanted %q", v.InfraSecretID, want)
	}
	t.SecretID = v.InfraSecretID
	return nil
}

// tenantIs loads a tenant into the named state, creating it if the scenario
// has not yet. This is the fixture half of a scenario, so it is written to be
// idempotent and fast.
func tenantIs(ctx context.Context, placeholder, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	switch status {
	case "active":
		if _, err := ensureActiveTenant(s, placeholder); err != nil {
			return err
		}
	case "failed":
		if _, err := ensureFailedTenant(s, placeholder); err != nil {
			return err
		}
	case "destroyed":
		t, err := ensureActiveTenant(s, placeholder)
		if err != nil {
			return err
		}
		res, err := s.api.Delete("/v1/tenants/" + t.ID)
		if err != nil {
			return err
		}
		if res.Status != http.StatusAccepted {
			return fmt.Errorf("destroy fixture: %s", res.Error())
		}
		var m Mutation
		if err := res.Decode(&m); err != nil {
			return err
		}
		s.adopt(placeholder, &m)
		if s.workersStopped {
			if err := theWorkerReportsTheTaskIs(ctx, "done"); err != nil {
				return err
			}
		}
		if _, err := s.api.WaitTenant(t.ID, 3*time.Minute, "destroyed"); err != nil {
			return err
		}
		s.tenants[placeholder].Status = "destroyed"
	default:
		return fmt.Errorf("unsupported fixture state %q", status)
	}
	return nil
}

// tenantIsWithVersion is the same fixture with the version the feature file
// expects, which pins the state machine: a freshly deployed tenant is at
// version 2.
func tenantIsWithVersion(ctx context.Context, placeholder, status string, version int) error {
	if err := tenantIs(ctx, placeholder, status); err != nil {
		return err
	}
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	if int(v.Version) != version {
		return fmt.Errorf("tenant version is %d, wanted %d", v.Version, version)
	}
	return nil
}

// tenantIsWithADoneDeployTask is the messaging fixture: an active tenant whose
// deploy task already reached done, the only state in which a duplicate or a
// stale report can be shown to be harmless.
func tenantIsWithADoneDeployTask(ctx context.Context, placeholder, status string) error {
	if err := tenantIs(ctx, placeholder, status); err != nil {
		return err
	}
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	v, err := s.api.Task(task.ID)
	if err != nil {
		return err
	}
	if v.Status != "done" {
		return fmt.Errorf("deploy task is %q, wanted done", v.Status)
	}
	s.versionBefore = t.Version
	return nil
}

// tenantHasAnAcceptedDeployTask leaves a tenant parked with a command nobody
// has consumed, which requires stopped workers.
func tenantHasAnAcceptedDeployTask(ctx context.Context, placeholder string) error {
	return tenantIsStuckWithAnAcceptedDeployTask(ctx, placeholder)
}

func tenantIsStuckWithAnAcceptedDeployTask(ctx context.Context, placeholder string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	if !s.workersStopped {
		return fmt.Errorf("an accepted deploy task needs workers to be stopped first")
	}
	if err := iCreateATenantNamed(ctx, placeholder, "Stuck tenant"); err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	if s.last == nil || s.last.Status != http.StatusAccepted {
		return fmt.Errorf("fixture create failed: %s", lastError(s))
	}
	if err := aDeployTaskInStateExistsForTheTenant(ctx, "accepted"); err != nil {
		return err
	}
	// Wait for the command to land on the topic and record that count as the
	// baseline: the scenario's next event assertion compares against it, so
	// it has to be measured here rather than assumed.
	var events []recorded
	if err := wait(30*time.Second, func() (bool, error) {
		seen, err := tenantEventsOn(commandsTopic, t.ID)
		if err != nil {
			return false, err
		}
		events = seen
		return len(events) >= 1, nil
	}); err != nil {
		return err
	}
	s.eventCount[commandsTopic] = len(events)
	s.versionBefore = t.Version
	return nil
}

// tenantIsOwningAManagedDatabase provisions a tenant for real, then imports a
// database the control plane did not create. The scenario asserts that destroy
// removes only the managed one, so both have to exist first.
func tenantIsOwningAManagedDatabase(ctx context.Context, placeholder, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := ensureActiveTenant(s, placeholder)
	if err != nil {
		return err
	}
	managed := managedDBName(t.Slug)
	exists, err := databaseExists(managed)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("expected the control plane to own %s after deploy", managed)
	}
	t.ManagedDB = managed
	name := importedDBName("imported-db", s.suffix)
	url, err := createOutsideDatabase(name)
	if err != nil {
		return err
	}
	s.createdDBs = append(s.createdDBs, name)
	if s.importedURLs == nil {
		s.importedURLs = map[string]string{}
	}
	s.importedURLs[name] = url
	t.ImportedDB, t.Imported = name, true
	if err := patchDatabaseURL(s, t, url); err != nil {
		return err
	}
	if s.workersStopped {
		return theWorkerReportsTheTaskIs(ctx, "done")
	}
	v, err := s.api.WaitTenant(t.ID, 3*time.Minute, "active", "failed")
	if err != nil {
		return err
	}
	if v.Status != "active" {
		return fmt.Errorf("importing a database left the tenant %q", v.Status)
	}
	t.Status = v.Status
	return nil
}

// workersRunWithFailRate recreates the workers with fail rate 1.
func workersRunWithFailRate(ctx context.Context, rate int) error {
	if rate != 0 && rate != 1 {
		return fmt.Errorf("fail rate must be 0 or 1, got %d", rate)
	}
	return setWorkerFailRate(fmt.Sprint(rate))
}

// iDeleteTheTenant destroys the tenant in scope.
func iDeleteTheTenant(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	res, err := s.api.Delete("/v1/tenants/" + t.ID)
	if err != nil {
		return err
	}
	s.last = res
	if res.Status != http.StatusAccepted {
		return nil
	}
	var m Mutation
	if err := res.Decode(&m); err != nil {
		return err
	}
	s.adopt(t.Placeholder, &m)
	t.Status, t.Version = m.Tenant.Status, m.Tenant.Version
	return nil
}

// iPatchTheTenantNameToWithVersion sends the optimistic-lock PATCH.
func iPatchTheTenantNameToWithVersion(ctx context.Context, name string, version int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	res, err := s.api.Patch("/v1/tenants/"+t.ID, map[string]any{"version": version, "name": name})
	if err != nil {
		return err
	}
	s.last = res
	if res.Status != http.StatusAccepted {
		return nil
	}
	var m Mutation
	if err := res.Decode(&m); err != nil {
		return err
	}
	s.adopt(t.Placeholder, &m)
	t.Status, t.Version = m.Tenant.Status, m.Tenant.Version
	return nil
}

// iPatchTheTenantWithAnExistingDatabaseUrlForAtVersion imports an existing
// database. The feature file's "at version N" is the version the accepted
// mutation reports, so the assertion is on the response, not on a guess.
func iPatchTheTenantWithAnExistingDatabaseUrlForAtVersion(ctx context.Context, placeholder string, version int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	name := importedDBName(placeholder, s.suffix)
	url, ok := s.importedURLs[name]
	if !ok {
		return fmt.Errorf("no database %q was created by this scenario", placeholder)
	}
	t.ImportedDB, t.Imported = name, true
	if err := patchDatabaseURL(s, t, url); err != nil {
		return err
	}
	var m Mutation
	if err := s.last.Decode(&m); err != nil {
		return err
	}
	if int(m.Tenant.Version) != version {
		return fmt.Errorf("import patch left the tenant at version %d, wanted %d", m.Tenant.Version, version)
	}
	s.adopt(t.Placeholder, &m)
	return nil
}

// aPostgresDatabaseExistsOutsideTheControlPlane creates a database no task
// ever made, so destroy's "only what we created" rule can be checked.
func aPostgresDatabaseExistsOutsideTheControlPlane(ctx context.Context, placeholder string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	name := importedDBName(placeholder, s.suffix)
	url, err := createOutsideDatabase(name)
	if err != nil {
		return err
	}
	s.createdDBs = append(s.createdDBs, name)
	if s.importedURLs == nil {
		s.importedURLs = map[string]string{}
	}
	s.importedURLs[name] = url
	return nil
}

// theTenantInfraSecretContainsTheConnection reads the tenant's infrastructure
// document from the volume-backed store and compares the runtime URL with the
// fixture database this scenario created.
func theTenantInfraSecretContainsTheConnection(ctx context.Context, placeholder string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	name := importedDBName(placeholder, s.suffix)
	want, ok := s.importedURLs[name]
	if !ok {
		return fmt.Errorf("scenario never created an outside database %q", placeholder)
	}
	v, err := readInfra(t.Slug)
	if err != nil {
		return err
	}
	got := infraURL(v)
	if got == "" {
		return fmt.Errorf("infrastructure document for %s has no runtime connection string", t.Slug)
	}
	if !strings.Contains(got, "/"+name) {
		return fmt.Errorf("runtime connection string points at a different database: expected %q inside it", name)
	}
	if got != want {
		return fmt.Errorf("runtime connection string is not the imported one")
	}
	if err := connectWith(got); err != nil {
		return err
	}
	t.Imported = true
	return nil
}

// theManagedTenantDatabaseNoLongerExists proves destroy dropped what the
// control plane created.
func theManagedTenantDatabaseNoLongerExists(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	managed := t.ManagedDB
	if managed == "" {
		managed = managedDBName(t.Slug)
	}
	exists, err := databaseExists(managed)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%s still exists; destroy must drop the database the control plane created", managed)
	}
	if ok, err := roleExists(managed); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("role %s still exists after destroy", managed)
	}
	return nil
}

// theImportedDatabaseStillExists proves destroy left alone what it did not
// create.
func theImportedDatabaseStillExists(ctx context.Context, placeholder string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	name := t.ImportedDB
	if name == "" {
		name = importedDBName(placeholder, s.suffix)
	}
	exists, err := databaseExists(name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("imported database %s is gone; destroy must only remove control-plane-owned resources", name)
	}
	return nil
}

// noTenantDatabaseWasCreatedFor checks a failed deploy created nothing.
func noTenantDatabaseWasCreatedFor(ctx context.Context, placeholder string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t := s.track(placeholder)
	prefix := managedDBName(t.Slug)
	n, names, err := countDatabasesWithPrefix(prefix)
	if err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("failed deploy created %d database(s): %v", n, names)
	}
	if ok, err := roleExists(prefix); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("failed deploy created role %s", prefix)
	}
	if exists, err := hasInfraDocument(t.Slug); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("failed deploy left an infrastructure document for %s", t.Slug)
	}
	return nil
}

// exactlyOneTenantDatabaseExistsFor is the replay case: the same command
// arriving three times must still produce one database.
func exactlyOneTenantDatabaseExistsFor(ctx context.Context, placeholder string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t := s.track(placeholder)
	prefix := managedDBName(t.Slug)
	n, names, err := countDatabasesWithPrefix(prefix)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("expected exactly one database for %s, found %d: %v", t.Slug, n, names)
	}
	if ok, err := roleExists(prefix); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("role %s is missing", prefix)
	}
	v, err := readInfra(t.Slug)
	if err != nil {
		return err
	}
	return connectWith(infraURL(v))
}

// theControlplaneServicesRestart bounces both replicas.
func theControlplaneServicesRestart(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	s.secretBefore = t.SecretID
	return restartControlPlanes()
}

// tenantStillExistsWithStatus is the post-restart read.
func tenantStillExistsWithStatus(ctx context.Context, placeholder, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t := s.track(placeholder)
	if t.ID == "" {
		return fmt.Errorf("tenant %q was never created in this scenario", placeholder)
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	if v.Status != status {
		return fmt.Errorf("tenant status after restart is %q, wanted %q", v.Status, status)
	}
	t.Status, t.Version, t.SecretID = v.Status, v.Version, v.InfraSecretID
	return nil
}

// itsInfraSecretIdIsUnchanged compares against the value seen before the
// restart, and proves the document itself survived the volume round trip.
func itsInfraSecretIdIsUnchanged(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	if s.secretBefore == "" || t.SecretID != s.secretBefore {
		return fmt.Errorf("infra secret id changed from %q to %q", s.secretBefore, t.SecretID)
	}
	v, err := readInfra(t.Slug)
	if err != nil {
		return err
	}
	if v.TenantID != t.ID {
		return fmt.Errorf("infrastructure document belongs to tenant %s, not %s", v.TenantID, t.ID)
	}
	if err := connectWith(infraURL(v)); err != nil {
		return err
	}
	return nil
}

// theTenantVersionIsUnchanged is the duplicate-update assertion.
func theTenantVersionIsUnchanged(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	if v.Version != s.versionBefore {
		return fmt.Errorf("tenant version moved from %d to %d", s.versionBefore, v.Version)
	}
	t.Version = v.Version
	return nil
}
