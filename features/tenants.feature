Feature: Tenant lifecycle and API contract

  Background:
    Given I am authenticated as an administrator

  Scenario: Provisioning a tenant walks the full lifecycle
    When I create a tenant "acme-prod" named "Acme Production"
    Then the tenant status is "provisioning" within 5 seconds
    And a deploy task in state "accepted" exists for the tenant
    When the worker reports the task is "in_progress"
    Then the task status is "in_progress"
    When the worker reports the task is "done"
    Then the tenant status is "active" within 10 seconds
    And the tenant has an infra secret id "control-plane/tenants/acme-prod/infra"

  Scenario: Injected failure moves the tenant to failed
    Given workers run with fail rate 1
    When I create a tenant "fail-tenant" named "Fail Tenant"
    And the task reaches a terminal state
    Then the deploy task status is "failed"
    And the tenant status is "failed"
    And no tenant database was created for "fail-tenant"

  Scenario: Failed tenant can be retried through destroy
    Given tenant "fail-tenant" is "failed"
    When I delete the tenant
    Then the tenant status is "destroying"
    And a destroy task in state "accepted" exists for the tenant
    When the worker reports the task is "done"
    Then the tenant status is "destroyed"

  Scenario: Updating an active tenant requires the current version
    Given tenant "acme-prod" is "active" with version 2
    When I patch the tenant name to "Acme Renamed" with version 2
    Then the tenant status is "updating" and an update task was created
    When the worker reports the task is "done"
    Then the tenant status is "active" and the name is "Acme Renamed"

  Scenario: An imported database replaces the connection string
    Given tenant "acme-prod" is "active"
    And a Postgres database "imported-db" exists outside the control plane
    When I patch the tenant with an existing database url for "imported-db" at version 3
    And the worker reports the task is "done"
    Then the tenant infra secret contains the "imported-db" connection
    And the tenant status is "active"

  Scenario: Destroying drops only databases the control plane created
    Given tenant "acme-prod" is "active" owning a managed database
    When I delete the tenant
    And the worker reports the task is "done"
    Then the managed tenant database no longer exists
    But the imported database "imported-db" still exists
    And the tenant status is "destroyed"

  Scenario: A destroyed tenant's slug is reserved forever
    Given tenant "acme-prod" is "destroyed"
    When I create a tenant "acme-prod" named "Second Acme"
    Then the request fails with 409 and code "tenant_already_exists"

  Scenario: Tenant and task endpoints are read-consistent
    Given 5 tenants exist
    When I list tenants with status "active"
    Then every returned tenant has status "active"
    And I can follow a cursor to fetch tenants in creation order
    And task listing filtered by that tenant returns its tasks oldest first

  Scenario: The tenant record contains no infrastructure values
    Given tenant "acme-prod" is "active"
    When I fetch the tenant
    Then the response fields are exactly "id, slug, name, status, version, infra_secret_id, created_at, updated_at"
    And no field in the response contains a password or connection string

  Scenario: Errors are machine-readable RFC 9457 problems
    When I create a tenant "Bad_Slug" named "Invalid"
    Then the request fails with 400 and code "validation_failed"
    When I fetch tenant "00000000-0000-4000-8000-000000000000"
    Then the request fails with 404 and code "tenant_not_found"
    And the error body has fields "type, title, status, code, detail, instance"

  Scenario: Only one of five concurrent same-version patches succeeds
    Given tenant "acme-prod" is "active" with version 2
    When 5 clients patch the tenant name concurrently with version 2
    Then exactly 1 request succeeds with 202
    And 4 requests fail with 409
    And 1 update task exists for the tenant

  Scenario: Only one of five concurrent creates wins a slug
    When 5 clients concurrently create tenant "race-slug" named "Racer"
    Then exactly 1 request succeeds with 202
    And 4 requests fail with 409 and code "tenant_already_exists"

  Scenario: Tenant state survives a control-plane restart
    Given tenant "acme-prod" is "active"
    When the control-plane services restart
    Then tenant "acme-prod" still exists with status "active"
    And its infra secret id is unchanged

  Scenario: Unauthenticated calls are rejected with a problem body
    Given I am not authenticated
    When I list tenants
    Then the request fails with 401 and code "unauthenticated"
