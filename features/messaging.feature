Feature: Messaging through Kafka

  Background:
    Given I am authenticated as an administrator

  Scenario: The task is the event
    When I create a tenant "event-tenant" named "Event Tenant"
    Then exactly one task.deploy event for the tenant appears on "tenant.tasks.v1" after commit
    And the event carries the task id, type "deploy", tenant id and status "accepted"
    And the event id equals the task id

  Scenario: A rolled-back request publishes nothing
    Given workers are stopped
    And tenant "held-tenant" is stuck with an accepted deploy task
    When I create a tenant "held-tenant" named "Clash"
    Then the request fails with 409 and code "tenant_already_exists"
    And no additional event was published on "tenant.tasks.v1"
    When I resume workers

  Scenario: Unpublished tasks are republished after five seconds
    Given the fast publish path is disabled
    When I create a tenant "sweeper-tenant" named "Sweeper"
    Then the deploy task is published within 15 seconds
    And the task later reaches a terminal state

  Scenario: A duplicate terminal update changes nothing
    Given tenant "acme-prod" is "active" with a done deploy task
    When I publish the same task.done update for the tenant twice
    Then the tenant version is unchanged
    And the task status is still "done"
    And nothing was routed to the dead-letter topic

  Scenario: A stale update cannot regress a terminal task
    Given tenant "acme-prod" is "active" with a done deploy task
    When I publish a task.in_progress update for the done deploy task
    Then the task status is still "done"
    And the tenant status is still "active"

  Scenario: A terminal update arriving before in_progress is not lost
    Given workers are stopped
    And tenant "early-tenant" has an accepted deploy task
    When I publish a task.done update for the accepted deploy task
    And I publish a task.in_progress update for the accepted deploy task
    And I resume workers
    Then the tenant status is "active" within 30 seconds

  Scenario: Poison messages go to the dead-letter topic
    When I publish an undecodable message to "tenant.task-updates.v1"
    Then the message appears on "tenant.task-updates.v1.dlq" within 10 seconds
    And the consumer is still processing subsequent valid updates

  Scenario: Redelivered commands are processed once by workers
    Given workers are stopped
    And tenant "retry-tenant" has an accepted deploy task
    When I redeliver the deploy task event twice and resume workers
    Then the deploy task reaches "done"
    And exactly one tenant database exists for "retry-tenant"

  Scenario: Updates are signed and carry a stable idempotency key
    When I publish an unsigned task.done update for a tenant
    Then the message is rejected to the dead-letter topic
    And two published updates with the same task and status share one id
