// Package bdd runs features/*.feature against the booted compose stack.
//
// The step code is generated. `task bdd:stubs` runs godog with no step
// definitions, captures the snippets godog itself prints for every undefined
// step, and writes tests/bdd/steps_generated.go: one registration per step plus
// a pending stub for every handler that no hand-written file implements yet.
// Implementations therefore live beside the generated file, and regenerating
// after adding a feature only emits what is still missing.
//
// Run the suite with `task test:bdd`.
//
// # Scenario isolation
//
// Every slug written literally in a feature file is a placeholder. The suite
// appends a per-scenario suffix, so "acme-prod" in step text and in an
// assertion always means "this scenario's tenant", and a rerun never collides
// with data an earlier run left behind. Slugs stay inside the 28-character,
// lowercase, alphanumeric-edged limit the API and the database both enforce.
//
// # What the scenarios really touch
//
// HTTP goes to the API as an ordinary client. Crafted broker messages are
// signed with M2M_SECRET through the same internal/messaging code the services
// use. Assertions read the tenant Postgres, the Kafka topics and the
// volume-backed secret store. "Workers are stopped", "workers run with fail
// rate 1" and "the control-plane services restart" drive the real containers
// over the mounted Docker socket.
package bdd
