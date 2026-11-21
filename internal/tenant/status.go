// Package tenant holds the domain rules for tenants and tasks.
// The state machines here are pure functions, so they are unit-tested
// without a database or broker.
package tenant

import "errors"

// ErrInvalidTransition means the requested change is not allowed from the current state.
var ErrInvalidTransition = errors.New("invalid transition")

type Status string

const (
	StatusProvisioning Status = "provisioning"
	StatusActive       Status = "active"
	StatusUpdating     Status = "updating"
	StatusFailed       Status = "failed"
	StatusDestroying   Status = "destroying"
	StatusDestroyed    Status = "destroyed"
)

type TaskType string

const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

type TaskStatus string

const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

// Create returns the status of a newly created tenant.
func Create() Status {
	return StatusProvisioning
}

// BeginUpdate is the PATCH transition. Only active tenants can be updated.
func BeginUpdate(current Status) (Status, error) {
	if current != StatusActive {
		return current, ErrInvalidTransition
	}
	return StatusUpdating, nil
}

// BeginDestroy is the DELETE transition. Only active or failed tenants can be destroyed.
func BeginDestroy(current Status) (Status, error) {
	if current != StatusActive && current != StatusFailed {
		return current, ErrInvalidTransition
	}
	return StatusDestroying, nil
}

// ApplyTaskOutcome returns the tenant status after a terminal task outcome.
// A task outcome only applies while the tenant is in the state that task started from.
// Any other combination returns ErrInvalidTransition, which the consumer treats as a stale no-op.
func ApplyTaskOutcome(current Status, taskType TaskType, outcome TaskStatus) (Status, error) {
	if outcome != TaskDone && outcome != TaskFailed {
		return current, ErrInvalidTransition
	}

	var from, succeeded Status
	switch taskType {
	case TaskDeploy:
		from, succeeded = StatusProvisioning, StatusActive
	case TaskUpdate:
		from, succeeded = StatusUpdating, StatusActive
	case TaskDestroy:
		from, succeeded = StatusDestroying, StatusDestroyed
	default:
		return current, ErrInvalidTransition
	}

	if current != from {
		return current, ErrInvalidTransition
	}
	if outcome == TaskFailed {
		return StatusFailed, nil
	}
	return succeeded, nil
}

// ApplyTaskProgress returns the task status after a progress report from the worker.
// accepted -> in_progress -> done | failed. Anything else is rejected, so a stale
// or duplicate report cannot move a task backwards or out of a terminal state.
func ApplyTaskProgress(current, next TaskStatus) (TaskStatus, error) {
	switch {
	case current == TaskAccepted && next == TaskInProgress:
		return next, nil
	case current == TaskInProgress && (next == TaskDone || next == TaskFailed):
		return next, nil
	default:
		return current, ErrInvalidTransition
	}
}
