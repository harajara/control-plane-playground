package tenant

import (
	"errors"
	"testing"
)

var allStatuses = []Status{
	StatusProvisioning, StatusActive, StatusUpdating,
	StatusFailed, StatusDestroying, StatusDestroyed,
}

var allTaskStatuses = []TaskStatus{
	TaskAccepted, TaskInProgress, TaskDone, TaskFailed,
}

func TestCreateStartsProvisioning(t *testing.T) {
	if got := Create(); got != StatusProvisioning {
		t.Fatalf("Create() = %q, want %q", got, StatusProvisioning)
	}
}

func TestBeginUpdateOnlyFromActive(t *testing.T) {
	for _, from := range allStatuses {
		got, err := BeginUpdate(from)
		if from == StatusActive {
			if err != nil || got != StatusUpdating {
				t.Errorf("BeginUpdate(%q) = %q, %v; want %q, nil", from, got, err, StatusUpdating)
			}
			continue
		}
		if !errors.Is(err, ErrInvalidTransition) || got != from {
			t.Errorf("BeginUpdate(%q) = %q, %v; want %q, ErrInvalidTransition", from, got, err, from)
		}
	}
}

func TestBeginDestroyFromActiveOrFailed(t *testing.T) {
	for _, from := range allStatuses {
		got, err := BeginDestroy(from)
		if from == StatusActive || from == StatusFailed {
			if err != nil || got != StatusDestroying {
				t.Errorf("BeginDestroy(%q) = %q, %v; want %q, nil", from, got, err, StatusDestroying)
			}
			continue
		}
		if !errors.Is(err, ErrInvalidTransition) || got != from {
			t.Errorf("BeginDestroy(%q) = %q, %v; want %q, ErrInvalidTransition", from, got, err, from)
		}
	}
}

func TestApplyTaskOutcomeTable(t *testing.T) {
	cases := []struct {
		name    string
		current Status
		task    TaskType
		outcome TaskStatus
		want    Status
		wantErr bool
	}{
		{"deploy done", StatusProvisioning, TaskDeploy, TaskDone, StatusActive, false},
		{"deploy failed", StatusProvisioning, TaskDeploy, TaskFailed, StatusFailed, false},
		{"update done", StatusUpdating, TaskUpdate, TaskDone, StatusActive, false},
		{"update failed", StatusUpdating, TaskUpdate, TaskFailed, StatusFailed, false},
		{"destroy done", StatusDestroying, TaskDestroy, TaskDone, StatusDestroyed, false},
		{"destroy failed", StatusDestroying, TaskDestroy, TaskFailed, StatusFailed, false},
		{"deploy done on active is stale", StatusActive, TaskDeploy, TaskDone, StatusActive, true},
		{"deploy done on destroyed is stale", StatusDestroyed, TaskDeploy, TaskDone, StatusDestroyed, true},
		{"update done after destroy started", StatusDestroying, TaskUpdate, TaskDone, StatusDestroying, true},
		{"destroy done on active is rejected", StatusActive, TaskDestroy, TaskDone, StatusActive, true},
		{"in_progress is not a terminal outcome", StatusProvisioning, TaskDeploy, TaskInProgress, StatusProvisioning, true},
		{"accepted is not a terminal outcome", StatusProvisioning, TaskDeploy, TaskAccepted, StatusProvisioning, true},
		{"unknown task type", StatusProvisioning, TaskType("rebuild"), TaskDone, StatusProvisioning, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ApplyTaskOutcome(tc.current, tc.task, tc.outcome)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("err = %v, want ErrInvalidTransition", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("status = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestApplyTaskProgressAllowsOnlyForwardMoves(t *testing.T) {
	allowed := map[[2]TaskStatus]bool{
		{TaskAccepted, TaskInProgress}: true,
		{TaskInProgress, TaskDone}:     true,
		{TaskInProgress, TaskFailed}:   true,
	}
	for _, from := range allTaskStatuses {
		for _, to := range allTaskStatuses {
			got, err := ApplyTaskProgress(from, to)
			if allowed[[2]TaskStatus{from, to}] {
				if err != nil || got != to {
					t.Errorf("ApplyTaskProgress(%q, %q) = %q, %v; want %q, nil", from, to, got, err, to)
				}
				continue
			}
			if !errors.Is(err, ErrInvalidTransition) || got != from {
				t.Errorf("ApplyTaskProgress(%q, %q) = %q, %v; want %q, ErrInvalidTransition", from, to, got, err, from)
			}
		}
	}
}

func TestTerminalTaskStatusesNeverMove(t *testing.T) {
	for _, terminal := range []TaskStatus{TaskDone, TaskFailed} {
		for _, next := range allTaskStatuses {
			if _, err := ApplyTaskProgress(terminal, next); !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("terminal %q moved to %q without error", terminal, next)
			}
		}
	}
}
