//go:build bdd

package bdd

import (
	"fmt"
	"time"
)

// Scenario-aware wrappers over the container primitives. They record what the
// scenario changed, so cleanup can put the stack back, and they keep the step
// code readable.

func stopWorkers() error {
	if err := stopWorkersNow(); err != nil {
		return err
	}
	s, err := getState()
	if err != nil {
		return err
	}
	s.workersStopped = true
	return nil
}

func startWorkers() error {
	if err := startWorkersNow(); err != nil {
		return err
	}
	s, err := getState()
	if err != nil {
		return err
	}
	s.workersStopped = false
	return nil
}

func restartControlPlanes() error { return restartControlPlanesNow() }

func setWorkerFailRate(rate string) error {
	if err := setWorkerFailRateNow(rate); err != nil {
		return err
	}
	s, err := getState()
	if err != nil {
		return err
	}
	s.failRate = rate != "0"
	s.workersStopped = false
	return nil
}

func setFastPublish(value string) error {
	if err := setFastPublishNow(value); err != nil {
		return err
	}
	s, err := getState()
	if err != nil {
		return err
	}
	s.sweeperOnly = value != "true"
	return nil
}

// normalizeStack returns the stack to the state the feature files assume when
// no Given says otherwise: workers running, consuming, fail rate 0, fast
// publish on.
//
// This is what makes the suite re-runnable. A scenario killed half way through
// - Ctrl-C, a crashed container, a timed out CI job - would otherwise leave
// workers stopped or failing, and the very first scenario of the next run
// would then time out waiting for a worker that is not coming. The checks are
// read-only when the stack is already correct, so a normal run pays for one
// API call and one container inspect.
func normalizeStack() error {
	api, err := NewClient()
	if err != nil {
		return err
	}
	if err := api.Login(); err != nil {
		return err
	}
	c, err := dockerClient()
	if err != nil {
		return err
	}
	ctx, cancel := opCtx(2)
	defer cancel()
	workers, err := c.RoleEnv(ctx, roleWorkers)
	if err != nil {
		return err
	}
	planes, err := c.RoleEnv(ctx, roleControlPlanes)
	if err != nil {
		return err
	}
	if workers["WORKER_FAIL_RATE"] != "0" {
		if err := setWorkerFailRateNow("0"); err != nil {
			return err
		}
	}
	if planes["CP_FAST_PUBLISH"] != "true" {
		if err := setFastPublishNow("true"); err != nil {
			return err
		}
	}
	// Stopped containers are not consuming, whatever their environment says.
	if err := startWorkersNoWait(); err != nil {
		return err
	}
	return waitForOnlineWorkers(api, 3, 2*time.Minute)
}

// lastStackState is only used to explain a normalization failure.
func lastStackState() string {
	c, err := dockerClient()
	if err != nil {
		return fmt.Sprintf("docker socket unavailable: %v", err)
	}
	ctx, cancel := opCtx(1)
	defer cancel()
	workers, err := c.RoleEnv(ctx, roleWorkers)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("WORKER_FAIL_RATE=%s", workers["WORKER_FAIL_RATE"])
}
