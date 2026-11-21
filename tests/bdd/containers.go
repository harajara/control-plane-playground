//go:build bdd

package bdd

import (
	"context"
	"fmt"
	"time"
)

// Container control. The feature files say "workers are stopped", "workers run
// with fail rate 1" and "the control-plane services restart", and each of
// those is only worth asserting if it is literally true. Every operation here
// goes to the Docker Engine over the socket the test service mounts, by the
// role label compose assigns, so the suite manipulates the same containers an
// operator would.
//
// These are primitives: they do not know about scenarios. The step layer
// records what a scenario changed so its cleanup can undo it, and TestMain can
// normalize the stack before the first scenario runs.

const (
	roleWorkers       = "worker"
	roleControlPlanes = "control-plane"
)

func opCtx(minutes int) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(minutes)*time.Minute)
}

// stopWorkersNow halts every worker and leaves it stopped. A command published
// while they are down stays on the topic until they return, which is exactly
// the state several scenarios need.
func stopWorkersNow() error {
	c, err := dockerClient()
	if err != nil {
		return err
	}
	ctx, cancel := opCtx(3)
	defer cancel()
	return c.Stop(ctx, roleWorkers, 15*time.Second)
}

// startWorkersNow resumes every worker and waits until the API reports three
// heartbeats, so a later step cannot race a container still joining the group.
func startWorkersNow() error {
	if err := startWorkersNoWait(); err != nil {
		return err
	}
	api, err := NewClient()
	if err != nil {
		return err
	}
	if err := api.Login(); err != nil {
		return err
	}
	if err := waitForOnlineWorkers(api, 3, 90*time.Second); err != nil {
		return err
	}
	// A group rebalance assigns partitions a beat after the heartbeat
	// appears; let the first fetch happen before the caller proceeds.
	time.Sleep(500 * time.Millisecond)
	return nil
}

// startWorkersNoWait resumes without waiting, for cleanup paths.
func startWorkersNoWait() error {
	c, err := dockerClient()
	if err != nil {
		return err
	}
	ctx, cancel := opCtx(3)
	defer cancel()
	return c.Start(ctx, roleWorkers)
}

// restartControlPlanesNow bounces both API replicas. Their state lives in
// Postgres and Kafka, so a restart must lose nothing.
func restartControlPlanesNow() error {
	c, err := dockerClient()
	if err != nil {
		return err
	}
	ctx, cancel := opCtx(3)
	defer cancel()
	if err = c.Restart(ctx, roleControlPlanes, 10*time.Second); err != nil {
		return err
	}
	api, err := NewClient()
	if err != nil {
		return err
	}
	if err := waitForReady(api, 2*time.Minute); err != nil {
		return err
	}
	// Sessions live in Postgres, so an existing cookie keeps working; a fresh
	// client here just proves the API is serving.
	return api.Login()
}

// setWorkerFailRateNow recreates the worker containers with a different
// WORKER_FAIL_RATE, the way an operator changing an environment value would.
func setWorkerFailRateNow(rate string) error {
	c, err := dockerClient()
	if err != nil {
		return err
	}
	ctx, cancel := opCtx(4)
	defer cancel()
	if err = c.SetEnv(ctx, roleWorkers, []string{"WORKER_FAIL_RATE=" + rate}); err != nil {
		return fmt.Errorf("recreate workers with WORKER_FAIL_RATE=%s: %w", rate, err)
	}
	api, err := NewClient()
	if err != nil {
		return err
	}
	if err := api.Login(); err != nil {
		return err
	}
	return waitForOnlineWorkers(api, 3, 2*time.Minute)
}

// setFastPublishNow recreates both control planes with CP_FAST_PUBLISH.
// Disabled, only the sweeper publishes, which is the path a scenario measures.
func setFastPublishNow(value string) error {
	c, err := dockerClient()
	if err != nil {
		return err
	}
	ctx, cancel := opCtx(4)
	defer cancel()
	if err = c.SetEnv(ctx, roleControlPlanes, []string{"CP_FAST_PUBLISH=" + value}); err != nil {
		return fmt.Errorf("recreate control planes with CP_FAST_PUBLISH=%s: %w", value, err)
	}
	api, err := NewClient()
	if err != nil {
		return err
	}
	return waitForReady(api, 2*time.Minute)
}

// waitForReady blocks until the published replica answers /readyz, which means
// its pool, schema and administrator bootstrap are back.
func waitForReady(api *Client, within time.Duration) error {
	return wait(within, func() (bool, error) {
		res, err := api.Get("/readyz")
		if err != nil {
			return false, nil
		}
		return res.Status == 200, nil
	})
}

type workerRow struct {
	ID     string `json:"id"`
	Online bool   `json:"online"`
}

// waitForOnlineWorkers proves the workers are consuming again rather than just
// running: /v1/workers is fed by their heartbeats through Kafka.
func waitForOnlineWorkers(api *Client, n int, within time.Duration) error {
	return wait(within, func() (bool, error) {
		res, err := api.Get("/v1/workers")
		if err != nil || res.Status != 200 {
			return false, nil
		}
		var rows []workerRow
		if err := res.Decode(&rows); err != nil {
			return false, err
		}
		online := 0
		for _, w := range rows {
			if w.Online {
				online++
			}
		}
		if online >= n {
			return true, nil
		}
		return false, fmt.Errorf("%d of %d workers online", online, n)
	})
}
