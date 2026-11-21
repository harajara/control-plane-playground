//go:build bdd

package bdd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cucumber/godog"
)

// initialize registers every step definition and the per-scenario lifecycle.
func initialize(s *godog.ScenarioContext) {
	registerSteps(s)
	// BDD_SCENARIO is a substring filter for debugging one scenario at a time,
	// the equivalent of `go test -run` for a step suite. Unset, it changes
	// nothing; the feature files stay exactly as approved.
	only := os.Getenv("BDD_SCENARIO")
	s.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		if only != "" && !strings.Contains(strings.ToLower(sc.Name), strings.ToLower(only)) {
			return ctx, godog.ErrSkip
		}
		next, err := newState()
		if err != nil {
			return ctx, err
		}
		setState(next)
		return ctx, nil
	})
	s.After(func(ctx context.Context, sc *godog.Scenario, failure error) (context.Context, error) {
		leaving := currentScene()
		setState(nil)
		if leaving == nil {
			return ctx, failure
		}
		// Cleanup runs even after a failed scenario. A fixture database or a
		// worker container left at fail rate 1 would break the next scenario
		// and bury the real failure.
		problems := leaving.cleanup()
		if len(problems) == 0 {
			return ctx, failure
		}
		cleanupErr := fmt.Errorf("scenario cleanup failed: %s", strings.Join(errText(problems), "; "))
		if failure != nil {
			return ctx, fmt.Errorf("%w; %v", failure, cleanupErr)
		}
		return ctx, cleanupErr
	})
}

func errText(v []error) []string {
	out := make([]string, 0, len(v))
	for _, e := range v {
		out = append(out, e.Error())
	}
	return out
}
