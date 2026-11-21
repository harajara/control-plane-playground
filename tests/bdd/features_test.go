//go:build bdd

package bdd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cucumber/godog"
)

// TestFeatures runs features/*.feature. Every step lives in the generated
// stub file and its hand-written implementation beside it; nothing here is a
// placeholder.
func TestFeatures(t *testing.T) {
	for _, key := range []string{"BASE_URL", "ADMIN_EMAIL", "ADMIN_PASSWORD", "M2M_SECRET", "KAFKA_BROKERS", "TENANT_DB_ADMIN_URL"} {
		if os.Getenv(key) == "" {
			t.Fatalf("%s is required; run task test:bdd through the compose test service", key)
		}
	}
	// Feature paths are given relative to the repository root, because
	// `go test` runs with the working directory set to this package.
	root := filepath.Join("..", "..")
	var paths []string
	// BDD_PATHS narrows the run while a scenario is being debugged, the way
	// `go test -run` does. It never changes what the quality gate runs.
	if v := os.Getenv("BDD_PATHS"); v != "" {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				paths = append(paths, filepath.Join(root, filepath.FromSlash(p)))
			}
		}
	} else {
		paths = []string{filepath.Join(root, "features")}
	}
	// Return the stack to the state the features assume with no Given. A
	// previous run that was interrupted half way through would otherwise
	// leave workers stopped or failing, and the first scenario here would
	// time out waiting for a worker that is never coming back.
	if err := normalizeStack(); err != nil {
		t.Fatalf("cannot normalize the stack (%s): %v", lastStackState(), err)
	}
	suite := godog.TestSuite{
		Name:                "control-plane",
		ScenarioInitializer: initialize,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    paths,
			TestingT: t,
			// Scenarios mutate real containers (stop workers, restart control
			// planes), so they must not overlap.
			Concurrency: 1,
			Strict:      true,
		},
	}
	if code := suite.Run(); code != 0 {
		t.Fatalf("godog exited with %d", code)
	}
}
