//go:build bdd

package bdd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"tenant-control-plane/internal/dockerapi"
)

// Suite-wide handles. Scenarios run one at a time (Concurrency 1) because they
// stop, start and recreate real containers; a shared engine client avoids
// re-dialing the socket for every step.
var (
	mu        sync.Mutex
	current   *state
	dockerAPI *dockerapi.Client
)

// state is one scenario: its unique slug suffix, its tenants, and the last
// response it saw.
type state struct {
	suffix string
	api    *Client

	// tenants is keyed by the placeholder slug written in the feature file.
	tenants map[string]*tracked
	// primary is the tenant unlaterated steps ("I delete the tenant") act on.
	primary string

	// last holds the most recent response and mutation, so an assertion step
	// can examine what the previous action step received.
	last     *Response
	lastDone *Mutation

	// Observations an assertion compares against.
	versionBefore int64
	secretBefore  string
	dlqCount      map[string]int
	eventCount    map[string]int

	// concurrent holds the responses of a parallel step, lastPage the most
	// recent listing, and cursorTenant a tenant carried across steps.
	concurrent   []*Response
	lastPage     *TenantPage
	batch        []*tracked
	cursorTenant *Tenant
	importedURLs map[string]string

	// poisonPayload and unsignedPayload are the exact bytes a rejection step
	// published, so the dead-letter check looks for those and not for any old
	// entry left by an earlier run.
	poisonPayload   []byte
	unsignedPayload []byte
	dlqTopic        string
	lastPosted      *posted
	recordedEvent   *recorded

	// Container changes this scenario made, all reverted in cleanup.
	workersStopped bool
	failRate       bool
	sweeperOnly    bool
	createdDBs     []string
}

// tracked is a tenant as the scenario knows it.
type tracked struct {
	Placeholder string
	Slug        string
	ID          string
	Name        string
	Status      string
	Version     int64
	SecretID    string
	// Tasks by type, most recent first seen for that type.
	Tasks map[string]*Task
	// acceptedAt records the task ids the API returned as accepted at commit
	// time, by task type. It is what "a task in state accepted exists" checks,
	// because a worker may legally have advanced the row since.
	acceptedAt map[string]string
	// Infrastructure the scenario knows about for this tenant.
	ManagedDB  string
	ImportedDB string
	Imported   bool
}

func newState() (*state, error) {
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	s := &state{
		suffix:       hex.EncodeToString(b),
		tenants:      map[string]*tracked{},
		dlqCount:     map[string]int{},
		eventCount:   map[string]int{},
		importedURLs: map[string]string{},
	}
	c, err := NewClient()
	if err != nil {
		return nil, err
	}
	s.api = c
	return s, nil
}

// slugFor expands a feature-file placeholder into this scenario's slug. The
// result always satisfies the API and database pattern
// ^[a-z][-a-z0-9]{1,26}[a-z0-9]$, and always ends in the run suffix, so two
// runs of the same scenario never share a tenant.
func (s *state) slugFor(placeholder string) string {
	suffix := "-" + s.suffix
	base := placeholder
	if len(base)+len(suffix) > 28 {
		base = base[:28-len(suffix)]
		base = strings.TrimRight(base, "-")
	}
	return base + suffix
}

// track returns the scenario's record for a placeholder, creating it on first
// mention so "tenant X is active" and a later "I delete the tenant" always
// mean the same row.
func (s *state) track(placeholder string) *tracked {
	if t, ok := s.tenants[placeholder]; ok {
		return t
	}
	s.tenants[placeholder] = &tracked{
		Placeholder: placeholder,
		Slug:        s.slugFor(placeholder),
		Tasks:       map[string]*Task{},
		acceptedAt:  map[string]string{},
	}
	return s.tenants[placeholder]
}

// adopt records a tenant the API returned under a placeholder.
func (s *state) adopt(placeholder string, m *Mutation) {
	t := s.track(placeholder)
	t.ID, t.Name, t.Status, t.Version, t.SecretID = m.Tenant.ID, m.Tenant.Name, m.Tenant.Status, m.Tenant.Version, m.Tenant.InfraSecretID
	t.Slug = m.Tenant.Slug
	copied := m.Task
	t.Tasks[copied.Type] = &copied
	if copied.Status == "accepted" {
		// The commit produced an accepted task; that fact is now recorded and
		// no worker can take it away.
		t.acceptedAt[copied.Type] = copied.ID
	}
	s.primary = placeholder
}

// current tenant helpers

func (s *state) primaryTenant() (*tracked, error) {
	if s.primary == "" {
		return nil, fmt.Errorf("no tenant in scope yet in this scenario")
	}
	return s.tenants[s.primary], nil
}

// lastTask returns the most recent task of a type for a tenant.
func (t *tracked) lastTask(kind string) (*Task, error) {
	v, ok := t.Tasks[kind]
	if !ok || v == nil {
		return nil, fmt.Errorf("tenant %s has no %s task in this scenario", t.Slug, kind)
	}
	return v, nil
}

// openTask returns the task a worker report should be about: the one that is
// not terminal yet, preferring the newest.
func (t *tracked) openTask() (*Task, error) {
	var best *Task
	for _, kind := range []string{"destroy", "update", "deploy"} {
		if v, ok := t.Tasks[kind]; ok && v != nil && (v.Status == "accepted" || v.Status == "in_progress") {
			best = v
			break
		}
	}
	if best == nil {
		return nil, fmt.Errorf("tenant %s has no open task (accepted or in_progress)", t.Slug)
	}
	return best, nil
}

// expand rewrites placeholder slugs inside assertion text, so an expected
// infrastructure secret id compares against this scenario's real slug.
func (s *state) expand(text string) string {
	for _, p := range s.knownPlaceholders() {
		t := s.tenants[p]
		if t.Slug != p {
			text = strings.ReplaceAll(text, p, t.Slug)
		}
	}
	return text
}

func (s *state) knownPlaceholders() []string {
	out := make([]string, 0, len(s.tenants))
	for k := range s.tenants {
		out = append(out, k)
	}
	// Longest first: "acme-prod" must not be rewritten inside a longer token.
	sort.Slice(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}

// importSlug finds the placeholder whose slug appears in text, used by steps
// that reference a tenant indirectly ("for that tenant").
var slugPattern = regexp.MustCompile(`[a-z][-a-z0-9]{1,26}[a-z0-9]`)

// cleanup reverts every side effect the scenario could have left running:
// recreated containers back to normal settings, and fixture databases it
// created outside the control plane. It uses the primitives directly because
// the scenario is already detached by the time it runs.
func (s *state) cleanup() []error {
	var problems []error
	if s.failRate {
		if err := setWorkerFailRateNow("0"); err != nil {
			problems = append(problems, err)
		}
	}
	if s.sweeperOnly {
		if err := setFastPublishNow("true"); err != nil {
			problems = append(problems, err)
		}
	}
	if s.workersStopped {
		if err := startWorkersNoWait(); err != nil {
			problems = append(problems, err)
		}
	}
	for _, name := range s.createdDBs {
		if err := dropDatabase(name); err != nil {
			problems = append(problems, err)
		}
	}
	return problems
}

// poll retries check until it reports done or the window passes. The interval
// is short: several steps assert on a state the workers move through, and a
// slow poll would sample after they have already left it.
func poll(window time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(window)
	var lastErr error
	for {
		done, err := check()
		if err != nil {
			lastErr = err
		} else if done {
			return nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("timed out after %v: %w", window, lastErr)
			}
			return fmt.Errorf("timed out after %v", window)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// wait runs a boolean check under a policy.
func wait(d time.Duration, check func() (bool, error)) error { return poll(d, check) }

// until is shorthand for a timeout policy; kept because steps read better with
// it at the call site.
func until(d time.Duration) time.Duration { return d }

// dockerClient opens (once) the engine API over the mounted socket.
func dockerClient() (*dockerapi.Client, error) {
	mu.Lock()
	defer mu.Unlock()
	if dockerAPI == nil {
		c, err := dockerapi.New()
		if err != nil {
			return nil, err
		}
		dockerAPI = c
	}
	return dockerAPI, nil
}

// getState returns the scenario in flight. Every step begins with this, so a
// step that somehow runs without a scenario fails clearly instead of nil
// dereferencing.
func getState() (*state, error) {
	mu.Lock()
	defer mu.Unlock()
	if current == nil {
		return nil, fmt.Errorf("no scenario in progress")
	}
	return current, nil
}

func setState(s *state) {
	mu.Lock()
	current = s
	mu.Unlock()
}
