// Package dockerapi talks to the Docker Engine over the mounted socket. It
// exists so the Gherkin suite and the log checker can manipulate the real
// stack - stop and resume workers, restart a control plane, recreate a
// container with different environment - without shipping a Docker CLI.
package dockerapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// EnvLabel selects containers by role label (cp.role=worker, cp.role=control-plane).
const RoleLabel = "cp.role"

// DefaultSocket is where compose mounts the engine socket.
const DefaultSocket = "/var/run/docker.sock"

// Client is a Docker Engine API client.
type Client struct {
	http *http.Client
	// APIVersion is negotiated from /version on first use.
	prefix string
}

// New builds a client for DOCKER_HOST (unix socket) or the default socket.
func New() (*Client, error) {
	path := os.Getenv("DOCKER_HOST")
	switch {
	case strings.HasPrefix(path, "unix://"):
		path = strings.TrimPrefix(path, "unix://")
	case path != "":
		return nil, fmt.Errorf("unsupported DOCKER_HOST %q: the suite expects a unix socket", path)
	default:
		path = DefaultSocket
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("docker socket unavailable: %w", err)
	}
	return &Client{
		http: &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		}}},
		prefix: "/v1.44",
	}, nil
}

type container struct {
	ID     string
	Labels map[string]string
	Names  []string
}

// do performs an API request. body may be nil; out may be nil to discard.
func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+c.prefix+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("docker %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

// list returns containers carrying the role label, sorted by name so callers
// act on a stable order.
func (c *Client) list(ctx context.Context, role string) ([]container, error) {
	var v []struct {
		ID     string            `json:"Id"`
		Labels map[string]string `json:"Labels"`
		Names  []string          `json:"Names"`
	}
	q := url.Values{"all": {"true"}, "filters": {fmt.Sprintf(`{"label":[%q]}`, RoleLabel+"="+role)}}
	if err := c.do(ctx, http.MethodGet, "/containers/json?"+q.Encode(), nil, &v); err != nil {
		return nil, err
	}
	out := make([]container, 0, len(v))
	for _, item := range v {
		names := make([]string, 0, len(item.Names))
		for _, n := range item.Names {
			names = append(names, strings.TrimPrefix(n, "/"))
		}
		out = append(out, container{ID: item.ID, Labels: item.Labels, Names: names})
	}
	sortNames(out)
	return out, nil
}

func sortNames(v []container) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && strings.Join(v[j].Names, ",") < strings.Join(v[j-1].Names, ","); j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

// Stop halts every container with the role label, leaving it ready to start.
func (c *Client) Stop(ctx context.Context, role string, timeout time.Duration) error {
	v, err := c.list(ctx, role)
	if err != nil {
		return err
	}
	if len(v) == 0 {
		return fmt.Errorf("no containers labelled %s=%s", RoleLabel, role)
	}
	for _, item := range v {
		if err = c.do(ctx, http.MethodPost, "/containers/"+item.ID+"/stop?"+url.Values{"t": {fmt.Sprint(int(timeout.Seconds()))}}.Encode(), nil, nil); err != nil && !notRunning(err) {
			return err
		}
		if err = c.wait(ctx, item.ID, "exited", 3*timeout+5*time.Second); err != nil {
			return err
		}
	}
	return nil
}

// Start resumes every container with the role label.
func (c *Client) Start(ctx context.Context, role string) error {
	v, err := c.list(ctx, role)
	if err != nil {
		return err
	}
	for _, item := range v {
		if err = c.do(ctx, http.MethodPost, "/containers/"+item.ID+"/start", nil, nil); err != nil && !alreadyStarted(err) {
			return err
		}
	}
	return c.waitRunning(ctx, role)
}

// Restart bounces every container with the role label.
func (c *Client) Restart(ctx context.Context, role string, timeout time.Duration) error {
	v, err := c.list(ctx, role)
	if err != nil {
		return err
	}
	for _, item := range v {
		if err = c.do(ctx, http.MethodPost, "/containers/"+item.ID+"/restart?"+url.Values{"t": {fmt.Sprint(int(timeout.Seconds()))}}.Encode(), nil, nil); err != nil {
			return err
		}
	}
	return c.waitRunning(ctx, role)
}

// SetEnv recreates every container with the role label, replacing (or adding)
// the given KEY=VALUE entries. Everything else about the container - image,
// network, mounts, healthcheck, restart policy - is copied from the running
// definition, so this is equivalent to editing the environment and bringing
// the service back up.
func (c *Client) SetEnv(ctx context.Context, role string, env []string) error {
	v, err := c.list(ctx, role)
	if err != nil {
		return err
	}
	if len(v) == 0 {
		return fmt.Errorf("no containers labelled %s=%s", RoleLabel, role)
	}
	for _, item := range v {
		name := item.ID
		if len(item.Names) > 0 {
			name = item.Names[0]
		}
		var inspect struct {
			Config struct {
				Image       string            `json:"Image"`
				Env         []string          `json:"Env"`
				Cmd         []string          `json:"Cmd"`
				Entrypoint  []string          `json:"Entrypoint"`
				Labels      map[string]string `json:"Labels"`
				WorkingDir  string            `json:"WorkingDir"`
				User        string            `json:"User"`
				Healthcheck *struct {
					Test        []string      `json:"Test"`
					Interval    time.Duration `json:"Interval"`
					Timeout     time.Duration `json:"Timeout"`
					Retries     int           `json:"Retries"`
					StartPeriod time.Duration `json:"StartPeriod"`
				} `json:"Healthcheck"`
				ExposedPorts map[string]struct{} `json:"ExposedPorts"`
			} `json:"Config"`
			HostConfig struct {
				Binds         []string       `json:"Binds"`
				NetworkMode   string         `json:"NetworkMode"`
				RestartPolicy map[string]any `json:"RestartPolicy"`
				PortBindings  map[string][]struct {
					HostIP   string `json:"HostIp"`
					HostPort string `json:"HostPort"`
				} `json:"PortBindings"`
				ExtraHosts []string `json:"ExtraHosts"`
			} `json:"HostConfig"`
			NetworkSettings struct {
				Networks map[string]struct {
					Aliases []string `json:"Aliases"`
				} `json:"Networks"`
			} `json:"NetworkSettings"`
		}
		if err = c.do(ctx, http.MethodGet, "/containers/"+item.ID+"/json", nil, &inspect); err != nil {
			return err
		}
		cfg := &inspect.Config
		if err = c.do(ctx, http.MethodPost, "/containers/"+item.ID+"/stop?"+url.Values{"t": {"10"}}.Encode(), nil, nil); err != nil && !notRunning(err) {
			return err
		}
		if err = c.wait(ctx, item.ID, "exited", 60*time.Second); err != nil {
			return err
		}
		if err = c.do(ctx, http.MethodDelete, "/containers/"+item.ID+"?"+url.Values{"force": {"true"}}.Encode(), nil, nil); err != nil {
			return err
		}
		body := map[string]any{
			"Image":        cfg.Image,
			"Env":          mergeEnv(cfg.Env, env),
			"Cmd":          cfg.Cmd,
			"Entrypoint":   cfg.Entrypoint,
			"Labels":       cfg.Labels,
			"WorkingDir":   cfg.WorkingDir,
			"User":         cfg.User,
			"ExposedPorts": cfg.ExposedPorts,
			"HostConfig": map[string]any{
				"Binds":         inspect.HostConfig.Binds,
				"NetworkMode":   inspect.HostConfig.NetworkMode,
				"RestartPolicy": inspect.HostConfig.RestartPolicy,
				"PortBindings":  inspect.HostConfig.PortBindings,
				"ExtraHosts":    inspect.HostConfig.ExtraHosts,
			},
		}
		if cfg.Healthcheck != nil {
			body["Healthcheck"] = cfg.Healthcheck
		}
		// NetworkingConfig restores the aliases compose assigned (the service
		// name), so other containers keep resolving this one by that name. The
		// alias equal to the short container id belongs to the old container.
		networks := map[string]any{}
		for netName, n := range inspect.NetworkSettings.Networks {
			if aliases := cleanAliases(n.Aliases, name); len(aliases) > 0 {
				networks[netName] = map[string]any{"Aliases": aliases}
			}
		}
		if len(networks) > 0 {
			body["NetworkingConfig"] = networks
		}
		var created struct {
			ID string `json:"Id"`
		}
		q := url.Values{"name": {name}}
		if err = c.do(ctx, http.MethodPost, "/containers/create?"+q.Encode(), body, &created); err != nil {
			return fmt.Errorf("recreate %s: %w", name, err)
		}
		if err = c.do(ctx, http.MethodPost, "/containers/"+created.ID+"/start", nil, nil); err != nil {
			return fmt.Errorf("start %s: %w", name, err)
		}
	}
	return c.waitRunning(ctx, role)
}

// Network-scoped aliases come back from inspect as ["<short-id>", "service"].
// The short id belongs to the destroyed container, so only the service alias
// (which equals the container name for these services) is worth restoring.
func cleanAliases(v []string, name string) []string {
	out := []string{}
	for _, a := range v {
		if a == name || (len(a) >= 12 && strings.HasPrefix(name, a)) {
			continue
		}
		out = append(out, a)
	}
	return out
}

func mergeEnv(current, overrides []string) []string {
	out := make([]string, 0, len(current)+len(overrides))
	seen := map[string]bool{}
	for _, o := range overrides {
		seen[strings.SplitN(o, "=", 2)[0]] = true
	}
	for _, item := range current {
		k := strings.SplitN(item, "=", 2)[0]
		if seen[k] {
			continue
		}
		out = append(out, item)
	}
	return append(out, overrides...)
}

func notRunning(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "not running") || strings.Contains(err.Error(), "cannot be stopped") || strings.Contains(err.Error(), "404"))
}

func alreadyStarted(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already started")
}

// wait polls the container until it reports the wanted status.
func (c *Client) wait(ctx context.Context, id, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var s struct {
			State struct {
				Status string `json:"Status"`
			} `json:"State"`
		}
		if err := c.do(ctx, http.MethodGet, "/containers/"+id+"/json", nil, &s); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			// A removed container counts as exited for our purposes.
			if strings.Contains(err.Error(), "404") && want == "exited" {
				return nil
			}
		} else if s.State.Status == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("container %s never reached %s", id[:12], want)
}

// waitRunning blocks until every container for the role reports running.
func (c *Client) waitRunning(ctx context.Context, role string) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Minute)
	}
	for {
		v, err := c.list(ctx, role)
		if err != nil {
			return err
		}
		if len(v) == 0 {
			return fmt.Errorf("no containers labelled %s=%s", RoleLabel, role)
		}
		all := true
		for _, item := range v {
			var s struct {
				State struct {
					Status string `json:"Status"`
				} `json:"State"`
			}
			if err = c.do(ctx, http.MethodGet, "/containers/"+item.ID+"/json", nil, &s); err != nil || s.State.Status != "running" {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("containers labelled %s=%s did not come up", RoleLabel, role)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Logs returns the combined stdout/stderr tail of every container carrying the
// role label. Used by the log-volume assertion.
func (c *Client) Logs(ctx context.Context, role string, tail int, since time.Duration) (string, error) {
	v, err := c.list(ctx, role)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	for _, item := range v {
		name := item.ID
		if len(item.Names) > 0 {
			name = item.Names[0]
		}
		q := url.Values{"stdout": {"true"}, "stderr": {"true"}, "tail": {fmt.Sprint(tail)}}
		if since > 0 {
			q.Set("since", fmt.Sprint(time.Now().Add(-since).Unix()))
			q.Del("tail")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker"+c.prefix+"/containers/"+item.ID+"/logs?"+q.Encode(), nil)
		if err != nil {
			return "", err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return "", err
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if err != nil {
			return "", err
		}
		if resp.StatusCode/100 != 2 {
			return "", fmt.Errorf("logs %s: %d", name, resp.StatusCode)
		}
		sb.Write(demultiplex(raw))
	}
	return sb.String(), nil
}

// demultiplex strips the Docker stream framing (8-byte header per frame).
func demultiplex(b []byte) []byte {
	if len(b) < 8 {
		return b
	}
	out := make([]byte, 0, len(b))
	for len(b) >= 8 {
		size := int(b[4])<<24 | int(b[5])<<16 | int(b[6])<<8 | int(b[7])
		b = b[8:]
		if size > len(b) {
			size = len(b)
		}
		out = append(out, b[:size]...)
		b = b[size:]
	}
	return out
}
