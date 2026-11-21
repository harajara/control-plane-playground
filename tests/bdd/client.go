//go:build bdd

package bdd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// Tenant is the API's tenant record. Exactly these eight fields are the
// contract; the "no infrastructure values" scenario proves it.
type Tenant struct {
	ID            string `json:"id"`
	Slug          string `json:"slug"`
	Name          string `json:"name"`
	Status        string `json:"status"`
	Version       int64  `json:"version"`
	InfraSecretID string `json:"infra_secret_id"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

// Task is the API's task record.
type Task struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// Mutation is a 202 response: the committed tenant and the task it created.
type Mutation struct {
	Tenant Tenant `json:"tenant"`
	Task   Task   `json:"task"`
}

// Problem is an RFC 9457 response body.
type Problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Code     string `json:"code"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

// Response is one API answer, kept whole because scenarios assert on status
// codes, problem codes, field sets and raw bodies.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

func (r *Response) Error() string {
	return fmt.Sprintf("HTTP %d: %s", r.Status, trim(string(r.Body), 400))
}

// Decode unmarshals the body.
func (r *Response) Decode(out any) error {
	if err := json.Unmarshal(r.Body, out); err != nil {
		return fmt.Errorf("body is not the documented JSON (%w); body: %s", err, trim(string(r.Body), 300))
	}
	return nil
}

// Problem decodes an RFC 9457 body.
func (r *Response) Problem() (*Problem, error) {
	p := &Problem{}
	if err := r.Decode(p); err != nil {
		return nil, err
	}
	return p, nil
}

// FieldNames lists the top-level JSON keys of the body.
func (r *Response) FieldNames() ([]string, error) {
	var v map[string]json.RawMessage
	if err := r.Decode(&v); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(v))
	for k := range v {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// LeafValues returns every scalar in the body as "path=value", so a scenario
// can prove no value anywhere looks like a credential.
func (r *Response) LeafValues() (map[string]string, error) {
	var v any
	if err := json.Unmarshal(r.Body, &v); err != nil {
		return nil, err
	}
	out := map[string]string{}
	walk("", v, out)
	return out, nil
}

func walk(path string, v any, out map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			next := k
			if path != "" {
				next = path + "." + k
			}
			walk(next, t[k], out)
		}
	case []any:
		for i, x := range t {
			walk(fmt.Sprintf("%s[%d]", path, i), x, out)
		}
	case string:
		out[path] = t
	default:
		out[path] = fmt.Sprint(t)
	}
}

// Client is an administrator's HTTP session against the running control plane.
// It is an ordinary client on purpose: the scenarios exercise the published
// contract, not internals.
type Client struct {
	Base   string
	http   *http.Client
	Authed bool
}

// NewClient builds a client for BASE_URL.
func NewClient() (*Client, error) {
	base := os.Getenv("BASE_URL")
	if base == "" {
		return nil, fmt.Errorf("BASE_URL is required")
	}
	jar, _ := cookiejar.New(nil)
	return &Client{Base: strings.TrimSuffix(base, "/"), http: &http.Client{Jar: jar, Timeout: 30 * time.Second}}, nil
}

// Login authenticates with the administrator credentials from the environment.
func (c *Client) Login() error {
	body := map[string]string{"email": os.Getenv("ADMIN_EMAIL"), "password": os.Getenv("ADMIN_PASSWORD")}
	if body["email"] == "" || body["password"] == "" {
		return fmt.Errorf("ADMIN_EMAIL and ADMIN_PASSWORD are required")
	}
	res, err := c.Post("/v1/auth/login", body)
	if err != nil {
		return err
	}
	if res.Status != http.StatusOK {
		return fmt.Errorf("administrator login failed: %s", res.Error())
	}
	c.Authed = true
	return nil
}

// Logout drops the session, leaving a client that can make unauthenticated
// calls with the same transport.
func (c *Client) Logout() error {
	res, err := c.Post("/v1/auth/logout", nil)
	if err != nil {
		return err
	}
	if res.Status != http.StatusNoContent {
		return fmt.Errorf("logout failed: %s", res.Error())
	}
	c.Authed = false
	return nil
}

func (c *Client) send(method, path string, body any) (*Response, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.Base+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	return &Response{Status: resp.StatusCode, Header: resp.Header, Body: raw}, nil
}

// Get issues a GET.
func (c *Client) Get(path string) (*Response, error) { return c.send(http.MethodGet, path, nil) }

// Post issues a POST with an optional JSON body.
func (c *Client) Post(path string, body any) (*Response, error) {
	return c.send(http.MethodPost, path, body)
}

// Patch issues a PATCH.
func (c *Client) Patch(path string, body any) (*Response, error) {
	return c.send(http.MethodPatch, path, body)
}

// Delete issues a DELETE.
func (c *Client) Delete(path string) (*Response, error) { return c.send(http.MethodDelete, path, nil) }

// Tenant fetches one tenant.
func (c *Client) Tenant(id string) (*Tenant, error) {
	res, err := c.Get("/v1/tenants/" + id)
	if err != nil {
		return nil, err
	}
	if res.Status != http.StatusOK {
		return nil, fmt.Errorf("GET tenant: %s", res.Error())
	}
	var v Tenant
	if err = res.Decode(&v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Task fetches one task.
func (c *Client) Task(id string) (*Task, error) {
	res, err := c.Get("/v1/tasks/" + id)
	if err != nil {
		return nil, err
	}
	if res.Status != http.StatusOK {
		return nil, fmt.Errorf("GET task: %s", res.Error())
	}
	var v Task
	if err = res.Decode(&v); err != nil {
		return nil, err
	}
	return &v, nil
}

// TenantTasks lists a tenant's tasks in creation order.
func (c *Client) TenantTasks(tenantID string) ([]Task, error) {
	res, err := c.Get("/v1/tasks?tenant_id=" + url.QueryEscape(tenantID) + "&limit=100")
	if err != nil {
		return nil, err
	}
	if res.Status != http.StatusOK {
		return nil, fmt.Errorf("GET tasks: %s", res.Error())
	}
	var page struct {
		Items []Task `json:"items"`
	}
	if err = res.Decode(&page); err != nil {
		return nil, err
	}
	return page.Items, nil
}

// TenantPage is one page of the tenant list.
type TenantPage struct {
	Items      []Tenant `json:"items"`
	NextCursor string   `json:"next_cursor"`
}

// Tenants lists tenants with the given filter.
func (c *Client) Tenants(status string, cursor string, limit int) (*TenantPage, *Response, error) {
	q := url.Values{"limit": {fmt.Sprint(limit)}}
	if status != "" {
		q.Set("status", status)
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	res, err := c.Get("/v1/tenants?" + q.Encode())
	if err != nil {
		return nil, nil, err
	}
	page := &TenantPage{}
	if res.Status == http.StatusOK {
		if err = res.Decode(page); err != nil {
			return nil, res, err
		}
	}
	return page, res, nil
}

// WaitTenant polls until the tenant reaches one of the wanted statuses.
func (c *Client) WaitTenant(id string, within time.Duration, want ...string) (*Tenant, error) {
	var last *Tenant
	var lastErr error
	err := wait(until(within), func() (bool, error) {
		v, err := c.Tenant(id)
		if err != nil {
			lastErr = err
			return false, nil
		}
		last = v
		for _, w := range want {
			if v.Status == w {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil && lastErr != nil {
		return nil, lastErr
	}
	if err != nil {
		return last, fmt.Errorf("tenant stayed %q, wanted %s: %w", last.Status, strings.Join(want, " or "), err)
	}
	return last, nil
}

// WaitTask polls until the task reaches one of the wanted statuses.
func (c *Client) WaitTask(id string, within time.Duration, want ...string) (*Task, error) {
	var last *Task
	err := wait(until(within), func() (bool, error) {
		v, err := c.Task(id)
		if err != nil {
			return false, err
		}
		last = v
		for _, w := range want {
			if v.Status == w {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		if last != nil {
			return last, fmt.Errorf("task stayed %q, wanted %s: %w", last.Status, strings.Join(want, " or "), err)
		}
		return nil, err
	}
	return last, nil
}

// Parallel runs the same request n times as concurrently as the client can
// manage, and returns every response in completion order.
func (c *Client) Parallel(method, path string, body any, n int) ([]*Response, error) {
	out := make(chan *Response, n)
	start := make(chan struct{})
	var inflight []chan *Response
	for i := 0; i < n; i++ {
		done := make(chan *Response)
		inflight = append(inflight, done)
		go func() {
			<-start
			var res *Response
			var err error
			switch method {
			case http.MethodPost:
				res, err = c.Post(path, body)
			case http.MethodPatch:
				res, err = c.Patch(path, body)
			default:
				err = fmt.Errorf("unsupported concurrent method %s", method)
			}
			if err != nil {
				out <- &Response{Status: 0, Body: []byte(err.Error())}
				return
			}
			out <- res
		}()
	}
	close(start)
	var responses []*Response
	for range inflight {
		responses = append(responses, <-out)
	}
	return responses, nil
}

func trim(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
