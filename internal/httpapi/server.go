package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/mail"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/argon2"
	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/store"
	"tenant-control-plane/internal/tenant"
)

type Sealer interface {
	Seal(context.Context, string) (string, error)
}
type Server struct {
	Store *store.Store
	// Sealer encrypts an imported database URL before anything is persisted.
	Sealer Sealer
	// Wake carries a just-committed task id to the relay goroutine.
	Wake chan string
	// FastPublish enables that in-request hand-off. When false the sweeper
	// alone publishes, which is how the recovery path stays testable.
	FastPublish bool
	// SecureCookies marks session cookies Secure (set behind TLS).
	SecureCookies bool
}
type userKey struct{}

func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, r *http.Request, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "/docs/problems#" + code, "title": http.StatusText(status), "status": status, "code": code, "detail": detail, "instance": r.URL.Path})
}
func result(w http.ResponseWriter, r *http.Request, v any, err error) {
	if err == nil {
		JSON(w, 200, v)
		return
	}
	var p store.Problem
	if errors.As(err, &p) {
		code := string(p)
		status := 409
		if p == store.NotFound || p == store.TaskNotFound {
			status = 404
		}
		if code == "validation_failed" {
			status = 400
		}
		fail(w, r, status, code, strings.ReplaceAll(code, "_", " "))
		return
	}
	slog.Error("request failed", "path", r.URL.Path, "error_type", fmt.Sprintf("%T", err))
	fail(w, r, 503, "service_unavailable", "A required service is unavailable; retry later.")
}
func body(w http.ResponseWriter, r *http.Request, v any) bool {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		fail(w, r, 415, "unsupported_media_type", "Use application/json.")
		return false
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		fail(w, r, 400, "validation_failed", "Invalid JSON request.")
		return false
	}
	if d.Decode(&struct{}{}) != io.EOF {
		fail(w, r, 400, "validation_failed", "Send one JSON object.")
		return false
	}
	return true
}
func (s *Server) Handler() http.Handler {
	h := HandlerWithOptions(s, StdHTTPServerOptions{ErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, _ error) {
		fail(w, r, 400, "validation_failed", "Invalid request parameter.")
	}})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = &problemWriter{ResponseWriter: w, request: r}
		defer func() {
			if recover() != nil {
				fail(w, r, 500, "internal_error", "Unexpected server error.")
			}
		}()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/healthz" {
			JSON(w, 200, map[string]string{"status": "ok"})
			return
		}
		if r.URL.Path == "/readyz" {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if s.Store.DB.Ping(ctx) != nil {
				fail(w, r, 503, "service_unavailable", "Database unavailable.")
			} else {
				JSON(w, 200, map[string]string{"status": "ready"})
			}
			return
		}
		// Origin validation complements SameSite cookies, including login CSRF.
		if r.Method != "GET" && r.Method != "HEAD" {
			if origin := r.Header.Get("Origin"); origin != "" {
				u, err := url.Parse(origin)
				if err != nil || u.Host != r.Host {
					fail(w, r, 403, "forbidden", "Origin mismatch.")
					return
				}
			}
		}
		if r.URL.Path == "/v1/auth/login" && r.Method == "POST" {
			h.ServeHTTP(w, r)
			return
		}
		token, kind := credential(r)
		var u domain.User
		err := s.Store.DB.QueryRow(r.Context(), "SELECT u.id::text,u.email FROM users u JOIN credentials c ON c.user_id=u.id WHERE c.hash=$1 AND c.kind=$2 AND c.expires_at>now()", Hash(token), kind).Scan(&u.ID, &u.Email)
		if errors.Is(err, pgx.ErrNoRows) || token == "" {
			fail(w, r, 401, "unauthenticated", "Log in or provide a valid API token.")
			return
		}
		if err != nil {
			result(w, r, nil, err)
			return
		}
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey{}, u)))
	})
}
func (s *Server) mutation(w http.ResponseWriter, r *http.Request, t domain.Tenant, k domain.Task, err error) {
	if err != nil {
		result(w, r, nil, err)
		return
	}
	if s.FastPublish {
		select {
		case s.Wake <- k.ID:
		default:
		}
	}
	w.Header().Set("Location", "/v1/tenants/"+t.ID)
	JSON(w, 202, map[string]any{"tenant": t, "task": k})
}
func (s *Server) CreateTenant(w http.ResponseWriter, r *http.Request) {
	var b CreateTenantJSONBody
	if !body(w, r, &b) {
		return
	}
	if tenant.ValidateSlug(b.Slug) != nil || tenant.ValidateName(b.Name) != nil || len(b.Name) > 200 {
		fail(w, r, 400, "validation_failed", "Invalid slug or name.")
		return
	}
	t, k, err := s.Store.Create(r.Context(), b.Slug, b.Name)
	s.mutation(w, r, t, k, err)
}
func (s *Server) UpdateTenant(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	var b UpdateTenantJSONBody
	if !body(w, r, &b) {
		return
	}
	if b.Version < 1 || (b.Name == nil && b.DatabaseUrl == nil) || (b.Name != nil && (tenant.ValidateName(*b.Name) != nil || len(*b.Name) > 200)) {
		fail(w, r, 400, "validation_failed", "Provide version and a nonempty name or database_url.")
		return
	}
	sealed := ""
	if b.DatabaseUrl != nil {
		u, err := url.Parse(*b.DatabaseUrl)
		if err != nil || len(*b.DatabaseUrl) > 8192 || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || u.Path == "" {
			fail(w, r, 400, "validation_failed", "Invalid PostgreSQL URL.")
			return
		}
		sealed, err = s.Sealer.Seal(r.Context(), *b.DatabaseUrl)
		if err != nil {
			result(w, r, nil, err)
			return
		}
	}
	t, k, err := s.Store.Mutate(r.Context(), id.String(), "update", b.Version, b.Name, sealed)
	s.mutation(w, r, t, k, err)
}
func (s *Server) DeleteTenant(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	t, k, err := s.Store.Mutate(r.Context(), id.String(), "destroy", 0, nil, "")
	s.mutation(w, r, t, k, err)
}
func (s *Server) GetTenant(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	v, err := s.Store.Tenant(r.Context(), id.String())
	result(w, r, v, err)
}
func (s *Server) GetTask(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	v, err := s.Store.Task(r.Context(), id.String())
	result(w, r, v, err)
}
func ptr[T ~string](p *T) string {
	if p == nil {
		return ""
	}
	return string(*p)
}
func filter(limit *int, cursor, status *string) (store.Filter, error) {
	f := store.Filter{Limit: 20, Cursor: ptr(cursor), Status: ptr(status)}
	if limit != nil {
		f.Limit = *limit
	}
	if f.Limit < 1 || f.Limit > 100 {
		return f, store.Problem("validation_failed")
	}
	return f, nil
}
func (s *Server) ListTenants(w http.ResponseWriter, r *http.Request, p ListTenantsParams) {
	f, err := filter(p.Limit, p.Cursor, p.Status)
	if err != nil {
		result(w, r, nil, err)
		return
	}
	v, err := s.Store.Tenants(r.Context(), f)
	result(w, r, v, err)
}
func (s *Server) ListTasks(w http.ResponseWriter, r *http.Request, p ListTasksParams) {
	f, err := filter(p.Limit, p.Cursor, p.Status)
	if err != nil {
		result(w, r, nil, err)
		return
	}
	f.Type = ptr(p.Type)
	if p.TenantId != nil {
		f.TenantID = p.TenantId.String()
	}
	v, err := s.Store.Tasks(r.Context(), f)
	result(w, r, v, err)
}
func (s *Server) ListWorkers(w http.ResponseWriter, r *http.Request) {
	v, err := s.Store.Workers(r.Context())
	result(w, r, v, err)
}
func Hash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func randomToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func Password(password string) string {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		panic(err)
	}
	h := argon2.IDKey([]byte(password), salt, 2, 64*1024, 2, 32)
	return hex.EncodeToString(salt) + ":" + hex.EncodeToString(h)
}
func checkPassword(password, encoded string) bool {
	p := strings.Split(encoded, ":")
	if len(p) != 2 {
		return false
	}
	salt, e := hex.DecodeString(p[0])
	if e != nil {
		return false
	}
	h, e := hex.DecodeString(p[1])
	if e != nil {
		return false
	}
	if len(salt) != 16 || len(h) != 32 {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, 2, 64*1024, 2, 32)
	return subtle.ConstantTimeCompare(got, h) == 1
}
func credential(r *http.Request) (string, string) {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer "), "api"
	}
	c, e := r.Cookie("cp_session")
	if e == nil {
		return c.Value, "session"
	}
	return "", "session"
}
func (s *Server) Bootstrap(ctx context.Context, email, password string) error {
	if password == "" || len(password) < 12 {
		return errors.New("ADMIN_PASSWORD must have at least 12 characters")
	}
	_, err := s.Store.DB.Exec(ctx, "INSERT INTO users(id,email,password_hash) VALUES($1,$2,$3) ON CONFLICT(email) DO NOTHING", uuid.NewString(), strings.ToLower(email), Password(password))
	return err
}
func (s *Server) Login(w http.ResponseWriter, r *http.Request) {
	var b LoginJSONBody
	if !body(w, r, &b) {
		return
	}
	if len(b.Password) > 1024 {
		fail(w, r, 400, "validation_failed", "Invalid credentials.")
		return
	}
	var u domain.User
	var hash string
	err := s.Store.DB.QueryRow(r.Context(), "SELECT id::text,email,password_hash FROM users WHERE email=$1", strings.ToLower(b.Email)).Scan(&u.ID, &u.Email, &hash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		result(w, r, nil, err)
		return
	}
	if err != nil || !checkPassword(b.Password, hash) {
		fail(w, r, 401, "unauthenticated", "Invalid email or password.")
		return
	}
	token := randomToken()
	_, err = s.Store.DB.Exec(r.Context(), "INSERT INTO credentials(hash,user_id,kind,expires_at) VALUES($1,$2,'session',$3)", Hash(token), u.ID, time.Now().Add(12*time.Hour))
	if err != nil {
		result(w, r, nil, err)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: "cp_session", Value: token, Path: "/", HttpOnly: true, Secure: s.SecureCookies, SameSite: http.SameSiteStrictMode, MaxAge: 43200})
	JSON(w, 200, u)
}
func (s *Server) Logout(w http.ResponseWriter, r *http.Request) {
	token, kind := credential(r)
	if kind == "session" {
		if _, err := s.Store.DB.Exec(r.Context(), "DELETE FROM credentials WHERE hash=$1 AND kind='session'", Hash(token)); err != nil {
			result(w, r, nil, err)
			return
		}
	}
	http.SetCookie(w, &http.Cookie{Name: "cp_session", Path: "/", MaxAge: -1, HttpOnly: true, Secure: s.SecureCookies, SameSite: http.SameSiteStrictMode})
	w.WriteHeader(204)
}
func (s *Server) Me(w http.ResponseWriter, r *http.Request) {
	JSON(w, 200, r.Context().Value(userKey{}))
}
func (s *Server) CreateUser(w http.ResponseWriter, r *http.Request) {
	var b CreateUserJSONBody
	if !body(w, r, &b) {
		return
	}
	if _, err := mail.ParseAddress(b.Email); err != nil || len(b.Password) < 12 || len(b.Password) > 1024 {
		fail(w, r, 400, "validation_failed", "Valid email and 12–1024 character password required.")
		return
	}
	u := domain.User{ID: uuid.NewString(), Email: strings.ToLower(b.Email)}
	_, err := s.Store.DB.Exec(r.Context(), "INSERT INTO users(id,email,password_hash) VALUES($1,$2,$3)", u.ID, u.Email, Password(b.Password))
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" {
		fail(w, r, 409, "user_already_exists", "Email already registered.")
		return
	}
	if err != nil {
		result(w, r, nil, err)
		return
	}
	JSON(w, 201, u)
}
func (s *Server) ListUsers(w http.ResponseWriter, r *http.Request) {
	rows, err := s.Store.DB.Query(r.Context(), "SELECT id::text,email FROM users ORDER BY email")
	if err != nil {
		result(w, r, nil, err)
		return
	}
	defer rows.Close()
	users := []domain.User{}
	for rows.Next() {
		var u domain.User
		if err = rows.Scan(&u.ID, &u.Email); err != nil {
			result(w, r, nil, err)
			return
		}
		users = append(users, u)
	}
	result(w, r, users, rows.Err())
}
func (s *Server) CreateToken(w http.ResponseWriter, r *http.Request) {
	var b CreateTokenJSONBody
	if !body(w, r, &b) {
		return
	}
	if strings.TrimSpace(b.Name) == "" || len(b.Name) > 100 {
		fail(w, r, 400, "validation_failed", "Name required (max 100 characters).")
		return
	}
	u := r.Context().Value(userKey{}).(domain.User)
	token := randomToken()
	_, err := s.Store.DB.Exec(r.Context(), "INSERT INTO credentials(hash,user_id,kind,name,expires_at) VALUES($1,$2,'api',$3,$4)", Hash(token), u.ID, b.Name, time.Now().Add(90*24*time.Hour))
	if err != nil {
		result(w, r, nil, err)
		return
	}
	JSON(w, 201, map[string]string{"token": token})
}
func (s *Server) ListTokens(w http.ResponseWriter, r *http.Request) {
	u := r.Context().Value(userKey{}).(domain.User)
	rows, err := s.Store.DB.Query(r.Context(), "SELECT hash,name FROM credentials WHERE user_id=$1 AND kind='api' AND expires_at>now() ORDER BY created_at", u.ID)
	if err != nil {
		result(w, r, nil, err)
		return
	}
	defer rows.Close()
	items := []map[string]string{}
	for rows.Next() {
		var id, name string
		if err = rows.Scan(&id, &name); err != nil {
			result(w, r, nil, err)
			return
		}
		items = append(items, map[string]string{"id": id, "name": name})
	}
	result(w, r, items, rows.Err())
}
func (s *Server) DeleteToken(w http.ResponseWriter, r *http.Request, id string) {
	u := r.Context().Value(userKey{}).(domain.User)
	_, err := s.Store.DB.Exec(r.Context(), "DELETE FROM credentials WHERE hash=$1 AND user_id=$2 AND kind='api'", id, u.ID)
	if err != nil {
		result(w, r, nil, err)
		return
	}
	w.WriteHeader(204)
}
