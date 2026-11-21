package domain

import (
	"encoding/json"
	"time"
)

type Time struct{ time.Time }

func Now() Time { return Time{time.Now().UTC()} }
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.UTC().Format("2006-01-02T15:04:05.000Z"))
}
func (t *Time) UnmarshalJSON(b []byte) error { return json.Unmarshal(b, &t.Time) }

type Tenant struct {
	ID            string `json:"id"`
	Slug          string `json:"slug"`
	Name          string `json:"name"`
	Status        string `json:"status"`
	Version       int64  `json:"version"`
	InfraSecretID string `json:"infra_secret_id"`
	CreatedAt     Time   `json:"created_at"`
	UpdatedAt     Time   `json:"updated_at"`
}
type Task struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	CreatedAt Time   `json:"created_at"`
	UpdatedAt Time   `json:"updated_at"`
}
type Command struct {
	Task        Task    `json:"task"`
	Slug        string  `json:"slug"`
	SecretID    string  `json:"infra_secret_id"`
	PendingName *string `json:"pending_name,omitempty"`
	SealedURL   string  `json:"sealed_database_url,omitempty"`
}
type Result struct {
	TaskID    string `json:"task_id"`
	TenantID  string `json:"tenant_id"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code,omitempty"`
}
type Worker struct {
	ID          string `json:"id"`
	LastSeen    Time   `json:"last_seen"`
	Online      bool   `json:"online"`
	CurrentTask string `json:"current_task,omitempty"`
}
type User struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}
