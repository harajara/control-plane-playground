CREATE TABLE IF NOT EXISTS tenants (
 id uuid PRIMARY KEY, slug text NOT NULL UNIQUE CHECK(slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
 name text NOT NULL CHECK(length(btrim(name))>0),
 status text NOT NULL CHECK(status IN ('provisioning','active','updating','failed','destroying','destroyed')),
 version bigint NOT NULL DEFAULT 1, infra_secret_id text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS tasks (
 id uuid PRIMARY KEY, type text NOT NULL CHECK(type IN ('deploy','update','destroy')),
 tenant_id uuid NOT NULL REFERENCES tenants(id),
 status text NOT NULL CHECK(status IN ('accepted','in_progress','done','failed')),
 command jsonb NOT NULL, published boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS tasks_one_open ON tasks(tenant_id) WHERE status IN ('accepted','in_progress');
CREATE INDEX IF NOT EXISTS tasks_unpublished ON tasks(created_at) WHERE NOT published;
CREATE INDEX IF NOT EXISTS tasks_tenant ON tasks(tenant_id,created_at,id);
CREATE TABLE IF NOT EXISTS inbox (
 id uuid PRIMARY KEY, task_id uuid NOT NULL REFERENCES tasks(id), result jsonb NOT NULL,
 applied boolean NOT NULL DEFAULT false, received_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS workers (id text PRIMARY KEY, last_seen timestamptz NOT NULL, current_task text NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS users (id uuid PRIMARY KEY, email text UNIQUE NOT NULL, password_hash text NOT NULL);
CREATE TABLE IF NOT EXISTS credentials (
 hash text PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id),
 kind text NOT NULL CHECK(kind IN ('session','api')), name text NOT NULL DEFAULT '',
 expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
