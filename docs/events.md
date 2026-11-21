# Messaging contract v1

Six partitions per topic, replication factor 2, minimum in-sync replicas 2.
Retention is explicit per topic: one hour for heartbeats, one week for commands,
results and their replayable sources, one month for DLQ evidence.
Commands/results use the tenant UUID as their Kafka key. Heartbeats use worker ID.

| Topic | Types | Consumer group |
|---|---|---|
| `tenant.tasks.v1` | `task.deploy`, `task.update`, `task.destroy` | `workers` |
| `tenant.task-updates.v1` | `task.in_progress`, `task.done`, `task.failed` | `control-plane` |
| `workers.heartbeats.v1` | `worker.heartbeat` | `control-plane` |

Each topic has a corresponding `.dlq`. Malformed, unsupported, or incorrectly
signed records are copied there with source topic, partition, offset, original
bytes, and a stable error code. A transient database or broker outage is retried,
not dead-lettered. Offsets advance only after successful durable processing or a
confirmed DLQ write.

```json
{
  "schema_version": 1,
  "id": "a2779b0a-1015-4db0-a536-67cd2689a405",
  "type": "task.deploy",
  "source": "control-plane",
  "time": "2026-11-21T10:00:00.000Z",
  "tenant_id": "321cbff8-96da-4f5c-9efb-f64544047260",
  "data": {
    "task": {
      "id": "a2779b0a-1015-4db0-a536-67cd2689a405",
      "type": "deploy",
      "tenant_id": "321cbff8-96da-4f5c-9efb-f64544047260",
      "status": "accepted",
      "created_at": "2026-11-21T10:00:00.000Z",
      "updated_at": "2026-11-21T10:00:00.000Z"
    },
    "slug": "acme-prod",
    "infra_secret_id": "control-plane/tenants/acme-prod/infra"
  },
  "token": "<HMAC-SHA256 JWT signature>"
}
```

`task.update` additionally accepts `pending_name` and/or
`sealed_database_url` in `data`; `task.destroy` uses just the metadata and secret
reference. Only active tenants can produce update commands.

Results contain `task_id`, `tenant_id`, `type` (the operation), `status`, and
an optional sanitized `error_code`. Their UUIDv5 ID is deterministic for
`task_id|status`, which is the idempotency key: retransmission preserves
identity, so a duplicate cannot apply twice. A command ID equals the task ID.

## Signatures

The token is a compact JWS-style `header.claims.signature`. Claims carry `jti`
(the event id) and a SHA-256 digest of the **whole envelope without the token**,
so nothing inside a signed message can be substituted while the signature still
verifies. The signature is HMAC-SHA256 over `header.claims` under `M2M_SECRET`,
a 32-byte base64 value in `.env`.

* Deterministic across restarts and reproductions. That is the whole reason the
  queue's trust anchor moved out of an emulator-held KMS alias: a restarted
  LocalStack lost `alias/cp-m2m`, and every previously signed message became
  unverifiable.
* Durable messages do not expire while queued; these are message signatures,
  not short-lived bearer tokens. A message that sits in a partition for an hour
  must still be processable.
* Every worker shares the one HMAC key, as requested: a principal that can sign
  can produce results. Per-worker identities are future work.

## Sealed connection strings

An imported database URL never appears in plaintext in the control-plane
database, in Kafka, or in a log. `PATCH` seals it with NaCl `SealAnonymous`
against `CONTROL_PLANE_RECEIVE_KEY` — the public half of the worker keypair —
and the ciphertext becomes part of the immutable task command. Only the holder
of `WORKER_SEAL_KEY` opens it, and only the worker needs that. A sealed blob is
tagged with its key version; a blob from another generation is refused rather
than silently mis-opened.

Sealed boxes are non-deterministic (fresh ephemeral key per call), so equal
URLs never produce equal ciphertext and cannot be fingerprinted.

## Delivery and state guarantees

* Tenant mutation and one immutable command/task are committed together.
* After commit the request hands the task id to the relay goroutine. A
  one-second sweeper also republishes any task still flagged unpublished after
  five seconds, using `FOR UPDATE SKIP LOCKED` so replicas cannot race.
* `CP_FAST_PUBLISH=false` disables only the in-request hand-off. The sweeper
  alone then publishes, which makes the recovery path deterministic.
* A crash after Kafka acknowledges but before Postgres marks published can
  duplicate delivery. Exactly one **logical** command exists, not exactly one
  physical broker record. Consumers handle that distinction explicitly.
* Inbound inbox insertion, task changes, and tenant/version changes commit in
  one transaction before the Kafka offset commit.
* A terminal result received before its `in_progress` is held in the inbox and
  applied when progress arrives. The public state machine remains
  `accepted → in_progress → done|failed`; there is no `accepted → done` edge.
* Terminal tasks never regress, including when a different result arrives later.
* Worker receipts survive restarts on the separate tenant resource server.
  A per-tenant advisory lock there serializes external side effects during
  consumer-group rebalance overlap. This is not control-plane leader election.
* Secret intent (including the generated password) is durable before
  `CREATE ROLE`. A failed attempt resumes with the same credential; destroy
  removes the recorded managed resources.
