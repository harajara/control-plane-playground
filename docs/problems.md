# Problem responses

Every error is RFC 9457 `application/problem+json` with `type`, `title`,
`status`, `code`, `detail`, `instance`. `code` is the stable, machine-readable
half; `type` points here.

| Status | Code | Meaning |
|---|---|---|
| 400 | `validation_failed` | Body, query or path parameters failed validation |
| 401 | `unauthenticated` | No or expired session cookie / API token |
| 403 | `forbidden` | `Origin` header does not match the request host |
| 404 | `tenant_not_found`, `task_not_found` | Unknown identifier |
| 404 | `route_not_found` | Path is not part of the API (normalized from the router) |
| 405 | `method_not_allowed` | Path exists but not for this verb (normalized) |
| 409 | `tenant_already_exists` | Slug already taken, including destroyed tenants |
| 409 | `tenant_version_conflict` | PATCH `version` is stale |
| 409 | `tenant_update_not_allowed` | Tenant is not in a state that allows the change |
| 409 | `user_already_exists` | Registration email is taken |
| 415 | `unsupported_media_type` | Request body is not `application/json` |
| 500 | `internal_error` | Recovered panic; details are never echoed |
| 4xx/5xx | `request_failed` | Any other error response that was not already a problem |
| 503 | `service_unavailable` | A dependency (database) is unavailable; retry |

Anything the router rejects on its own — unmatched path, wrong method, a
handler that forgot to set a problem content type — is normalized by the
response wrapper, so a client never sees Go's plain-text `404 page not found`.

Semantics worth knowing:

* PATCH checks existence, then version, then tenant state, in that order.
* DELETE is allowed for `active` and `failed` tenants only.
* A destroyed tenant's slug stays reserved forever; retries hit
  `tenant_already_exists`.
* Problem details never contain credentials, connection strings or key
  material; the same rule applies to logs.

There is no `429`: rate limiting lived in the removed nginx gateway. Put a
limiting proxy in front if you need one.
