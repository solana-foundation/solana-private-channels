# private-channel-auth

Authentication service for the Solana Private Channels platform. Handles user registration, login, and Solana wallet verification. Issues JWTs consumed by the gateway for RBAC enforcement.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `AUTH_PORT` | `8903` | Port to listen on |
| `AUTH_DATABASE_URL` | — | Postgres connection URL |
| `JWT_SECRET` | — | HS256 signing secret. Must match the gateway's `JWT_SECRET`. |
| `CORS_ALLOWED_ORIGIN` | `*` | Value for `Access-Control-Allow-Origin`. Set to your frontend origin in production (e.g. `https://app.example.com` — placeholder, replace with your real domain before use). Defaults to `*` for local dev. |
| `AUTH_DATABASE_MAX_CONNECTIONS` | `10` | Maximum Postgres pool size. Increase under high concurrency. |
| `AUTH_ARGON2_MAX_CONCURRENCY` | `4` | Concurrent Argon2 hashes. Hashing is CPU-bound, so past the core count this costs memory without adding throughput. |
| `AUTH_RATE_LIMIT_PER_SECOND` | `5` | Sustained per-IP request rate for `/auth/register` and `/auth/login`. |
| `AUTH_RATE_LIMIT_BURST` | `10` | Burst allowance above the sustained per-IP rate. |
| `AUTH_USERNAME_ATTEMPTS_PER_MINUTE` | `5` | Credential attempts per minute against a single username, across all IPs. |
| `AUTH_MAX_CONNECTIONS` | `1024` | Maximum concurrent client connections. Past this, a new connection is dropped rather than queued. |
| `AUTH_MAX_CONNECTIONS_PER_IP` | `64` | Maximum concurrent connections from one client IP, so one host cannot take the whole budget. |
| `AUTH_HEADER_READ_TIMEOUT_SECS` | `10` | Seconds a client may take to send a full request header block (slowloris protection). Doubles as the idle timeout: see below. |
| `AUTH_REQUEST_TIMEOUT_SECS` | `15` | Seconds a request may take once its headers are in, covering the body read and the handler. Shed as `503`. Must stay above the 5s pool acquire timeout, or pool exhaustion is cancelled before `/health` can observe it. |
| `AUTH_TCP_KEEPALIVE_IDLE_SECS` | `60` | Idle seconds before the OS starts sending TCP keepalive probes. A backstop: the header timeout closes an idle connection first, so this only bites once that value is raised. |
| `AUTH_TCP_KEEPALIVE_INTERVAL_SECS` | `15` | Seconds between TCP keepalive probes. |

The credential routes run Argon2, which is deliberately CPU and memory heavy. They are
rate limited per IP and per username, capped at `AUTH_ARGON2_MAX_CONCURRENCY` concurrent
hashes, and limited to a 4 KB request body. Over-budget requests get `429`; requests that
wait too long for a hashing slot, or outrun `AUTH_REQUEST_TIMEOUT_SECS`, get `503`.

Those budgets are per request, so they are only charged once a request arrives. Connections
are bounded separately by `AUTH_MAX_CONNECTIONS` and `AUTH_MAX_CONNECTIONS_PER_IP`, and the
header and request timeouts close clients that connect and then stall. These hold whether or
not an ingress proxy adds limits of its own, so a proxy bypass still meets a bounded listener.

`AUTH_HEADER_READ_TIMEOUT_SECS` is also the idle timeout. The header deadline is re-armed for
each request on a kept-alive connection, so an idle connection is closed that many seconds
after the previous response. Keep a pooled client's idle timeout below it: a client that
reuses a connection the server has just closed sees `connection closed before message
completed`, and a `POST` in that race is not safely retryable. Raise this value if a client
you don't control pools for longer.

Because the per-IP limit keys on the peer address, the service must be reached directly.
Putting it behind a proxy without forwarding the client address would bucket every user
into the proxy's IP.

Both per-IP budgets key on that address masked to a /64 for IPv6, so a client handed a whole
/64 gets one budget rather than one per address in it. Where addresses are aggregated — an
ingress proxy, IPv4 CGNAT, an office /64 — the connection cap is the harsher of the two and
needs raising. An over-budget *request* gets a `429` the client can retry, but an over-cap
*connection* is dropped with no response at all, which a browser reports as a network error.
`AUTH_MAX_CONNECTIONS_PER_IP=64` is roughly ten browsers.

The in-container health probe reaches `/health` over loopback, so it competes for
`AUTH_MAX_CONNECTIONS` with everyone else. Under Compose that is harmless, but a Kubernetes
liveness probe would turn a connection flood into a restart loop: give the probe its own
listener or budget before relying on one.

## API

All endpoints are under `/auth`.

### `POST /auth/register`

Create a new account. All users are registered with the `user` role.

```json
{ "username": "alice", "password": "hunter2" }
```

Username requirements: 5–32 characters, alphanumeric plus underscores and hyphens only.

Password requirements: 6–128 characters. The cap is measured in characters, not bytes.

Returns the created user. Passwords are hashed with Argon2 and never returned.

---

### `POST /auth/login`

Authenticate and receive a signed JWT (valid for 24 hours).

```json
{ "username": "alice", "password": "hunter2" }
```

Returns `{ "token": "<jwt>" }`. Both wrong username and wrong password return `401` to prevent username enumeration. Credentials over the length caps also return `401` rather than a validation error, so the response surface stays uniform.

---

### `POST /auth/challenge-wallet` 🔒

Request a sign challenge to prove ownership of a Solana wallet. Requires a valid JWT.

Returns a `message`, `nonce`, and `expires_at`. The challenge expires in 10 minutes.

```json
{
  "message": "Solana Private Channels wallet verification\nuser: <uuid>\nnonce: <uuid>\nexpires: <unix>",
  "nonce": "<uuid>",
  "expires_at": "<iso8601>"
}
```

---

### `POST /auth/verify-wallet` 🔒

Submit the signed challenge to register a wallet as verified. Requires a valid JWT.

```json
{
  "pubkey": "<base58 pubkey>",
  "nonce": "<uuid from challenge>",
  "signature": "<base58 signature>"
}
```

The service reconstructs the exact challenge message, verifies the Ed25519 signature against the provided pubkey, then stores the wallet. Each nonce can only be consumed once — replays are rejected.

---

### `GET /auth/wallets` 🔒

List all verified wallets for the authenticated user. Requires a valid JWT.

---

### `GET /health`

Liveness check. Returns `200 ok`.

## Roles

There are two roles: `user` (default) and `operator`.

| Role | Description |
|---|---|
| `user` | Standard role. All registered accounts start as `user`. |
| `operator` | Elevated role. Can call operator-only methods on the gateway without ownership checks. |

**Operators must be provisioned with the admin CLI** — there is no API to assign or escalate to the operator role. This is intentional: operator access is an infrastructure-level concern, not a self-service one.

Never provision by username. Usernames are claimed first-come on `/auth/register`, so anyone who registers the intended operator's name before you promote it receives the operator role instead. The admin CLI takes the immutable user id for exactly this reason.

## Admin CLI

Operator-only commands for managing users directly against the auth database.

| Variable | Description |
|---|---|
| `AUTH_DATABASE_URL` | Same DB the auth service uses. |
| `AUTH_ADMIN_ACTOR` | Who is running the command. Required for `set-role` and `attach-wallet`; recorded in the audit trail. |

Both mutating commands print the target's id, username, current role and creation time and wait for a typed `yes` before proceeding. `--yes` skips the prompt for scripted use.

### Provisioning flow

Ask the account owner for their user id out of band — the registration response and the JWT `sub` claim both carry it. Look that id up and check the username and creation time match the account you mean to grant:

```bash
AUTH_DATABASE_URL=postgres://... cargo run -p auth --bin auth-admin -- show-user --user-id <uuid>
```

Do not go the other way. `show-user --username alice` resolves a name to whoever holds it, which answers "is this name taken" but not "is this the person" — deriving the id from a name reintroduces exactly the confusion the id-based commands exist to prevent.

### Set a user's role

```bash
AUTH_DATABASE_URL=postgres://... AUTH_ADMIN_ACTOR=you@example.com cargo run -p auth --bin auth-admin -- set-role --user-id <uuid> --role operator
```

### Attach a wallet to a user

Inserts a row into `private_channel_auth.verified_wallets` without running the challenge/signature flow — the operator is asserting trust, the user does not prove ownership. Use this for provisioning or recovery, not as a substitute for the normal verification flow.

```bash
AUTH_DATABASE_URL=postgres://... AUTH_ADMIN_ACTOR=you@example.com cargo run -p auth --bin auth-admin -- attach-wallet --user-id <uuid> --pubkey <base58>
```

### Audit trail

Every role change and administrative wallet attach writes a row to `private_channel_auth.admin_audit` in the same transaction as the change itself, recording the actor, action, target user id and detail (`user -> operator`, or the attached pubkey). The `set-role` detail is read by the same statement that performs the update, so it records the role actually replaced.

This is the trail of privileged grants — one account acting on another. Self-service wallet changes are not in it: verification proves key ownership before it stores anything, and removal only ever touches the caller's own wallets.

Nothing in the service updates or deletes from that table. The CLI only runs DDL when the schema is missing, so it works under a role with no create rights; if the trail needs to survive a compromised admin credential, that role should also have `INSERT` but not `UPDATE`/`DELETE` on the audit table.

```sql
SELECT * FROM private_channel_auth.admin_audit ORDER BY created_at DESC;
```

## Wallet verification flow

Wallets are not trusted on assertion — the user must cryptographically prove they control the private key.

```
1. POST /auth/challenge-wallet
   ← { message, nonce, expires_at }

2. Sign `message` with the wallet's private key (Ed25519)

3. POST /auth/verify-wallet  { pubkey, nonce, signature }
   ← { pubkey, created_at }
```

Once verified, the gateway allows that user to query accounts owned or delegated by that wallet (ATAs, token accounts, etc.). Transaction history is the exception: `getSignaturesForAddress` requires the wallet to be the token account's owner, not its delegate.

## JWT format

Tokens are signed with HS256. The payload contains:

```json
{
  "sub": "<user uuid>",
  "role": "user | operator",
  "iss": "private-channel-auth",
  "aud": "private-channel-gateway",
  "exp": <unix timestamp>
}
```

The gateway validates `iss`, `aud`, and `exp` on every request. A token issued by any other service, even with the same secret but missing these claims, will be rejected.

## Running tests

```
cargo test --test integration -- --test-threads=1
```

Tests spin up a real Postgres via Docker (testcontainers). Docker must be running.
