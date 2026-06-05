# private-channel-auth

Authentication service for the Solana Private Channels platform. Handles user registration, login, and Solana wallet verification. Issues JWTs consumed by the gateway for RBAC enforcement.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `AUTH_PORT` | `8903` | Port to listen on |
| `AUTH_DATABASE_URL` | — | Postgres connection URL |
| `JWT_SECRET` | — | Non-empty HS256 signing secret. Must match the gateway's `JWT_SECRET`. |
| `CORS_ALLOWED_ORIGIN` | `*` | Value for `Access-Control-Allow-Origin`. Set to your frontend origin in production (e.g. `https://app.example.com` — placeholder, replace with your real domain before use). Defaults to `*` for local dev. |
| `AUTH_DATABASE_MAX_CONNECTIONS` | `10` | Maximum Postgres pool size. Increase under high concurrency. |

## API

All endpoints are under `/auth`.

### `POST /auth/register`

Create a new account. All users are registered with the `user` role.

```json
{ "username": "alice", "password": "hunter2" }
```

Username requirements: 5–32 characters, alphanumeric plus underscores and hyphens only.

Password requirements: 6–72 characters (Argon2's input limit — inputs beyond 72 bytes are silently truncated, so longer passwords are rejected outright).

Returns the created user. Passwords are hashed with Argon2 and never returned.

---

### `POST /auth/login`

Authenticate and receive a signed JWT (valid for 24 hours).

```json
{ "username": "alice", "password": "hunter2" }
```

Returns `{ "token": "<jwt>" }`. Both wrong username and wrong password return `401` to prevent username enumeration.

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

**Operators must be provisioned directly in the database** — there is no API to assign or escalate to the operator role. This is intentional: operator access is an infrastructure-level concern, not a self-service one.

```sql
UPDATE private_channel_auth.users SET role = 'operator' WHERE username = 'alice';
```

## Admin CLI

Operator-only commands for managing users directly against the auth database. Requires `AUTH_DATABASE_URL` (same DB the auth service uses).

### Attach a wallet to a user

Inserts a row into `private_channel_auth.verified_wallets` without running the challenge/signature flow — the operator is asserting trust, the user does not prove ownership. Use this for provisioning or recovery, not as a substitute for the normal verification flow.

```bash
AUTH_DATABASE_URL=postgres://... cargo run -p auth --bin admin -- attach-wallet --username alice --pubkey <base58>
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

Once verified, the gateway allows that user to query accounts owned or delegated by that wallet (ATAs, token accounts, etc.).

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
