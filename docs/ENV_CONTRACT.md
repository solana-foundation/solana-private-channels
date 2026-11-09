# Environment variable contract

Three surfaces start the same Docker stack — the **Makefile** (`make docker-*`),
**raw `docker compose`** in docs/runbooks, and the **Ansible deploy**
(`private-channel-deploy`). They legitimately source variable *values* from
different places, but they must agree on the variable *contract*: the key set,
their meaning, and the load order. This doc is that contract.

## Canonical key list

[`.env.example`](../.env.example) is the **single source of truth** for which
variables exist. Every key any surface references must be documented there.
`scripts/check-env-contract.sh` enforces this — it fails if `.env.local`,
`.env.devnet`, or `private-channel-deploy/templates/env.j2` references a key
`.env.example` doesn't document. It runs automatically before `make docker-up` /
`docker-devnet-up` (via `check-env-local` / `check-env-devnet`) and can be run
directly with `make check-env-contract`. It compares **names only, never values**.

## Value source per surface

The contract is "same keys, same precedence" — *not* "same files". Where a value
comes from differs on purpose:

| Surface | Version pins | Environment values | Secrets |
|---|---|---|---|
| **Makefile (local)** | `versions.env` | tracked `.env.local` | filled into gitignored `.env` (or `.env.local`) by the operator / `make build-*` |
| **Makefile (devnet)** | `versions.env` | tracked `.env.devnet` | gitignored `.env` |
| **Ansible deploy** | `versions.env` (copied to target) | rendered `.env` from `templates/env.j2` | `secrets.yml` or Doppler, written to per-service files, **not** to `.env` (see below) |

Local dev keeps **checked-in, secret-free presets** so `git clone && make docker-up`
works out of the box; the deploy delivers **real secrets** that must never be
committed. Same contract, different source — by design.

### Ansible deploy: secrets as files

The deploy never puts a secret in `.env`, the compose config, `docker inspect`, argv or a
world-readable file:

- The rendered `.env` keeps every secret key **present but empty**, so compose does not
  warn and a `${X:-default}` key takes its non-secret default.
- Each container's secrets are files under `{{ host_data_dir }}/secrets/<service>/<KEY>`
  (root-owned: 0700 parent, 0755 service dirs, 0444 files), mounted read-only at
  `/run/secrets`. The compose override sets `KEY_FILE=/run/secrets/KEY` for each one, and
  only for non-empty values. The set per service is `secret_files` in
  `private-channel-deploy/vars/common.yml`.
- The app image's entrypoint, `/usr/local/bin/with-secrets`, exports `KEY` from the file
  named by `KEY_FILE` (only under `/run/secrets/`), unsets `KEY_FILE`, and execs the
  command. A missing or empty file stops the container, naming `KEY` but never the
  value. The deploy refuses an app image without this entrypoint.
- Postgres reads `POSTGRES_PASSWORD_FILE` natively, the init SQL and the replica command
  read `X_FILE` when set and `X` otherwise (so raw compose still works), Grafana uses
  `__FILE` and `$__file{}`, and the exporters `DATA_SOURCE_PASS_FILE`.
- Secrets in files: the nine Postgres passwords (and the DB URLs built from them),
  `JWT_SECRET`, `JWT_SECRET_PREVIOUS`, Grafana's admin password, both webhooks, the
  signer secrets (`*_PRIVATE_KEY`, `*_VAULT_TOKEN`, `*_TURNKEY_API_PRIVATE_KEY`,
  `*_PRIVY_APP_SECRET`), `INDEXER_YELLOWSTONE_TOKEN`, and off localnet the provider RPC
  URLs (`COMMON_RPC_URL`, `COMMON_SOURCE_RPC_URL`, `COMMON_FALLBACK_RPC_URL`,
  `INDEXER_BACKFILL_RPC_URL`) and `GATEWAY_WHITELIST_RPC_URL`.

To read what a container sees, use `sudo` on the host, not `.env`:
`sudo cat {{ host_data_dir }}/secrets/<service>/<KEY>`.

## Precedence (load order)

Every surface layers env files in the same order, later files winning on conflict:

```
versions.env  →  <environment file>  →  [optional .env override]
```

- **Makefile:** `--env-file versions.env --env-file .env.local|.env.devnet [--env-file .env]`
- **Deploy:** `--env-file versions.env --env-file .env` (the rendered file plays the
  role of the environment file)

`versions.env` (toolchain/image pins) is **universal** — always first, on every
surface, including doc snippets. Never start the stack without it.

## Required secrets (fail-closed)

`scripts/check-required-env.sh` enforces that a non-empty value resolves for the
required secrets before the stack starts. It is shared: the Makefile's
`check-env-*` targets and the Ansible deploy (after rendering `.env`) both run it,
so a blank secret is caught identically on both surfaces. Currently required:

- `POSTGRES_PASSWORD` — primary cluster superuser
- `POSTGRES_REPLICATION_PASSWORD` — streaming replication
- `POSTGRES_RUNTIME_PASSWORD` — write-node, read-node, streamer
- `POSTGRES_AUTH_RUNTIME_PASSWORD` — auth service
- `POSTGRES_AUTH_OWNER_PASSWORD` — auth schema owner; migrations + admin CLI
- `POSTGRES_GATEWAY_PASSWORD` — gateway; `SELECT` only
- `POSTGRES_MONITORING_PASSWORD` — postgres_exporter; `pg_monitor` only
- `POSTGRES_GRAFANA_PASSWORD` — Grafana's datasource; one view only
- `POSTGRES_INDEXER_PASSWORD` — indexer cluster superuser
- `GF_ADMIN_PASSWORD` — Grafana's admin login; the literal `admin` is rejected

No working defaults ship for these. Generate each with `openssl rand -hex 32`.
`JWT_SECRET` is required only when auth/RBAC is enabled (see `.env.example`). When set it must be at least 32 bytes after trimming; the gateway treats an empty value as auth off and refuses a whitespace-only one, and auth refuses an empty one. `GATEWAY_REQUIRE_JWT=1` (rendered by the Ansible deploy whenever RBAC is on) makes the gateway refuse a blank `JWT_SECRET` instead. `JWT_SECRET_PREVIOUS` is optional and set only during a rotation; the gateway and auth refuse it under 32 bytes, equal to `JWT_SECRET`, or without `JWT_SECRET`.

The Ansible deploy runs `check-required-env.sh` on its rendered `.env` with the secret
values passed in the checker's own process environment, which wins over the file, so the
check still sees them although `.env` holds none.

The nine `POSTGRES_*` values must also be **distinct from one another**, which the
same script enforces. Each names a login with different rights, so reusing one
hands a compromised service the others' access. The sharp case is
`POSTGRES_INDEXER_PASSWORD` equal to `POSTGRES_PASSWORD`: the indexer processes
share a network with `postgres-primary` and the primary's username is in this
repository, so an equal value lets them authenticate there as superuser.

## Adding or removing a variable

1. Add/remove it in **`.env.example`** first (the contract).
2. Update the surfaces that set it: `.env.local`, `.env.devnet`, and/or
   `templates/env.j2`.
3. If it's a secret with no safe default, add it to `REQUIRED_VARS` in
   `scripts/check-required-env.sh`.
4. Run `make check-env-contract` — it must pass before the change lands.

Keys with compose-side defaults (e.g. `PRIVATE_CHANNEL_VERSION`, `STREAMER_PORT`)
are still documented in `.env.example` so the contract is complete, even though
omitting them from an env file is harmless.

`ESCROW_CONFIDENTIAL_KEYS` is an optional secret: the escrow instance's confidential
transfer keys, `{"elgamal_secret":"<base64 32 bytes>","aes_key":"<base64 16 bytes>"}`,
read by `indexer-solana`, `operator-solana` and `operator-private-channel` and by no
other service. Every surface ships it blank, which the services read as not
configured. It becomes required once any escrow account is configured for confidential
transfers: then those services refuse to start without the key that account holds.
Single-quote it in an env file so compose reads the JSON literally.
`scripts/check-required-env.sh` refuses a set value that is not that JSON shape (the
message never shows the value), and the deploy asserts the same of
`escrow_confidential_keys` (`secrets.yml`, or `ESCROW_CONFIDENTIAL_KEYS` in Doppler),
which `templates/env.j2` renders quoted, or blank when unset. Generate it with
`confidential_keygen` (see [confidential_transfers.md](runbooks/confidential_transfers.md#setup-ceremony)).

`ESCROW_CONFIDENTIAL_PUBKEY` is optional and not secret: the instance's ElGamal pubkey,
base64, exactly as the `ESCROW_CONFIDENTIAL_PUBKEY=` line `confidential_keygen` prints.
It reaches the same three services and ships blank everywhere. When set,
`indexer-solana`, both operators and `resync` refuse to start unless
`ESCROW_CONFIDENTIAL_KEYS` is set and its pubkey equals it, whatever the database holds;
the per-mint check against the escrow accounts the database knows still runs. Errors
name the variables only. `scripts/check-required-env.sh` and the deploy (with
`escrow_confidential_pubkey`, or `ESCROW_CONFIDENTIAL_PUBKEY` in Doppler) refuse a value
that is not base64 of 32 bytes, or one set while the keys are blank; `templates/env.j2`
renders it as is, or blank when unset.

`PRIVATE_CHANNEL_COMPLIANCE_OWNER` is optional; the node reads a blank value as unset.
`.env.example` documents it as a commented line, which the check does not count. Compose passes it through only when it is set, and
`templates/env.j2` renders it only when `compliance_owner` is set.

The permanent delegate is one value with two names: `PRIVATE_CHANNEL_PERMANENT_DELEGATE`
for the node and `OPERATOR_PERMANENT_DELEGATE` for the operators. They must be equal.
Neither has a fallback to an admin key: write and aio nodes and both operators refuse to
start without it. Compose fills a blank operator value from the node's, `make build-*` fills
a blank node value with the admin pubkey once, and the Ansible deploy renders both from the
required `permanent_delegate`.
The read node gets the same admin keys, delegate and compliance owner as the write node,
because it serves `simulateTransaction`.

Signers have one key set per role, `ADMIN_*` (the channel admin) and `OPERATOR_*` (the
withdraw operator's release key). `ADMIN_SIGNER` and `OPERATOR_SIGNER` pick `memory`
(`*_PRIVATE_KEY`), `vault` (`*_VAULT_ADDR`, `*_VAULT_TOKEN`, `*_VAULT_KEY_NAME`,
`*_VAULT_PUBKEY`), `turnkey` (`*_TURNKEY_API_PUBLIC_KEY`, `*_TURNKEY_API_PRIVATE_KEY`,
`*_TURNKEY_ORGANIZATION_ID`, `*_TURNKEY_PRIVATE_KEY_ID`, `*_TURNKEY_PUBKEY`) or `privy`
(`*_PRIVY_APP_ID`, `*_PRIVY_APP_SECRET`, `*_PRIVY_WALLET_ID`). `operator-solana` gets only
`ADMIN_*`; `operator-private-channel` gets both. In raw compose a blank
`OPERATOR_PRIVATE_KEY` falls back to `ADMIN_PRIVATE_KEY`, so a single-key dev env keeps
working; the remote backends' vars reach a container only through an override, which the
Ansible deploy renders for the selected backend. The deploy renders
`PRIVATE_CHANNEL_ADMIN_KEYS` from the admin pubkey plus `extra_admin_keys`, the overlap
used during a channel admin rotation.
