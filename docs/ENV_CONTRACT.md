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
| **Ansible deploy** | `versions.env` (copied to target) | rendered `.env` from `templates/env.j2` | templated from SOPS-backed Ansible vars into the rendered `.env` |

Local dev keeps **checked-in, secret-free presets** so `git clone && make docker-up`
works out of the box; the deploy templates **real secrets** that must never be
committed. Same contract, different source — by design.

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

- `POSTGRES_PASSWORD`
- `POSTGRES_REPLICATION_PASSWORD`

No working defaults ship for these. Generate values with `openssl rand -hex 32`.
`JWT_SECRET` is required only when auth/RBAC is enabled (see `.env.example`).

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
