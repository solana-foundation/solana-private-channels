# Runbook - Secret Rotation

How to rotate every secret of a stack deployed with `private-channel-deploy`. Signing
keys (channel admin, withdraw operator, `Instance.admin`) are on-chain identities and
have their own runbook: [`key_rotation.md`](key_rotation.md).

## How secrets reach the stack

- The source of truth is `private-channel-deploy/secrets.yml` (file backend) or a Doppler
  config (Doppler backend). See the deploy README, "Secrets backends".
- Each deploy writes every container's secrets to
  `{{ host_data_dir }}/secrets/<service>/<KEY>` on the host: a root-owned 0700 parent,
  0755 service dirs and 0444 files. Each container mounts only its own dir at
  `/run/secrets`. The rendered `.env` keeps the keys but no values.
- Each service carries a `secrets-rev` label derived from its secret values. A changed
  secret changes the label, and the deploy recreates exactly the services that read it.
- So every rotation is: change the value at the source, then redeploy.

```bash
cd private-channel-deploy && ansible-playbook deploy.yml -l <group>
```

Check what a container reads, as root on the host:

```bash
sudo ls -l {{ host_data_dir }}/secrets/<service>/
docker inspect -f '{{ index .Config.Labels "secrets-rev" }}' private-channel-<service>
```

## `scripts/rotate-secret.sh`

```
scripts/rotate-secret.sh <key> [--backend file|doppler --project P --config C]
                               [--value-stdin] [--finish] [--secrets-file PATH]
```

- **Generated keys** get a fresh random value: `openssl rand -hex 16` for the Postgres
  passwords and `grafana_admin_password`, `openssl rand -hex 32` for `jwt_secret`.
- **Supplied keys** (tokens, webhooks, provider URLs, signer secrets) are refused without
  `--value-stdin`. The value is read from stdin, and refused if empty or containing a
  newline.
- **File backend** (default): edits `private-channel-deploy/secrets.yml` in place, or
  `--secrets-file PATH`. Only the target line changes, the value is written as a
  single-quoted YAML scalar, and the file stays 0600. A key that is not in the file yet
  is appended.
- **Doppler backend**: `--backend doppler --project P --config C`, both required. It runs
  `doppler secrets set` with the value on stdin and `DOPPLER_TOKEN` unset, so it uses
  your own `doppler login`, which needs write access. The deploy still reads with its
  read-only service token. Doppler names are the upper-case key, except `notify_webhook`
  (`ALERT_WEBHOOK_URL`) and `alert_webhook_url` (`GRAFANA_ALERT_WEBHOOK_URL`).
- No value reaches a command line or the output. The script ends by printing the
  redeploy command.

Examples:

```bash
# Generated value, file backend
scripts/rotate-secret.sh postgres_runtime_password

# Generated value, Doppler backend
scripts/rotate-secret.sh postgres_runtime_password --backend doppler --project <project> --config <config>

# Supplied value: read it without echo, then pipe it in
read -rs NEW && printf '%s' "$NEW" | scripts/rotate-secret.sh ghcr_token --value-stdin; unset NEW
```

## Every secret

| Key (Doppler name) | Kind | Read by | Notes |
|---|---|---|---|
| `postgres_password` | generated | primary and replica | Primary superuser. See [Postgres](#postgres) |
| `postgres_replication_password` | generated | primary, replica, `pg-backup-primary` | Replicator. See [Postgres](#postgres) |
| `postgres_runtime_password` | generated | write-node, read-node, streamer (localnet) | |
| `postgres_auth_runtime_password` | generated | auth | |
| `postgres_auth_owner_password` | generated | auth-migrate | |
| `postgres_gateway_password` | generated | gateway | |
| `postgres_monitoring_password` | generated | primary postgres-exporter | |
| `postgres_grafana_password` | generated | Grafana datasource | Indexer cluster |
| `postgres_indexer_password` | generated | indexers, operators, streamer (localnet), `pg-backup-indexer`, indexer exporter | Indexer cluster superuser |
| `jwt_secret` (`JWT_SECRET`) | generated | gateway, auth | See [JWT](#jwt) |
| `jwt_secret_previous` (`JWT_SECRET_PREVIOUS`) | set by the script | gateway, auth | Only during a JWT rotation |
| `grafana_admin_password` | generated | Grafana | See [Grafana](#grafana-admin) |
| `ghcr_token` | supplied | control node and the pull only | See [GHCR](#ghcr-pat) |
| `notify_webhook` (`ALERT_WEBHOOK_URL`) | supplied | indexers, operators, report phase | See [Webhooks](#webhooks) |
| `alert_webhook_url` (`GRAFANA_ALERT_WEBHOOK_URL`) | supplied | Grafana contact point | See [Webhooks](#webhooks) |
| `rpc_url` (`RPC_URL`) | supplied | indexer-solana, both operators | Off localnet only. See [Provider URLs](#provider-urls-and-yellowstone) |
| `fallback_rpc_url` (`FALLBACK_RPC_URL`) | supplied | indexer-solana, withdraw operator | Off localnet only |
| `gateway_whitelist_rpc_url` (`GATEWAY_WHITELIST_RPC_URL`) | supplied | gateway | |
| `yellowstone_token` | supplied | indexer-solana | Off localnet only |
| `admin_vault_token`, `operator_vault_token` | supplied | operators | See [Signer secrets](#signer-secrets) |
| `admin_turnkey_api_private_key`, `operator_turnkey_api_private_key` | supplied | operators | |
| `admin_privy_app_secret`, `operator_privy_app_secret` | supplied | operators | |

`ghcr_user` is not rotated by the script: change it by hand together with the token.
Doppler `ADMIN_KEYPAIR` and `OPERATOR_KEYPAIR` are signing keys, see
[`key_rotation.md`](key_rotation.md).

## Postgres

```bash
scripts/rotate-secret.sh <postgres key>
cd private-channel-deploy && ansible-playbook deploy.yml -l <group>
```

Every deploy applies all passwords to the running clusters before the services start, over
the containers' trusted local socket, so the old password is never needed:

- the five primary service logins, from `init-auth-roles.sql`;
- the primary superuser (`postgres_password`) and replicator
  (`postgres_replication_password`), with `ALTER ROLE`;
- the indexer superuser (`postgres_indexer_password`) and the Grafana login on the
  indexer cluster.

After the full `up`, when the replica is running, the deploy rewrites its
`primary_conninfo` with the new replicator password (keeping user, host, port and other
fields) and reloads it, so streaming reconnects with the new password. The services that
read a rotated password, `pg-backup-*` included, are recreated by their `secrets-rev`
label.

Check after a replicator rotation:

```bash
docker exec private-channel-postgres-replica psql -U <postgres_user> -d postgres -tAc 'SELECT status FROM pg_stat_wal_receiver'
docker exec private-channel-postgres-primary psql -U <postgres_user> -d <postgres_db> -tAc 'SELECT count(*) FROM pg_stat_replication'
```

The first should print `streaming`, the second at least `1`.

A raw compose stack (`make docker-*`) does none of this; use `scripts/migrate-stack.sh`
there for the service logins.

## JWT

Auth signs tokens with `jwt_secret` (HS256, 24 h lifetime). During a rotation
`jwt_secret_previous` holds the old secret, and both auth and the gateway accept tokens
signed with either.

**Routine rotation** (nobody is logged out):

1. Stage it. The current secret moves to `jwt_secret_previous` and a new one is generated.
   Refused if a previous secret is already set.

   ```bash
   scripts/rotate-secret.sh jwt_secret [--backend doppler --project P --config C]
   cd private-channel-deploy && ansible-playbook deploy.yml -l <group>
   ```

2. Wait at least 24 hours, so every token signed with the old secret has expired.
3. Finish it. `jwt_secret_previous` is cleared (deleted in Doppler). Refused if it is
   already empty.

   ```bash
   scripts/rotate-secret.sh jwt_secret --finish [--backend doppler --project P --config C]
   cd private-channel-deploy && ansible-playbook deploy.yml -l <group>
   ```

Until step 3, any token signed with the previous secret is accepted, including tokens
newly forged with it if it leaked. Roles are still checked against the database, but a
forger can claim any user.

**After a compromise** of `jwt_secret` or `jwt_secret_previous`, do a hard cut instead:
replace `jwt_secret` and leave `jwt_secret_previous` empty, then redeploy. Every issued
token stops working at once and users log in again.

- File backend: if a rotation is in progress, run `--finish` first. Then set
  `jwt_secret` in `secrets.yml` by hand to a new `openssl rand -hex 32` value.
- Doppler backend: `scripts/rotate-secret.sh jwt_secret --finish --backend doppler ...`
  if a rotation is in progress, then
  `openssl rand -hex 32 | tr -d '\n' | env -u DOPPLER_TOKEN doppler secrets set JWT_SECRET --silent --project P --config C`.

Preflight, auth and the gateway refuse a previous secret under 32 bytes, equal to the
current one, or without a current one.

## Grafana admin

```bash
scripts/rotate-secret.sh grafana_admin_password
cd private-channel-deploy && ansible-playbook deploy.yml -l <group>
```

Grafana applies its admin password only when it creates the admin user, so the monitoring
phase resets the stored password on every deploy. Log in as `admin` with the new value.

## GHCR PAT

1. Create a new classic PAT with exactly `read:packages`, ideally on an org-scoped bot
   account. Fine-grained `github_pat_` tokens and any other scope set are refused.
2. `read -rs NEW && printf '%s' "$NEW" | scripts/rotate-secret.sh ghcr_token --value-stdin; unset NEW`
3. Redeploy. Preflight checks the scopes with GitHub, and the pull uses the token from a
   throwaway docker config, so the new token takes effect on this deploy.
4. Revoke the old PAT on GitHub.

## Webhooks

Webhook URLs carry their own credential.

- `notify_webhook` (Doppler `ALERT_WEBHOOK_URL`, `NOTIFY_WEBHOOK` also accepted): the
  indexers' and operators' row alerts, the escrow operator's reconciliation halts, and
  the deploy report.
- `alert_webhook_url` (Doppler `GRAFANA_ALERT_WEBHOOK_URL`): Grafana's contact point.
  Empty keeps the no-op default.

```bash
read -rs NEW && printf '%s' "$NEW" | scripts/rotate-secret.sh notify_webhook --value-stdin; unset NEW
cd private-channel-deploy && ansible-playbook deploy.yml -l <group>
```

Revoke the old URL at the provider after the deploy. A test alert from Grafana
(Alerting, Contact points, Test) confirms the contact point.

## Provider URLs and Yellowstone

`rpc_url`, `fallback_rpc_url` and `gateway_whitelist_rpc_url` usually carry a provider API
key, and `yellowstone_token` is one.

```bash
read -rs NEW && printf '%s' "$NEW" | scripts/rotate-secret.sh rpc_url --value-stdin; unset NEW
cd private-channel-deploy && ansible-playbook deploy.yml -l <group>
```

- A value in `secrets.yml` overrides the same key in `vars/<env>.yml`. A non-empty Doppler
  value does too; an empty one keeps the vars value.
- On localnet the RPC URLs point at the bundled validator and are not secret files.
- Issue the new provider key before rotating and revoke the old one after the deploy,
  so the indexer and operators never run without a working endpoint.

## Signer secrets

The Vault token, Turnkey API private key or Privy app secret of a remote signer. The
signing key itself does not change, so nothing happens on chain.

1. Issue the new credential at the provider (a new Vault token with the same policy, a new
   Turnkey API key pair, a new Privy app secret).
2. `read -rs NEW && printf '%s' "$NEW" | scripts/rotate-secret.sh admin_vault_token --value-stdin; unset NEW`
   (or the operator, Turnkey or Privy key). A new Turnkey API key pair also changes
   `<role>_turnkey_api_public_key` in vars.
3. Redeploy. Only the operators that read it are recreated.
4. Revoke the old credential.
