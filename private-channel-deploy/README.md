# Solana Private Channels Operator Runbook (single host)

Single-host deployment automation for Solana Private Channels. Wraps `docker-compose.yml` so the deploy is one command, repeatable, and verifies itself.

**Phases** (fail-fast, idempotent): preflight → bootstrap → reset-state → render → deploy → health → monitoring → sanity → report.

## Prerequisites

Run [`scripts/install-prereqs.sh`](./scripts/install-prereqs.sh) once **on both the control node and the target host** (Ubuntu/Debian, idempotent). Preflight re-verifies them on every run and fails fast with an actionable message before touching the host.

```
./scripts/install-prereqs.sh        # run on control node, then again on the target host
```

> Uses `sudo` (apt + Docker repo). Run this in a interactive terminal that can show password prompts.

**Control node** (where you run `ansible-playbook`):

- `ansible >= 2.16`
- `docker >= 26` reachable as the running user
- extra Ansible modules (`community.general`)

**Target host** (the box being deployed to):

- `docker >= 26` + Compose plugin
- Writable `host_data_dir`
- Passwordless `sudo` for the SSH user

## Initial environment setup

Three one-time edits per environment, done before the first deploy. They tell the playbook where the host is, what to deploy, and which credentials to use.

1. **Point at a host** ([`inventory.ini`](./inventory.ini)): IP, SSH user, key path. [`inventory.example.ini`](./inventory.example.ini) has one group per network (`dev`, `devnet`, `mainnet`).
2. **Choose what to deploy** ([`vars/dev.yml`](./vars/dev.yml)): image registry/tag, target Solana network, on-host paths. Each option has an inline comment explaining what to pick. The inventory group selects `vars/<group>.yml`; for devnet and mainnet copy [`vars/devnet.example.yml`](./vars/devnet.example.yml) or [`vars/mainnet.example.yml`](./vars/mainnet.example.yml).
3. **Provide credentials**, via one of the two backends below. For the default file backend, copy the template and fill in real values:

   ```bash
   cp secrets.yml.example secrets.yml
   ```

   `secrets.yml` is plain text and gitignored. See the file header for required vs optional and how to generate each value. Doppler-only operators skip this file entirely — preflight does not require it when the doppler backend is selected.

Everything else is wired and shouldn't need routine changes.

### Choose your setup

Pick a network, a secrets backend and a signer backend, copy the files in that row, then run its command. Every row also needs `inventory.example.ini` copied to `inventory.ini` with the group you deploy. With Doppler, upload the secrets once with `scripts/doppler-import.sh` (or fill [`examples/doppler/secrets.template.json`](./examples/doppler/secrets.template.json)) instead of keeping `secrets.yml`, and `export DOPPLER_TOKEN=<read-only service token>` before the command.

| Network | Secrets | Signers | Files to copy and fill | Command |
| --- | --- | --- | --- | --- |
| localnet | file or Doppler | memory | `secrets.yml.example` to `secrets.yml` (`vars/dev.yml` is shipped) | `ansible-playbook deploy.yml -l dev` |
| devnet | file or Doppler | memory | `vars/devnet.example.yml` to `vars/devnet.yml`, `secrets.yml.example` to `secrets.yml` | `ansible-playbook deploy.yml -l devnet` |
| devnet | file or Doppler | vault, turnkey or privy | as above, plus the backend secret in `secrets.yml` or Doppler | `ansible-playbook deploy.yml -l devnet -e @examples/signers/<backend>.yml` |
| mainnet | file or Doppler | vault | `vars/mainnet.example.yml` to `vars/mainnet.yml` (Vault already set), `secrets.yml` or Doppler | `ansible-playbook deploy.yml -l mainnet` |
| mainnet | file or Doppler | turnkey or privy | as above | `ansible-playbook deploy.yml -l mainnet -e @examples/signers/<backend>.yml` |

- **Signers.** The channel admin (`admin_signer`) is fee payer, mint and freeze authority and withdraw fee treasury; the withdraw operator (`operator_signer`) signs `ReleaseFunds` with its own key. Each is `memory`, `vault`, `turnkey` or `privy` (default `memory`). [`examples/signers/`](./examples/signers/) has one file per backend with every var it needs; `-e @file` overrides the vars file. Mainnet refuses `memory` for either role.
- **Separate keys.** Off localnet the admin and operator pubkeys must differ, and the operator needs an Operator PDA: run `add_operator` for the operator pubkey with the escrow `Instance.admin` key before the deploy, or preflight refuses it. On localnet `keypairs/withdraw-operator.json` is generated next to `keypairs/admin.json`; once the instance exists, run `add_operator` for its pubkey before the deploy that sets `escrow_instance_id`, or preflight refuses it.
- **Image.** The app image must have the `with-secrets` entrypoint. The deploy refuses older images, `v0.1.0` included.

Key and secret rotation: [`docs/runbooks/key_rotation.md`](../docs/runbooks/key_rotation.md) and [`docs/runbooks/secret_rotation.md`](../docs/runbooks/secret_rotation.md).

### Secrets backends

The deploy reads secrets from one of two backends, chosen automatically:

- **file** (default): the gitignored `secrets.yml` above. No change for existing users.
- **doppler** (opt-in, per-operator): pulls the same keys from Doppler at deploy time. Auto-selected when `DOPPLER_TOKEN` is set in your shell; otherwise the file backend is used. Force either with `-e secrets_backend=file|doppler`.

Doppler setup (one time):

1. Create a Doppler project and config, and add the `secrets.yml` keys as secrets. Doppler names are the UPPERCASE of the file keys: `POSTGRES_PASSWORD`, `POSTGRES_REPLICATION_PASSWORD`, `POSTGRES_RUNTIME_PASSWORD`, `POSTGRES_AUTH_RUNTIME_PASSWORD`, `POSTGRES_AUTH_OWNER_PASSWORD`, `POSTGRES_GATEWAY_PASSWORD`, `POSTGRES_MONITORING_PASSWORD`, `POSTGRES_GRAFANA_PASSWORD`, `POSTGRES_INDEXER_PASSWORD`, `JWT_SECRET`, `JWT_SECRET_PREVIOUS` (only during a rotation), `GRAFANA_ADMIN_PASSWORD`, `GHCR_USER`, `GHCR_TOKEN`, `YELLOWSTONE_TOKEN`, `RPC_URL`, `FALLBACK_RPC_URL`, `GATEWAY_WHITELIST_RPC_URL`, and per signer role the backend secret: `ADMIN_VAULT_TOKEN`, `OPERATOR_VAULT_TOKEN`, `ADMIN_TURNKEY_API_PRIVATE_KEY`, `OPERATOR_TURNKEY_API_PRIVATE_KEY`, `ADMIN_PRIVY_APP_SECRET`, `OPERATOR_PRIVY_APP_SECRET`. A memory signer may take its key from `ADMIN_KEYPAIR` or `OPERATOR_KEYPAIR` (the keypair JSON byte array, which wins over `<role>_keypair_path`; refused on mainnet). Three names differ from the file keys: `ALERT_WEBHOOK_URL` is `notify_webhook` (`NOTIFY_WEBHOOK` is also accepted), `GRAFANA_ALERT_WEBHOOK_URL` is `alert_webhook_url`, and `DEVNET_YELLOWSTONE_ENDPOINT` (optional) overrides `yellowstone_endpoint` from `vars/<env>.yml`. An empty `RPC_URL`, `FALLBACK_RPC_URL`, `GATEWAY_WHITELIST_RPC_URL` or `DEVNET_YELLOWSTONE_ENDPOINT` keeps the vars value. [`examples/doppler/secrets.template.json`](./examples/doppler/secrets.template.json) lists every key.
2. Import your existing file in one shot with [`scripts/doppler-import.sh`](../scripts/doppler-import.sh). It applies the names above, skips empty values, keeps the plaintext JSON in a 0700 temp dir it always deletes, never prints a value, and uploads with your own `doppler login` (it ignores `DOPPLER_TOKEN`). It needs `python3` with PyYAML.
   ```bash
   # from private-channel-deploy/; --create-config creates the config first.
   ../scripts/doppler-import.sh --project <project> --config <config> [--create-config] secrets.yml
   ../scripts/doppler-import.sh --dry-run secrets.yml   # key names only, uploads nothing
   ```
3. Mint a **read-only** service token scoped to that config, then run deploys with it in the environment:
   ```bash
   export DOPPLER_TOKEN=dp.st.xxxxx
   ansible-playbook deploy.yml -l dev
   ```

The token pins the project + config, so the playbook needs nothing else. The file backend stays as a fallback: unset `DOPPLER_TOKEN` (or pass `-e secrets_backend=file`) and the deploy reads `secrets.yml` again. Cross-env defaults that you can override but rarely need to (db/user names, replication user, health-probe budgets) live in [`vars/common.yml`](./vars/common.yml); set the same key in [`vars/dev.yml`](./vars/dev.yml) to override per environment.

### Using GHCR

The deploy pulls images from GHCR. Build + push them once from the control node, then every host can `docker compose pull`.

1. **Create two GitHub Personal Access Tokens (classic PATs).**
   - **Deploy PAT** (goes in `secrets.yml`): `read:packages` [only](https://github.com/settings/tokens/new?scopes=read:packages), ideally on an org-scoped bot account. The deploy refuses any other scope set and fine-grained `github_pat_` tokens, and uses the PAT only for the pull, from a throwaway docker config it deletes afterwards.
   - **Push PAT** (stays on your workstation, never in `secrets.yml`): `write:packages`, used only for the manual push below. CI pushes with its own `GITHUB_TOKEN`.
2. **Set the Personal Access Token (PAT) in `secrets.yml`:**
   - `ghcr_user`: GitHub username
   - `ghcr_token`: the deploy (read-only) PAT from step 1.
3. **Build and push from the control node.** Set `image_registry` and `image_tag` in [`vars/dev.yml`](./vars/dev.yml) (e.g. `image_registry: ghcr.io/<your-github-username-lowercase>`, `image_tag: v0.1.0`), then run the snippet below from inside the `private-channel-deploy/` directory.

   ```bash
   export GHCR_USER=<your-github-username>
   export GHCR_TOKEN=<your-push-pat-from-step-1>
   OWNER=$(echo "$GHCR_USER" | tr '[:upper:]' '[:lower:]')   # GHCR namespace must be lowercase
   echo "$OWNER"           # sanity-check it's all lowercase
   TAG=v0.1.0              # must match image_tag in vars/dev.yml

   # Load pinned tool versions (SOLANA_VERSION, PNPM_VERSION, YELLOWSTONE_TAG, ...)
   # required as --build-arg by the Dockerfiles.
   set -a; source ../versions.env; set +a

   echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USER" --password-stdin
   docker build \
     --build-arg SOLANA_VERSION="$SOLANA_VERSION" \
     --build-arg PNPM_VERSION="$PNPM_VERSION" \
     -t ghcr.io/$OWNER/private-channel-app:$TAG ..
   docker build \
     --build-arg SOLANA_VERSION="$SOLANA_VERSION" \
     --build-arg YELLOWSTONE_TAG="$YELLOWSTONE_TAG" \
     -t ghcr.io/$OWNER/private-channel-validator:$TAG -f ../validator.Dockerfile ..
   docker push ghcr.io/$OWNER/private-channel-app:$TAG
   docker push ghcr.io/$OWNER/private-channel-validator:$TAG
   ```

   Image names must match what the compose stack expects: `<image_registry>/private-channel-app:<image_tag>` and `<image_registry>/private-channel-validator:<image_tag>`.

GHCR docs: [ref](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)

### Verifying image signatures

Images published by [`.github/workflows/publish-image.yml`](../.github/workflows/publish-image.yml) are signed keyless with cosign (OIDC, no stored keys). Set `verify_image_signature: true` in a stack's vars to make preflight refuse an app image that wasn't built and signed by that workflow: cosign resolves the tag to its digest and checks the signature's Fulcio identity + Rekor entry, so a tampered, unsigned, or foreign image aborts the deploy before any pull. Requires `cosign` on the control node (`brew install cosign`). Override the expected signer via `image_signer_identity` (a cosign `--certificate-identity-regexp`); the default matches this repo's workflow on main or a release tag.

## Operating

Cluster-agnostic by design, the commands don't change between localnet, devnet/mainnet. The deploy target is set once in [`vars/dev.yml`](./vars/dev.yml) (`network` + `rpc_url`); the playbook auto-selects the matching compose file ([`docker-compose.yml`](../docker-compose.yml) for localnet, [`docker-compose.devnet.yml`](../docker-compose.devnet.yml) for devnet/mainnet) and renders a per-env `.env` from `vars/dev.yml`. Secrets never go in that `.env` (their keys are present but empty): each container reads only its own, from files under `{{ host_data_dir }}/secrets/<service>/` (root-owned: a 0700 parent, 0755 service dirs, 0444 files), mounted read-only at `/run/secrets`. A per-service `secrets-rev` label changes when that service's secrets change, so a redeploy recreates exactly those containers. `--tags secrets_render` rewrites only the secret files.

PHASE 1 refuses a config the binaries would refuse or ignore:

- Off localnet, an escrow instance is required: set `escrow_instance_id` (created with `create_instance` in `scripts/devnet`), or keep the one a previous render left in `.env`. The all-ones placeholder is refused everywhere.
- On a localnet first boot there is no instance yet, so the stack comes up without `indexer-solana`, `operator-solana` and `operator-private-channel`. Create the instance on the validator, set `escrow_instance_id`, and redeploy with `-e reset_state=false -e validator_reset=false`.
- `escrow_program_id` / `withdraw_program_id` overrides must equal the compiled IDs. They are fixed by `declare_id!`; a custom ID needs rebuilt programs, images and clients.

Run `allow_mint` only after the deploy, once `indexer-solana` is healthy. With no backfill `start_slot` set, as in the shipped devnet config, a fresh escrow indexer starts at the chain tip, so an `AllowMint` that lands before it is never indexed and the first deposit of that mint is quarantined (`docs/runbooks/deposit_manual_review.md` Path F).

**Routine operations**

| What you want                                           | Command&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Detail                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Deploy (or roll forward / back to a specific image tag) | `ansible-playbook deploy.yml -l dev [-e image_tag=<tag>]`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Idempotent: same command for first deploy, redeploy, and rollback — only `image_tag` changes. Compose replaces only the containers whose config differs; volumes survive. In dev, `reset_state: true` (default in `vars/dev.yml`) wipes validator + indexer DB before deploy — flip to `false` to redeploy without losing state. |

**Pause / resume**

| What you want                                        | Command&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Detail                                                                                                                                                                                                                                                                             |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stop the stack, keep DB volumes                      | `ansible-playbook down.yml -l dev`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Containers exit and remain (`docker ps -a` shows them); named volumes and `host_data_dir` are untouched. Right verb for "pause overnight, resume tomorrow."                                                                                                                        |
| Bring it back up                                     | `ansible-playbook deploy.yml -l dev`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Same command as a fresh deploy. Volumes preserved by `down.yml` only survive if `reset_state: false` in `vars/dev.yml` — with the dev default (`true`), the deploy's reset-state phase wipes the validator + indexer DB anyway. Set it to `false` first if you want a true resume. |
| Wipe everything (containers, volumes, host_data_dir) | `ansible-playbook teardown.yml -l dev`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Destructive and not reversible. Removes containers, named volumes, and on-host config.                                                                                                                                                                                             |

**Diagnostics**

| What you want             | Command&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Detail                                                                                                                                              |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Re-run health probes only | `ansible-playbook deploy.yml -l dev --tags health`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Read-only: doesn't recreate containers or re-render config. Useful to re-check after a transient flap.                                              |
| Re-run sanity only        | `ansible-playbook deploy.yml -l dev --tags sanity`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Read-only. Needs Prometheus + the stack already running, so use it after a successful deploy, not before.                                           |
| Run a single phase        | `ansible-playbook deploy.yml -l dev --tags <preflight\|bootstrap\|render\|deploy\|health\|monitoring\|sanity\|report>`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Phases assume the prior ones already ran (e.g. `deploy` expects `render` to have produced `.env`). Useful for partial reruns, not first-time setup. |
| Tail the run log          | `tail -f ansible.log`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Written by [`ansible.cfg`](./ansible.cfg) → `log_path`. Captures the full output of every run, appended.                                            |

## Sanity check

Default-on after deploy (skip with `--skip-tags sanity`). **Passive** — sends no transactions, just inspects HTTP `/health` endpoints, Prometheus metrics, and recent container logs. Each check prints `==== CHECK N: <name> ====` followed by `PASS` / `FAIL` / `SKIP` lines. Seven checks:

1. **Service `/health` endpoints** — gateway, write/read nodes, indexer + operator (`/health` on `:9100` inside containers), Postgres `pg_isready`.
2. **Indexer lag** — `chain_tip_slot − current_slot` per program, asserted ≤ `INDEXER_LAG_MAX` (default 50 slots).
3. **Operator backlog depth** — `private_channel_operator_backlog_depth` per program, ≤ `OPERATOR_BACKLOG_MAX` (default 100).
4. **Yellowstone gRPC stability** — `private_channel_indexer_datasource_reconnects_total` snapshot, wait 10 s, re-snapshot; PASS if unchanged.
5. **Gateway error ratio** — `errors_total / requests_total` < `GATEWAY_ERROR_RATIO_MAX` (default 0.05). SKIPs on a fresh deploy with zero requests.
6. **Pipeline movement** — `private_channel_dedup_received_total` sampled twice over 10 s; PASS if incrementing, SKIP if zero (no traffic on a fresh deploy is expected).
7. **Reconciliation log signals** — last 10 min of `private-channel-operator-solana` logs; FAIL on `MismatchExceedsThreshold`, PASS on recent reconciliation-OK lines, SKIP if neither.

Tunable via env vars (see [`scripts/sanity.sh`](./scripts/sanity.sh) header). Sanity exits 0 on no-FAIL, 1 otherwise. SKIPs are honest and don't fail — they tell you what couldn't be verified passively.

Operator feepayer balance is **not** a sanity gate. It's monitored continuously via the `feepayer-warn-balance` (< 1 SOL, warning) and `feepayer-low-balance` (< 0.5 SOL, critical) Grafana alerts in [`monitoring/provisioning/alerting/alert-rules.yml`](../monitoring/provisioning/alerting/alert-rules.yml). A fresh deploy with an unfunded feepayer no longer fails sanity; alerts will fire if it stays unfunded.

`feepayer-burn-rate` (more than 0.05 SOL spent in an hour, warning) catches a drain before those floors, for example a zero-fee mint whose withdrawals keep failing and being reminted. To find the mint, run this against the indexer DB as its owner (the Grafana login cannot read `transactions`):

```sql
SELECT mint, status, COUNT(*) FROM transactions WHERE transaction_type = 'withdrawal' AND updated_at > NOW() - INTERVAL '1 hour' GROUP BY mint, status ORDER BY 3 DESC;
```

Then block that mint's withdrawals, or reprice it with a nonzero fee (see the [zero-fee warning](../docs/ESCROW_INTERACTION_GUIDE.md#allowmint)).

## Monitoring

Default-on (skip with `--skip-tags monitoring`). PHASE 6 brings up Prometheus + Grafana + cAdvisor + node_exporter + postgres_exporter + blackbox-exporter on the private-channel Docker network via a sibling `monitoring.compose.yml`.

- **Grafana** — bound to loopback on the target host, so reach it over an SSH tunnel: `ssh -L 3001:127.0.0.1:3001 <host>`, then `http://127.0.0.1:3001`. Login `admin` / `grafana_admin_password` from `secrets.yml`. Its datasource reads the indexer database, which is why the port is not published; set `monitoring_bind_address: ""` in `vars/<env>.yml` to publish on every interface, and only behind a reverse proxy with TLS and an allowlist. Dashboards (Health, Containers, Host, Postgres, RPC, Indexer, Operator) and datasources are provisioned read-only from [`monitoring/`](../monitoring/).
- **Prometheus** — `http://<host>:9090`. Scrape config rendered from [`monitoring/prometheus.yml.j2`](../monitoring/prometheus.yml.j2); 15d retention. Use `Status → Targets` to see what's UP.
- **Blackbox probes** — external `/health` checks (gateway / write / read / indexer / operator); query `probe_success` in Prometheus.

Dashboard JSON + alerting rules live under [`monitoring/`](../monitoring/) — edit there, redeploy to apply.

## Troubleshooting

| #   | Symptom                                                                                              | Fix                                                                                                                                                                                                                                                                                                                                           |
| --- | ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Indexer crashes with `MismatchExceedsThreshold`                                                      | Validator was reset but indexer DB wasn't. Run [`teardown.yml`](./teardown.yml) then redeploy: `reset_state: true` (default in [`vars/dev.yml`](./vars/dev.yml)) wipes both in lockstep.                                                                                                                                                      |
| 2   | `postgres-replica` unhealthy / `pg_basebackup: password authentication failed for user "replicator"` | Every deploy re-applies the superuser and replicator passwords on the primary and rewrites the replica's `primary_conninfo`, so first redeploy: `ansible-playbook deploy.yml -l dev`. Secrets are not in `.env`; compare against the files the containers read, as root: `sudo cat {{ host_data_dir }}/secrets/postgres-primary/POSTGRES_REPLICATION_PASSWORD`. If it still fails on a dev stack, `ansible-playbook teardown.yml -l dev && ansible-playbook deploy.yml -l dev` wipes the volume and the primary re-initialises with the current secret. |
| 3   | `dependency failed to start: container ... is unhealthy`                                             | Stale Postgres volume from a different password. Teardown + redeploy. A container that exits at once with `with-secrets: cannot read the file for X` or `the file for X is empty` is missing a secret: rerun `ansible-playbook deploy.yml -l <group> --tags secrets_render,deploy` and check `sudo ls {{ host_data_dir }}/secrets/<service>/`. |
| 4   | `password authentication failed for user "private_channel_runtime"` (or `..._auth_runtime`, `..._auth_owner`) | The service logins come from [`init-auth-roles.sql`](../init-auth-roles.sql), which the Postgres entrypoint runs only on an empty data directory. PHASE 4 applies it on every deploy, so this means the cluster is one Ansible does not manage (on an Ansible stack, read the expected value with `sudo cat {{ host_data_dir }}/secrets/postgres-primary/POSTGRES_RUNTIME_PASSWORD`, not from `.env`, which holds no secrets). On a raw Compose stack run `make docker-migrate` (or `make docker-devnet-migrate`), which applies both role scripts and resets Grafana's stored admin password. Otherwise apply it by hand, from the directory holding the compose files: `set -a; . ./.env; set +a; docker exec -i -e POSTGRES_REPLICATION_USER -e POSTGRES_RUNTIME_PASSWORD -e POSTGRES_AUTH_RUNTIME_PASSWORD -e POSTGRES_AUTH_OWNER_PASSWORD -e POSTGRES_GATEWAY_PASSWORD -e POSTGRES_MONITORING_PASSWORD private-channel-postgres-primary sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f -' < init-auth-roles.sql`. |

## Potential future improvements

- **Encrypt `secrets.yml` with SOPS+age** so it can live in git instead of travelling out of band.
- **Harden runtime alert delivery** (timeouts, retries, rate-limit, dead-letter) — today each alert is posted once to `ALERT_WEBHOOK_URL` (10s timeout) from a bounded queue that drops and counts overflow. Until then, route alerts via Grafana, which handles those concerns.
- **CI-built images via GHCR** instead of building on the deploy host (~3 min faster, pull-only deploys).
