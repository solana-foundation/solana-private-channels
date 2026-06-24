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

1. **Point at a host** ([`inventory.ini`](./inventory.ini)): IP, SSH user, key path.
2. **Choose what to deploy** ([`vars/dev.yml`](./vars/dev.yml)): image registry/tag, target Solana network, on-host paths. Each option has an inline comment explaining what to pick.
3. **Provide credentials**: copy the template, then fill in real values:

   ```bash
   cp secrets.yml.example secrets.yml
   ```

   `secrets.yml` is plain text and gitignored. See the file header for required vs optional and how to generate each value.

Everything else is wired and shouldn't need routine changes. Cross-env defaults that you can override but rarely need to (db/user names, replication user, health-probe budgets) live in [`vars/common.yml`](./vars/common.yml); set the same key in [`vars/dev.yml`](./vars/dev.yml) to override per environment.

### Using GHCR

The deploy pulls images from GHCR. Build + push them once from the control node, then every host can `docker compose pull`.

1. **Create a GitHub Personal Access Token (PAT)** with the `write:packages` [scope](https://github.com/settings/tokens/new?scopes=write:packages) (covers both push and pull)
2. **Set the Personal Access Token (PAT) in [`secrets.yml`](./secrets.yml):**
   - `ghcr_user`: GitHub username
   - `ghcr_token`: the PAT from step 1.
3. **Build and push from the control node.** Set `image_registry` and `image_tag` in [`vars/dev.yml`](./vars/dev.yml) (e.g. `image_registry: ghcr.io/<your-github-username-lowercase>`, `image_tag: v0.1.0`), then run the snippet below from inside the `private-channel-deploy/` directory.

   ```bash
   export GHCR_USER=<your-github-username>
   export GHCR_TOKEN=<your-pat-from-step-1>
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

## Operating

Cluster-agnostic by design, the commands don't change between localnet, devnet/mainnet. The deploy target is set once in [`vars/dev.yml`](./vars/dev.yml) (`network` + `rpc_url`); the playbook auto-selects the matching compose file ([`docker-compose.yml`](../docker-compose.yml) for localnet, [`docker-compose.devnet.yml`](../docker-compose.devnet.yml) for devnet/mainnet) and renders a per-env `.env` from `vars/dev.yml` + `secrets.yml`.

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

## Monitoring

Default-on (skip with `--skip-tags monitoring`). PHASE 6 brings up Prometheus + Grafana + cAdvisor + node_exporter + postgres_exporter + blackbox-exporter on the private-channel Docker network via a sibling `monitoring.compose.yml`.

- **Grafana** — `http://<host>:3001`, login `admin` / `grafana_admin_password` from [`secrets.yml`](./secrets.yml). Dashboards (Health, Containers, Host, Postgres, RPC, Indexer, Operator) and datasources are provisioned read-only from [`monitoring/`](../monitoring/).
- **Prometheus** — `http://<host>:9090`. Scrape config rendered from [`monitoring/prometheus.yml.j2`](../monitoring/prometheus.yml.j2); 15d retention. Use `Status → Targets` to see what's UP.
- **Blackbox probes** — external `/health` checks (gateway / write / read / indexer / operator); query `probe_success` in Prometheus.

Dashboard JSON + alerting rules live under [`monitoring/`](../monitoring/) — edit there, redeploy to apply.

## Troubleshooting

| #   | Symptom                                                                                              | Fix                                                                                                                                                                                                                                                                                                                                           |
| --- | ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Indexer crashes with `MismatchExceedsThreshold`                                                      | Validator was reset but indexer DB wasn't. Run [`teardown.yml`](./teardown.yml) then redeploy: `reset_state: true` (default in [`vars/dev.yml`](./vars/dev.yml)) wipes both in lockstep.                                                                                                                                                      |
| 2   | `postgres-replica` unhealthy / `pg_basebackup: password authentication failed for user "replicator"` | The primary's stored credentials drifted from `postgres_replication_password` in [`secrets.yml`](./secrets.yml) (typically a stale Postgres volume from an earlier password). Run `ansible-playbook teardown.yml -l dev && ansible-playbook deploy.yml -l dev` so the volume is wiped and the primary re-initialises with the current secret. |
| 3   | `dependency failed to start: container ... is unhealthy`                                             | Stale Postgres volume from a different password. Teardown + redeploy.                                                                                                                                                                                                                                                                         |

## Potential future improvements

- **Encrypt [`secrets.yml`](./secrets.yml) with SOPS+age** so it can live in git instead of travelling out of band.
- **Harden runtime alert delivery** (timeouts, retries, rate-limit, dead-letter) — today it's a fire-and-forget POST to `ALERT_WEBHOOK_URL`. Until then, route alerts via Grafana, which handles those concerns.
- **CI-built images via GHCR** instead of building on the deploy host (~3 min faster, pull-only deploys).
