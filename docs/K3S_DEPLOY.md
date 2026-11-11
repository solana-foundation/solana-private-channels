# Solana Private Channels on k3s (single host)

A second deploy target next to the compose deploy in [`private-channel-deploy/`](../private-channel-deploy/README.md). It runs the same stack on one host with k3s, from the same inventory, env vars and secrets. The compose deploy is unchanged.

**Phases** (fail-fast, idempotent): preflight, bootstrap, reset_state (localnet only), connect, cluster checks, render, secrets, database, deploy, health, monitoring, sanity, report, disconnect.

## How it fits together

| Piece | Runs on | What |
| --- | --- | --- |
| [`charts/private-channel`](../charts/private-channel/) | control node | The app: nodes, gateway, auth, indexers, operators, validator (localnet), blackbox and the Postgres exporters, NetworkPolicies. |
| [`charts/pc-db`](../charts/pc-db/) | control node | The ledger (`pc-ledger`, primary plus synchronous replica) and indexer (`pc-indexer`) CloudNativePG (CNPG) Clusters, their logins, the grants hook, backups. |
| [`private-channel-deploy/k3s.yml`](../private-channel-deploy/k3s.yml) | control node | The playbook. helm and kubectl run on the control node through an SSH tunnel with a 1h, namespace-scoped `deployer` token. |
| bootstrap ([`tasks/k3s/bootstrap.yml`](../private-channel-deploy/tasks/k3s/bootstrap.yml)) | host, as root | k3s at the pinned version, the firewall rule below, the `pc-retain` StorageClass, namespaces, deployer RBAC, an admission policy, cert-manager, CNPG, the Barman plugin and kube-prometheus-stack. |

Firewall rule (nftables table `pc_k3s_guard`, loaded before k3s by `pc-k3s-guard.service`): on the public interface it drops TCP 6443 (API), 10250 (kubelet), 9100 (node-exporter), the NodePort range 30000-32767 (TCP and UDP), and flannel's UDP 8472 (VXLAN), 51820 and 51821 (WireGuard). Pods still reach all of these. The interface is the default route's unless `k3s_public_iface` is set.

Namespaces: `private-channel` (app and databases), `pc-monitoring`, `cnpg-system`, `cert-manager`.

## Prerequisites

Versions are pinned in [`versions.env`](../versions.env): `K3S_VERSION`, `HELM_VERSION`, `KUBECTL_VERSION`, `CRANE_VERSION`, `KUBERNETES_CORE_VERSION` and the chart versions (`CNPG_CHART_VERSION`, `BARMAN_PLUGIN_VERSION`, `CERT_MANAGER_CHART_VERSION`, `KPS_CHART_VERSION`). Preflight checks each one and refuses before touching the host.

**Control node** (where you run `ansible-playbook`):

- `ansible >= 2.16`
- `helm`, `kubectl` and `crane` on `PATH`, at exactly the pinned versions
- the `kubernetes.core` collection and the Python `kubernetes` package

```bash
ansible-galaxy collection install kubernetes.core:6.3.0   # KUBERNETES_CORE_VERSION
pip install kubernetes
```

[`scripts/install-prereqs.sh`](../private-channel-deploy/scripts/install-prereqs.sh) does not install these four; install them by hand.

**Target host**:

- Passwordless `sudo` for the SSH user (bootstrap runs as root)
- At least 20 GB free under `/var/lib/rancher` (`k3s_min_free_gb`)
- On localnet, `kernel.io_uring_disabled=0` (the validator needs io_uring)
- No k3s newer than `K3S_VERSION` (k3s cannot downgrade; bump `versions.env` instead)

k3s itself is installed by bootstrap from the pinned release, checksum verified.

## Initial environment setup

Same files as compose: [`inventory.ini`](../private-channel-deploy/inventory.example.ini), `vars/<group>.yml`, and `secrets.yml` or Doppler (`export DOPPLER_TOKEN=...`). Follow [Initial environment setup](../private-channel-deploy/README.md#initial-environment-setup) there. k3s adds no inventory group.

k3s-only defaults live in [`vars/k3s.yml`](../private-channel-deploy/vars/k3s.yml). Override any key in `vars/<env>.yml` or with `-e`. The ones you are most likely to touch:

| Var | Default | What |
| --- | --- | --- |
| `k3s_token_duration` | `1h` | Lifetime of the per-run deployer token, at most `4h` (`k3s_token_duration_max_hours`). |
| `k3s_stop_after` | `""` | `database` ends the run after the database phase (migration, PITR). |
| `k3s_helm_timeout` | `15m` | helm `--timeout`. |
| `k3s_public_iface` | `""` | Interface the firewall rule guards; empty picks the default route's. |
| `k3s_ledger_instances` | `2` | Ledger primary plus replica. |
| `k3s_ledger_storage`, `k3s_indexer_storage` | `20Gi` | Database volume sizes. |
| `k3s_sync_replication` | `true` | Synchronous ledger replica. |
| `k3s_prometheus_retention` | `15d` | Prometheus retention. |
| `k3s_backup_*` | off | See [Backups](#backups). |

Secrets come from `secrets.yml` or Doppler exactly as for compose and become one `pc-<service>` Secret each, mounted at `/run/secrets`. Only key names and salted hashes reach Helm; a change rolls exactly the workloads in [`charts/private-channel/secrets-rev-table.yaml`](../charts/private-channel/secrets-rev-table.yaml).

## Operating

All commands run from `private-channel-deploy/`. The group picks the env, as for compose: `-l dev` is localnet ([`vars/dev.yml`](../private-channel-deploy/vars/dev.yml)), `-l devnet` and `-l mainnet` need their `vars/<env>.yml`.

```bash
cd private-channel-deploy
ansible-playbook k3s.yml -l devnet
```

- gateway 8899 and auth 8903 are LoadBalancer Services published on the host by k3s ServiceLB, plain HTTP as today. Nothing else has a host port.
- The escrow instance a deploy used is recorded in the `pc-deploy-state` ConfigMap, so a later run without `escrow_instance_id` keeps it.
- If a compose stack still holds 8899 and 8903, the deploy phase refuses; stop compose first (or see [migration](#compose-to-k3s-migration-dump-and-restore)).

**Routine operations**

| What you want | Command | Detail |
| --- | --- | --- |
| Deploy, redeploy, roll forward or back | `ansible-playbook k3s.yml -l <env> [-e image_tag=<tag>]` | Same command every time. Bootstrap installs or upgrades k3s and the add-ons; helm upgrades a release whenever its values or chart files change. An indexer or validator pod left not Ready by a bad release is replaced on the next run. Prefer this over a manual `helm rollback`, which leaves such a pod stuck until it is deleted. |
| Stop, keep data | `ansible-playbook down-k3s.yml -l <env>` | App workloads scale to 0 and both Clusters hibernate; data and PVCs stay. The next `k3s.yml` run resumes them at their old replica counts. |
| Wipe everything | `ansible-playbook teardown-k3s.yml -l <host> -e confirm_teardown=<host>` | Removes both releases, the add-ons, the namespaces, every volume and its data directory. `-e k3s_uninstall=true` also removes k3s and the firewall rule. Refused on a host without `e2e_host: true` unless `-e teardown_allow_non_e2e=true`. Not reversible. |
| Wipe localnet state | `ansible-playbook k3s.yml -l dev -e reset_state=true` | Wipes the validator, the indexer and ledger Clusters, Prometheus and Grafana in lockstep. Refused off localnet. `reset_state: true` is the default in `vars/dev.yml`. |

**Diagnostics**

| What you want | Command | Detail |
| --- | --- | --- |
| Run one phase | `ansible-playbook k3s.yml -l <env> --tags <phase>` | Cluster phases (render to report) always add connect and disconnect. `--tags deploy` also runs render and secrets; `--tags monitoring` also runs secrets. |
| Re-run health only | `ansible-playbook k3s.yml -l <env> --tags health` | `/health` on gateway, write-node and read-node, gated reads, only gateway and auth exposed, a NetworkPolicy probe pod, `pg_isready` in both CNPG primaries. |
| Re-run sanity only | `ansible-playbook k3s.yml -l <env> --tags sanity` | [`scripts/sanity.sh`](../private-channel-deploy/scripts/sanity.sh) with `RUNTIME=k8s`, through port-forwards. Same checks as [compose sanity](../private-channel-deploy/README.md#sanity-check). |
| Tail the run log | `tail -f ansible.log` | Same log as compose. |

### Localnet first boot

1. `ansible-playbook k3s.yml -l dev` with no `escrow_instance_id`: `indexer-solana` and the operators are not deployed.
2. Create the instance over a port-forward to the validator:
   ```bash
   eval "$(../scripts/k3s-connect.sh <inventory host>)"
   kubectl -n private-channel port-forward svc/validator 18899:8899 18900:8900 &
   # create_instance and add_operator as in docs/DEVNET_QUICKSTART.md, with --url http://127.0.0.1:18899
   ```
   See [`docs/DEVNET_QUICKSTART.md`](DEVNET_QUICKSTART.md).
3. Redeploy with the instance and without the ledger reset (the default `--reset` would wipe it):
   ```bash
   ansible-playbook k3s.yml -l dev -e escrow_instance_id=<pda> -e reset_state=false -e validator_reset=false
   ```

### Backups

Off by default. Set in `vars/<env>.yml`:

```yaml
k3s_backup_enabled: true
k3s_backup_destination_path: s3://my-bucket/pc-devnet
k3s_backup_endpoint_url: ""        # only for R2 or MinIO, e.g. https://<account>.r2.cloudflarestorage.com
k3s_backup_encryption: AES256      # AWS S3 only; leave "" on R2
```

and the secrets `backup_s3_access_key_id`, `backup_s3_secret_access_key` in `secrets.yml` (Doppler `BACKUP_S3_ACCESS_KEY_ID`, `BACKUP_S3_SECRET_ACCESS_KEY`). Preflight refuses `k3s_backup_enabled` without all three.

The Barman Cloud plugin archives WAL continuously and takes a base backup every 6h (`k3s_backup_schedule`). Retention is `30d` on mainnet, `7d` elsewhere (`k3s_backup_retention`). Restore: [PITR on k3s](PITR.md#pitr-on-k3s-cloudnativepg).

### Monitoring

kube-prometheus-stack (release `pc-kps`) in `pc-monitoring`, with our scrape jobs, dashboards and alert rules from [`monitoring/`](../monitoring/). Nothing is published on the host; reach it with a port-forward:

```bash
eval "$(../scripts/k3s-connect.sh <inventory host>)"
kubectl -n pc-monitoring port-forward svc/pc-kps-grafana 3001:80        # http://127.0.0.1:3001, admin / grafana_admin_password
kubectl -n pc-monitoring port-forward svc/pc-kps-prometheus 9090:9090
```

## Ad hoc access

[`scripts/k3s-connect.sh`](../scripts/k3s-connect.sh) opens the same tunnel and token a deploy uses (via [`k3s-connect.yml`](../private-channel-deploy/k3s-connect.yml)) and prints the `export KUBECONFIG=...` line plus the command to close it.

```bash
scripts/k3s-connect.sh <inventory host>      # prints export KUBECONFIG=... and how to close
eval "$(scripts/k3s-connect.sh <inventory host>)"
kubectl -n private-channel get pods
kubectl -n private-channel port-forward svc/postgres-primary 15432:5432   # tools that used host loopback ports
# when done, run the printed: ssh -S <socket> -O exit pc-k3s; rm -rf <kubeconfig dir>
```

Database Services: `postgres-primary` and `postgres-replica` (ledger), `postgres-indexer`.

Each session has its own token and tunnel, so several can stay open side by side. The token has the deployer's scope and expires after 1h. In `private-channel` it manages the app and the Clusters. In `pc-monitoring` it has ConfigMaps, read access to pods and Services, and port-forward; no Secrets, no exec, no pod create. No cluster-wide rights.

## Truncate

[`scripts/k3s-truncate.sh`](../scripts/k3s-truncate.sh) runs `admin truncate` as a Job: `pg_dump` 16 next to the `admin` CLI copied from the app image. DB URLs are built inside the pod from the CNPG superuser Secrets, never on argv.

```bash
eval "$(scripts/k3s-connect.sh <inventory host>)"
scripts/k3s-truncate.sh 1000000 --dry-run      # <keep-slots>; cuts nothing, dump in a throwaway emptyDir
scripts/k3s-truncate.sh 1000000                # real cut
```

The script waits until the Job succeeds or fails, however long the cut takes. `--no-withdraw-url` leaves out the withdraw indexer DB URL. A real cut keeps its `core.dump` on a `pc-retain` PVC named after the Job; copy it off-host, then delete the PVC.

## Compose to k3s migration (dump and restore)

On the same host or a new one. [`scripts/k3s-migrate-from-compose.sh`](../scripts/k3s-migrate-from-compose.sh) does the data move.

1. `ansible-playbook k3s.yml -l <env> -e k3s_stop_after=database`: k3s plus empty Clusters and roles, no app. Allowed while compose still runs; only the deploy phase needs 8899 and 8903.
2. `eval "$(scripts/k3s-connect.sh <host>)"` (add `-e k3s_token_duration=4h` for a large database).
3. `scripts/k3s-migrate-from-compose.sh <ssh target of the compose host>`: stops the compose writers (Postgres stays up), streams `pg_dump -Fc` of both databases into `pg_restore` in the CNPG primaries, fails on any `pg_restore` message, and compares exact row counts per table. `--dry-run` as the second argument only lists the compose containers; `COMPOSE_SSH_OPTS` passes extra ssh options.
4. Stop compose Postgres (`docker compose -p private-channel stop`), then `ansible-playbook k3s.yml -l <env>`.
5. Check: the fence and bitmap boot checks pass (indexer and operator logs), a user registered before the move logs in, one deposit and one withdraw land.

Downtime is steps 3 and 4. A logical restore rebuilds every index, so the Alpine (musl) to Debian (glibc) collation change cannot corrupt them. Objects keep their compose owners: `private_channel` exists as a NOLOGIN role on both Clusters.

## Client IP gate (mainnet)

auth and the gateway limit each client by its source IP. ServiceLB keeps it when the public IP sits on the host's NIC (the public Services use `externalTrafficPolicy: Local`); on a cloud VM behind NAT every client may show up as the svclb pod. Before mainnet go-live, check from two client IPs that the gateway and auth logs show each client's own address. If not, the fallback is trusting `X-Forwarded-For` or the PROXY protocol from the proxy's range (a code change in [`auth/src/throttle.rs`](../auth/src/throttle.rs) and [`gateway/src/lib.rs`](../gateway/src/lib.rs)).

## Testing

- **Chart and preflight contract tests** (no cluster): [`scripts/tests/k3s-chart.test.sh`](../scripts/tests/k3s-chart.test.sh) (helm-unittest, lint, kubeconform, the chart rendered from the real deploy vars, the secrets-rev table; `UPDATE_REV_TABLE=1` rewrites it) and [`scripts/tests/k3s-preflight.test.sh`](../scripts/tests/k3s-preflight.test.sh). Run from the repo root.
- **End to end**: [`scripts/e2e/k3s/`](../scripts/e2e/k3s/) has one numbered step per script (`01-bootstrap.sh` to `28-bench.sh`), sharing [`lib.sh`](../scripts/e2e/k3s/lib.sh). `E2E_PROFILE=local` deploys `-l dev`, `E2E_PROFILE=devnet` deploys `-l devnet`. `E2E_CTL` and `E2E_HOST` are the commands that reach the control node and the host (default: the `k3s-e2e-ctl` and `k3s-e2e-<profile>` containers), so the same steps run against real VMs. Logs and `progress.log` go to `$RUN`.

## k3s to a managed cluster later

Not built yet. Only the `infra:` block of the chart values and the connect step would change. The new cluster's CNPG Clusters would bootstrap from the same object store (`bootstrap.recovery` with `replica.enabled`, which [`charts/pc-db`](../charts/pc-db/) does not render today) under a new `serverName`, then cut over as above.
