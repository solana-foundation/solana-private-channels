# Deploying on k3s

A second deploy target next to the compose deploy in `private-channel-deploy/`. It runs the same stack on one host with k3s, from the same inventory, env vars and secrets. The compose deploy is unchanged.

## How it fits together

| Piece | Where | What |
|---|---|---|
| `charts/private-channel` | control node | The app: nodes, gateway, auth, indexers, operators, validator (localnet), blackbox and the Postgres exporters, NetworkPolicies. |
| `charts/pc-db` | control node | The ledger (`pc-ledger`, primary plus synchronous replica) and indexer (`pc-indexer`) CloudNativePG Clusters, their logins, the grants hook, backups. |
| `private-channel-deploy/k3s.yml` | control node | The playbook. helm and kubectl run on the control node through an SSH tunnel with a 1h, namespace-scoped `deployer` token. |
| bootstrap | host, as root | k3s at the pinned version, a firewall rule that drops 6443 and 10250 on the public interface, the `pc-retain` StorageClass, namespaces, deployer RBAC, an admission policy, cert-manager, CNPG, the Barman plugin and kube-prometheus-stack. |

Versions are pinned in `versions.env` (`K3S_VERSION`, `HELM_VERSION`, `KUBECTL_VERSION`, `CRANE_VERSION`, the chart versions). The control node needs helm, kubectl and crane at those versions, the `kubernetes.core` collection and the Python `kubernetes` package; preflight checks each.

## Deploy

```bash
cd private-channel-deploy
ansible-playbook k3s.yml -l devnet
```

Phases: preflight, bootstrap, reset_state (localnet only), connect, cluster checks, render, secrets, database, deploy, health, monitoring, sanity, report, disconnect. Run one with `--tags <phase>`; connect and disconnect always come with it.

- Secrets come from `secrets.yml` or Doppler exactly as for compose and become one `pc-<service>` Secret each, mounted at `/run/secrets`. Only key names and salted hashes reach Helm; a change rolls exactly the workloads in `charts/private-channel/secrets-rev-table.yaml`.
- gateway 8899 and auth 8903 are LoadBalancer Services published on the host by k3s ServiceLB, plain HTTP as today. Nothing else has a host port.
- Backups: set `k3s_backup_enabled: true`, `k3s_backup_destination_path: s3://bucket/path` (plus `k3s_backup_endpoint_url` for R2 or MinIO, `k3s_backup_encryption: AES256` on AWS only) and the secrets `backup_s3_access_key_id`, `backup_s3_secret_access_key` (Doppler `BACKUP_S3_ACCESS_KEY_ID`, `BACKUP_S3_SECRET_ACCESS_KEY`).

### Client IP gate (mainnet)

auth and the gateway limit each client by its source IP. ServiceLB keeps it when the public IP sits on the host's NIC; on a cloud VM behind NAT every client may show up as the svclb pod. Before mainnet go-live, check from two client IPs that the gateway and auth logs show each client's own address. If not, the fallback is trusting `X-Forwarded-For` or the PROXY protocol from the proxy's range (a code change in `auth/src/throttle.rs` and `gateway/src/lib.rs`).

## Ad hoc access

```bash
scripts/k3s-connect.sh <inventory host>      # prints export KUBECONFIG=... and how to close
kubectl -n private-channel get pods
kubectl -n private-channel port-forward svc/postgres-primary 15432:5432   # tools that used host loopback ports
```

The token has the deployer's scope: no cluster-wide rights, no Secrets or pods in `pc-monitoring`. It expires after 1h.

## Localnet first boot

1. `ansible-playbook k3s.yml -l dev` with no `escrow_instance_id`: indexer-solana and the operators are not deployed.
2. Create the instance over a port-forward to the validator:
   ```bash
   kubectl -n private-channel port-forward svc/validator 18899:8899 18900:8900 &
   # create_instance and add_operator as in docs/DEVNET_QUICKSTART.md, with --url http://127.0.0.1:18899
   ```
3. Redeploy with the instance and without the ledger reset (the default `--reset` would wipe it):
   `ansible-playbook k3s.yml -l dev -e escrow_instance_id=<pda> -e reset_state=false -e validator_reset=false`

`reset_state=true` wipes the validator, the indexer and ledger Clusters, Prometheus and Grafana in lockstep. It is refused off localnet.

## Stop and remove

- `ansible-playbook down-k3s.yml -l <env>`: app workloads scale to 0 and both Clusters hibernate; data and PVCs stay. The next `k3s.yml` resumes.
- `ansible-playbook teardown-k3s.yml -l <host> -e confirm_teardown=<host>`: removes both releases, the add-ons, the namespaces, every volume and its data directory (`-e k3s_uninstall=true` also removes k3s). Refused on a host without `e2e_host: true` unless `-e teardown_allow_non_e2e=true`.

## Compose to k3s migration (dump and restore)

On the same host or a new one:

1. `ansible-playbook k3s.yml -l <env> -e k3s_stop_after=database`: k3s plus empty Clusters and roles, no app. Allowed while compose still runs; only the deploy phase needs 8899 and 8903.
2. `eval "$(scripts/k3s-connect.sh <host>)"`
3. `scripts/k3s-migrate-from-compose.sh <ssh target of the compose host>`: stops the compose writers (Postgres stays up), streams `pg_dump -Fc` of both databases into `pg_restore` in the CNPG primaries, fails on any `pg_restore` message, and compares exact row counts per table.
4. Stop compose Postgres (`docker compose -p private-channel stop`), then `ansible-playbook k3s.yml -l <env>`.
5. Check: the fence and bitmap boot checks pass (indexer and operator logs), a user registered before the move logs in, one deposit and one withdraw land.

Downtime is steps 3 and 4. A logical restore rebuilds every index, so the Alpine (musl) to Debian (glibc) collation change cannot corrupt them. Objects keep their compose owners: `private_channel` exists as a NOLOGIN role on both Clusters.

## k3s to a managed cluster later

Only the `infra:` block of the chart values and the connect step change. The new cluster's CNPG Clusters bootstrap from the same object store (`bootstrap.recovery` with `replica.enabled`) under a new `serverName`, then cut over as above.
