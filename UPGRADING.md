# Upgrading

How to move a running stack from one release to the next, and back. Cutting a release is
in [RELEASING.md](RELEASING.md). Per-release notes are in [CHANGELOG.md](CHANGELOG.md).

## Before every upgrade

1. Read the release's upgrade notes in CHANGELOG.md. They say whether the upgrade is an
   image swap, needs `scripts/migrate-stack.sh`, needs a resync, or needs a program
   upgrade.
2. Take a labelled base backup of both clusters and keep it until the new release is
   confirmed:

   ```bash
   docker compose exec pg-backup-primary sh -c '[ -n "${PGPASSWORD_FILE:-}" ] && PGPASSWORD="$(cat "$PGPASSWORD_FILE")";
     export PGPASSWORD; pg_basebackup -h postgres-primary -U "$PGUSER" -D /backups/pre-upgrade_$(date +%Y%m%d_%H%M%S) -Ft -z -X stream'
   docker compose exec pg-backup-indexer sh -c '[ -n "${PGPASSWORD_FILE:-}" ] && PGPASSWORD="$(cat "$PGPASSWORD_FILE")";
     export PGPASSWORD; pg_basebackup -h postgres-indexer -U "$PGUSER" -D /backups/pre-upgrade_$(date +%Y%m%d_%H%M%S) -Ft -z -X stream'
   ```

   On a stack deployed with `private-channel-deploy` the sidecars get the password as a
   file (`PGPASSWORD_FILE`), not in env, so the command reads it from there first.

   The sidecar only prunes directories named `base_*`, so `pre-upgrade_*` survives
   retention. It carries its own WAL (`-X stream`) and restores to the moment it was
   taken, but it cannot be rolled forward: the sidecar prunes archived WAL older than its
   oldest `base_*`, so the WAL after this backup is gone once those rotate past it. For a
   later recovery target, use a `base_*` backup and `docs/PITR.md`.

## Kinds of upgrade

| Kind | What to do |
|---|---|
| Image swap | Redeploy with the new `image_tag`. |
| Roles or Grafana admin on existing volumes | `scripts/migrate-stack.sh` (the Ansible deploy runs the same steps on every run). |
| Resync | A change to the channel address index or the bitmap generation. The release notes name the command. |
| On-chain program upgrade | `docs/DEVNET_QUICKSTART.md` step 0, then new images and clients, because program ids are compiled in. |

## Stop order

Databases and the `pg-backup-*` sidecars stay up.

```bash
docker compose stop operator-solana operator-private-channel
docker compose stop indexer-solana indexer-private-channel
docker compose stop gateway auth read-node write-node
```

## Start order

Compose `depends_on` does not encode the withdraw operator's boot gate, and the Ansible
deploy starts everything in one `up -d --wait`, so an upgrade over existing state is
started by hand in this order:

```bash
docker compose up -d write-node
docker compose logs -f write-node                 # wait for "Redis cache aligned with Postgres"
docker compose up -d read-node gateway
docker compose --profile auth up -d auth          # only when JWT_SECRET is set (gateway enforces JWT)
docker compose up -d operator-private-channel
docker compose logs -f operator-private-channel   # wait for "Withdrawal bitmap verification passed"
docker compose up -d indexer-solana indexer-private-channel operator-solana
```

A read node returns `-32000` on `getLatestBlockhash` until a writer on the same build has
published its blockhash window, so the writer always goes first.

## Rollback

Redeploy the previous `image_tag`. That is the whole rollback when the release changed no
schema and no on-chain program. A release that changes a program says in its notes whether
the program must be rolled back first.

Schema changes are forward-only. A database restore is disaster recovery, not a rollback:
`docs/PITR.md` forbids restoring the primary alone, requires the indexer database restored
to a target at least 60 s earlier plus a replica wipe, and the withdraw operator refuses
to boot on an indexer database rolled back past a release. A release that changes schema
says so in its notes and cannot be rolled back by image swap.

## Token-2022 channel

Breaking. The channel's only token program becomes Token-2022, so this release is not an
image swap:

- **Reset required.** Existing channel mints and token accounts cannot move under the new
  release. Every channel mint is emptied, the channel account state is wiped, and each
  mint is recreated on its first deposit. Follow
  [docs/runbooks/t22_channel_reset.md](docs/runbooks/t22_channel_reset.md); its supply
  sign-off is the only gate.
- **New env vars.** `PRIVATE_CHANNEL_PERMANENT_DELEGATE` (required on write and aio
  nodes, no fallback), `OPERATOR_PERMANENT_DELEGATE` (required, must equal it) and the
  optional `PRIVATE_CHANNEL_COMPLIANCE_OWNER` (blank or unset means seize is off). See
  [docs/CONFIG.md](docs/CONFIG.md).
- **Channel ATAs change.** Clients must derive channel ATAs with the Token-2022 program
  id, and channel ownership handoffs stop working.
- **Rollback.** Before the reset, redeploy the previous `image_tag`. After it, restore
  the pre-reset base backups as the runbook describes.

## Secrets management upgrade

For stacks deployed with `private-channel-deploy`. Raw compose on `.env.local` is
unchanged: `make docker-up` still signs both roles with the one admin key and reads
secrets from env. Check that by hand after pulling this release (`make docker-up`, then a
deposit and a withdrawal).

Raw compose on devnet changes: the next `make build-devnet` creates
`keypairs/withdraw-operator.json` and writes it to `.env` as `OPERATOR_PRIVATE_KEY`, and
the withdraw operator signs releases with it. Register it before restarting the stack,
or every `ReleaseFunds` fails:

```bash
cargo run --bin add_operator -- <solana-rpc> <escrow-admin-keypair> <instance-id> \
  "$(solana-keygen pubkey keypairs/withdraw-operator.json)"
```

1. **Register the new withdraw operator key first.** The withdraw operator now signs
   `ReleaseFunds` with its own key, not the channel admin's. Create it (or set up a remote
   signer, see `private-channel-deploy/examples/signers/`), then add it with the escrow
   `Instance.admin` key before deploying:

   ```bash
   cargo run --bin add_operator -- <solana-rpc> <escrow-admin-keypair> <instance-id> <operator-pubkey>
   ```

   Preflight refuses an operator with no Operator PDA whenever an escrow instance is
   set (localnet included), and off localnet refuses an operator key equal to the
   admin key. Set `operator_keypair_path` (memory) or
   `operator_signer` and its vars in `vars/<env>.yml`. Mainnet refuses the memory
   signer for either role.
2. **Deploy an image with `with-secrets`.** Secrets now reach containers as files, and the
   app image entrypoint `/usr/local/bin/with-secrets` turns them back into env. The deploy
   checks the pulled image and refuses one without it, so `v0.1.0` and older tags cannot be
   deployed or rolled back to by this playbook.
3. **Downgrade the GHCR PAT.** `ghcr_token` must be a classic PAT with exactly
   `read:packages`; preflight refuses any other scope set and fine-grained `github_pat_`
   tokens. Create the new token, put it in `secrets.yml` or Doppler, deploy, then revoke
   the old one. Keep a `write:packages` PAT only where you push images.
4. **Expect the first deploy to re-apply Postgres passwords.** Every deploy now sets the
   primary's superuser and replicator passwords from the secrets and rewrites the
   replica's `primary_conninfo`. If the stored passwords drifted from `secrets.yml`, the
   first deploy brings them back in line rather than leaving the replica broken.
5. **The old GHCR login is removed.** Older bootstraps wrote the deploy PAT into the deploy
   user's `~/.docker/config.json` on the target. The deploy deletes that `ghcr.io` entry
   when its username equals `ghcr_user`, and keeps a personal login under any other name.
   Pulls use a throwaway docker config from then on.
6. **Move secrets out of vars files.** `notify_webhook` and `alert_webhook_url` are secrets
   now: move them from `vars/<env>.yml` to `secrets.yml` or Doppler (`ALERT_WEBHOOK_URL`,
   `GRAFANA_ALERT_WEBHOOK_URL`). Move a keyed `rpc_url`, `fallback_rpc_url` or
   `gateway_whitelist_rpc_url` too (Doppler `RPC_URL`, `FALLBACK_RPC_URL`,
   `GATEWAY_WHITELIST_RPC_URL`).
7. **Retire the admin's Operator PDA.** If the channel admin key held the Operator PDA
   before (the old single-key setup), remove it once withdrawals work with the new key:

   ```bash
   export DATABASE_URL="postgres://<postgres_indexer_user>:$(sudo cat {{ host_data_dir }}/secrets/postgres-indexer/POSTGRES_PASSWORD)@127.0.0.1:5434/<postgres_indexer_db>"
   cargo run --bin remove_operator -- <solana-rpc> <escrow-admin-keypair> <instance-id> <channel-admin-pubkey>
   unset DATABASE_URL
   ```

   It reads the indexer database and refuses while a release is in flight.

A separate operator key also lowers how many transfer-hook accounts a release fits, from
15 to 12; see the CHANGELOG note before upgrading a stack that allows hook mints.

The secret files live under `{{ host_data_dir }}/secrets/<service>/`, root-owned. Read
them with `sudo`; the rendered `.env` keeps their keys empty. Rotation:
[docs/runbooks/secret_rotation.md](docs/runbooks/secret_rotation.md) and
[docs/runbooks/key_rotation.md](docs/runbooks/key_rotation.md).

## Rehearsal

Before the first upgrade on a shared environment, rehearse on devnet: deploy N, run a
reduced smoke test, deploy N+1, roll back to N, and confirm the PITR fence and the
blockhash window checks pass on both transitions.
