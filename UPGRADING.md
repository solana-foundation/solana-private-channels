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
   docker compose exec pg-backup-primary sh -c \
     'pg_basebackup -h postgres-primary -U "$PGUSER" -D /backups/pre-upgrade_$(date +%Y%m%d_%H%M%S) -Ft -z -X stream'
   docker compose exec pg-backup-indexer sh -c \
     'pg_basebackup -h postgres-indexer -U "$PGUSER" -D /backups/pre-upgrade_$(date +%Y%m%d_%H%M%S) -Ft -z -X stream'
   ```

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
- **New env vars.** `PRIVATE_CHANNEL_PERMANENT_DELEGATE` (blank means the first key in
  `PRIVATE_CHANNEL_ADMIN_KEYS`; write and aio nodes refuse to start with neither), `OPERATOR_PERMANENT_DELEGATE` (must equal it) and the optional
  `PRIVATE_CHANNEL_COMPLIANCE_OWNER` (leave it unset, not blank, if unused). See
  [docs/CONFIG.md](docs/CONFIG.md).
- **Channel ATAs change.** Clients must derive channel ATAs with the Token-2022 program
  id, and channel ownership handoffs stop working.
- **Rollback.** Before the reset, redeploy the previous `image_tag`. After it, restore
  the pre-reset base backups as the runbook describes.

## Rehearsal

Before the first upgrade on a shared environment, rehearse on devnet: deploy N, run a
reduced smoke test, deploy N+1, roll back to N, and confirm the PITR fence and the
blockhash window checks pass on both transitions.
