# Changelog

## [Unreleased]

Apex SOLA13, Cantina reaudit and OtterSec remediation on top of `main` `acdbb4bf`.

### Core node and RPC
- Refuse System at ingress and burn new System accounts (Cantina 15; #361)
- Reject the native mint (#344)
- Index `address_signatures` by signature (#346)
- Make `lastValidBlockHeight` a hard landing deadline (Cantina 27; #359)
- Spare the preloaded batch from the BOB entry cap (Cantina 64; #362)
- Cap concurrent block listings at the gateway and read node (Cantina 26; #364)
- Sigverify requires an admin signature for mint and freeze authority instructions and `SetWithdrawConfig` (Cantina 69; #367)
- Bind read deadlines and truncation to the writer's published blockhash window (Apex SOLA13-31, 65, 329, 275, 254; #386)
- Truncation prunes the address index and Redis `tx:` keys expire (Apex SOLA13-208; #395)
- Reject unused writable keys and writable program ids (Cantina 40, 41, 59, 66, 80; #388)

### Indexer and operators
- Scope `AllowMintEvent` to its own instruction (Cantina 7; #341)
- Accept pre-memo `ReleaseFunds` (Cantina 18; #347)
- Resync reads receipt mint histories (Cantina 24; #345)
- Per-mint withdraw fee and minimum withdrawal amount (Cantina 34, 11; #340)
- Refresh the mint profile after a mint is recreated (Cantina 20, 29; #348)
- Only allowed mints enter startup reconciliation (Cantina 39; #343)
- Time the Yellowstone stall watchdog on chain progress (Cantina 47; #351)
- Exit when the startup backfill fails (Cantina 43; #352)
- Bound every RpcPoller request with a 60 s deadline (Cantina 49; #354)
- Same-slot BlockMint no longer blocks earlier deposits (Cantina 21; #356)
- Webhook alerts off the status write path, terminal writes fenced to their incarnation (Cantina 36; #358)
- Count hook extras before resolving them (Cantina 17; #349)
- Resync refuses until the address index covers its newest block (Cantina 25; #360)
- Refuse the withdraw operator when the boot bitmap check cannot run (Cantina 73; #363)
- Prove channel absences from one database snapshot (Cantina 33; #365)
- Withdrawal gate reads bound to the current tip (Cantina 37; #366)
- Settled transfers after a stale snapshot no longer fake an insolvency halt (Cantina 38; #368)
- Normal indexer lag no longer escalates refunds (Cantina 48; #369)
- Claim a deposit only once the escrow checkpoint covers its slot (Cantina 30; #370)
- Decode CPI deposits without stack heights (Cantina 76; #371)
- Fee payer monitor, operator signer fallback and four more low findings (Cantina 12, 62, 65, 74, 77, 82; #382)
- Fail closed on storage read errors, bound backfill and remint work (Cantina 14, 44, 50, 75; #384)
- Await the Privy signer at startup (Cantina 13; #385)
- Load pending remints in bounded pages (Cantina 75; #389)
- Anchor startup, generation, coverage and hook reads to proven-fresh slots (Apex SOLA13-50, 292, 25, 109, 276, 127, 165; #379)
- Keep the remint journal until recovery and require a landed signature on `failed_reminted` (Apex SOLA13-35, 52, 189, 210; #380)
- Refuse a restored channel and prove channel mints before paying (Apex SOLA13-33, 68, 99, 164, 186, 212, 269; #381)
- Gate escrow value movement on a solvency fence (Apex SOLA13-12, 32, 44, 213; #387)
- Cap remint retries at 3 (Apex SOLA13-294, 373; #390)
- Watch the live-state lock through shutdown and report a silent live source as stalled (Apex SOLA13-290, 341, 374, 170, 380; #391)
- Release base-only transfer-hook mints (Apex SOLA13-82; #393)
- Resync fence and replay bounds (Apex SOLA13-28, 100, 182; #392)

### Gateway and auth
- Bound forwarded requests (Cantina 3; #353)
- Bind account-gated reads to the snapshot they were authorized on (Cantina 35; #373)
- Delegate access needs a nonzero allowance (Cantina 41; #388)
- Unlinking a wallet revokes its outstanding challenge (Apex SOLA13-39; #394)

### Programs
- Reject zero-value deposits on chain (Cantina 56; #342)

### Deploy, compose and scripts
- Env files written 0600, symlinks and foreign owners refused (Cantina 58; #350)
- Scripts no longer pass secrets in process arguments (Cantina 57; #355)
- Refuse zero, weak or unsafe operator, node and gateway settings (Cantina 45, 46, 51 to 55, 60, 61, 63, 67, 68, 71, 72, 78, 79; #375)
- Validate signer, instance, program id and metrics config at startup (Apex SOLA13-75, 17, 67, 101, 327; #376)
- `pg-backup` logs completion only after the retention check (Cantina 78; #383)
- Backup sidecars can authenticate for replication, so base backups complete
- Ansible starts the backup sidecars, and auth when the gateway enforces JWT
- Alert when the escrow solvency fence trips
- The withdraw operator sends remints through the gateway's internal listener; the read node refused them, so a failed release was never refunded
- Postgres chowns its WAL archive mount at start, so archiving works on compose boots, not only after the Ansible chown
- The replica recreates `replica_slot` on the primary when it is missing, as it is after a PITR restore
- Alert when an indexer or operator restarts: a lock-loss exit beats the scrape of its counter
- `sanity.sh` check 7 reads the solvency fence gauge; the log lines it looked for were never emitted
- The devnet withdraw tool can read through the read node (`PRIVATE_CHANNEL_READ_RPC_URL`) when the gateway enforces JWT

### Docs and CI
- Published images carry SBOM and provenance attestations
- `cargo audit` runs on dependency changes and weekly; rustls, quinn-proto and crossbeam-epoch bumped past their advisories; reqwest 0.12 drops h2 0.3
- RELEASING.md and UPGRADING.md

### Upgrade notes
- `COMMON_ESCROW_INSTANCE_ID` replaces `ESCROW_INSTANCE_ID`; blank or all-ones is refused.
- `ADMIN_SIGNER` and `OPERATOR_SIGNER` must be set in compose; a set `OPERATOR_SIGNER` that fails to load stops the operator.
- Off localnet the Ansible preflight requires `fallback_rpc_url`.
- `indexer.rpc_polling.batch_size` and `indexer.backfill.batch_size` above 100 are refused.
- Operator metrics ports 9102 and 9103 bind to `127.0.0.1` on the host.
- The withdraw indexer must use the `rpc_polling` datasource.
- Start write-node before read-node; a read node returns `-32000` on `getLatestBlockhash` until a writer publishes its window.
- Booting a writer with a larger window than the ledger recorded needs `--allow-max-blockhashes-increase`.
- `private-channel-admin truncate` needs `--withdraw-indexer-db-url` (or `--skip-withdraw-indexer-check`) and refuses a cut keeping fewer than two windows.
- `scripts/reconcile-escrow-balance.sh` is removed; use `indexer -c <escrow config> reconcile`.
- Hand edits to `failed_reminted` rows must set `landed_remint_signature`.
- Clients: unused writable keys and writable program ids are rejected with `-32602`; a `null` status past the deadline proves nothing unless `getFirstAvailableBlock` is at or below the signing `context.slot`.
- `pg-backup-primary` logs in as `POSTGRES_REPLICATION_USER`, and `postgres-indexer` runs `init-indexer-hba.sh`. Both take effect on fresh volumes only.
- Ansible always starts `pg-backup-primary` and `pg-backup-indexer`, and starts `auth-migrate` and `auth` when `gateway_enforce_jwt` is true.
- `postgres-primary` and `postgres-indexer` have a new entrypoint, so the upgrade recreates both containers (a short database restart).
- `operator-private-channel` now reaches the channel through `GATEWAY_INTERNAL_URL`, so it needs the gateway up as well as the read node.
- After a PITR restore, reset the recovery settings before starting the replica (`docs/PITR.md` Step 5).
