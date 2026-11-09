# Runbook - Confidential transfers

Covers the escrow instance's confidential keys, every park label a confidential
withdrawal can end in, and every confidential alert. Reconciliation outcomes
(mismatch, lagging, held) are in
[reconciliation_halt_runbook.md](reconciliation_halt_runbook.md#confidential-custody);
the entries below point there.

How it works, in short:

- One ElGamal key and one AES key per escrow instance, in `ESCROW_CONFIDENTIAL_KEYS`.
  `indexer-solana` and both operators load it. Every mint's escrow account in the
  instance is configured under the same key, and it can never be rotated.
- A confidential deposit becomes a normal deposit row with `confidential = true`.
  A deposit the key cannot decrypt becomes a row flagged `confidential_undecrypted`
  with its raw event bytes, and **poisons** its mint: nothing is minted for it,
  custody is held, and confidential withdrawals of that mint park.
- Custody of a configured mint is the public amount + the decrypted pending balance
  + the available balance, read from the escrow's AES copy and confirmed against the
  on-chain ciphertext. No event history is involved.
- The withdraw operator's sender is the only process that changes the escrow's
  confidential balance: releases, applies, AES repairs and rebalances all run in it,
  one at a time. After each of its transactions lands it keeps the available balance it
  verified in `confidential_mint_state`, and at boot also takes back its last landed
  apply from the apply journal, so a restart still has a value the opening check can
  confirm. `confidential-recover` and an escrow resync keep it: it is checked against
  the ciphertext before use, so a stale value is skipped, never spent.
- Every park goes through the journal and finality gate, so a parked row never
  hides a release that later lands. A park is not a refund: the user's channel
  tokens were burned when the row was created.

## Setup ceremony

Once per instance:

1. Generate the keys offline:
   `cargo run -p devnet-scripts --bin confidential_keygen -- ./escrow-confidential.json`.
   It writes a new owner-only file and prints the line
   `ESCROW_CONFIDENTIAL_PUBKEY=<base64 ElGamal pubkey>`. Record that line with the
   instance's records (it is not secret). Back the file up like `Instance.admin`:
   losing it strands every confidential balance of the instance for good.
2. Set the file's content, single-quoted, as `ESCROW_CONFIDENTIAL_KEYS` in the
   stack's `.env` (Ansible: `escrow_confidential_keys` in `secrets.yml`, Doppler:
   `ESCROW_CONFIDENTIAL_KEYS`). `scripts/check-required-env.sh` and the deploy
   refuse a value that is not JSON with `elgamal_secret` and `aes_key`.
3. Set the printed line as is: `ESCROW_CONFIDENTIAL_PUBKEY` in the same `.env`
   (Ansible: `escrow_confidential_pubkey`, Doppler: `ESCROW_CONFIDENTIAL_PUBKEY`),
   before the first configure. From then on `indexer-solana`, both operators and
   `resync` refuse to start unless `ESCROW_CONFIDENTIAL_KEYS` holds the matching keys,
   even on an empty database, where the per-mint check has no mint to read. The env
   check and the deploy refuse a pubkey that is not base64 of 32 bytes, or one set
   without the keys.
4. Restart `indexer-solana`, `operator-solana` and `operator-private-channel`.

Once per mint:

1. `block_mint` the mint's deposits, so no deposit lands while the switch flips.
2. `configure_confidential <rpc> <escrow-admin> <instance> <mint> ./escrow-confidential.json`.
   It refuses a mint whose escrow account is already configured under another key.
3. Confirm `indexer-solana` logged at INFO the line `configure_confidential` printed:
   `escrow account configured under the instance key mint=<mint> elgamal_pubkey=<pubkey>`
   (followed by the configure `signature`), and
   `private_channel_confidential_mint_poisoned{mint="<mint>"}` is 0. A foreign key
   poisons the mint at index time; stop here and [escalate](_escalation.md) (Tier 1).
4. On a mint that requires approval, have the issuer's confidential authority
   approve the escrow account.
5. On a `ConfidentialMintBurn` mint (confidential-only: the channel refuses its
   public withdrawals, including the treasury's):
   - Configure the Solana account the treasury withdraws its fees to for confidential
     transfers, approved if the mint requires it. Otherwise every fee withdrawal parks
     [`destination_not_ready`](#destination_not_ready).
   - If it has a transfer hook, check the hook accepts `u64::MAX`. Such a mint has no
     public release, so with a hook that refuses it no withdrawal can ever be paid, and
     every one ends in remint. Nothing on chain detects this.
6. `allow_mint` again to reopen deposits, with the same arguments. This also records
   whether the mint is confidential-only.

Never remove `ESCROW_CONFIDENTIAL_KEYS` or `ESCROW_CONFIDENTIAL_PUBKEY` once any escrow
account is configured: the indexer and both operators refuse to start without the key
that account holds, and without the pin a fresh database (a new host, or a resync into
an empty one) has no mint to check, so a missing key would only show as poisoned deposits.

## Park labels

A parked confidential withdrawal is a `manual_review` row whose
`confidential_park_label` column holds the label, for pre-flight parks and parks after a
send alike. A public release parked `public_balance_low` keeps its label in the same
column. The label is the park's own: a later park without one clears it, and leaving
`manual_review` by any re-arm SQL clears it together with the row's wait. The alert webhook's `error_message` starts with it too; the database has no
`error_message` column.

```sql
SELECT id, withdrawal_nonce, mint, confidential, confidential_park_label, updated_at
  FROM transactions
 WHERE status = 'manual_review'
   AND (confidential OR confidential_park_label = 'public_balance_low')
 ORDER BY updated_at;
```

Run the remint journal check from
[withdrawal_manual_review.md](withdrawal_manual_review.md#triage---dispatch-by-error_message)
first, then [`_verify_onchain_release.md`](_verify_onchain_release.md): continue
only on `NOT_LANDED` with an empty or all-dead journal.

| Label | Section |
|---|---|
| `confidential_mint_not_configured` | [Mint not configured](#confidential_mint_not_configured) |
| `confidential_mint_poisoned` | [Mint poisoned](#confidential_mint_poisoned) |
| `destination_not_ready` | [Destination not ready](#destination_not_ready) |
| `confidential_balance_low` | [Confidential balance low](#confidential_balance_low) |
| `public_balance_low` | [Public balance low](#public_balance_low) |
| `confidential_stale_proof` | [Stale proof](#confidential_stale_proof) |
| `confidential_transient_failure` | [Transient failure](#confidential_transient_failure) |
| `confidential_tx_too_large` | [Transaction too large](#confidential_tx_too_large) |
| `confidential_escrow_account_not_ready` | [Escrow account not ready](#confidential_escrow_account_not_ready) |
| `confidential_compute_exceeded` | [Compute exceeded](#confidential_compute_exceeded) |
| `hook_unresolvable` (on a confidential row) | [Hook refuses `u64::MAX`](#hook_unresolvable-on-a-confidential-row) |

Every label has three possible ends:

- **Re-arm**, once the cause is fixed. The sender rebuilds the proofs.
  ```sql
  UPDATE transactions
     SET status = 'pending', recovery_requeue_attempts = 0,
         not_before = NULL, wait_started_at = NULL, confidential_park_label = NULL,
         updated_at = NOW()
   WHERE id = :transaction_id
     AND status = 'manual_review'
     AND landed_remint_signature IS NULL;
  ```
- **Release publicly**: clear `confidential` and re-arm, so the same nonce settles
  with `ReleaseFunds` into the destination's public balance. Needs enough public
  escrow balance (rebalance to public first, see
  [Confidential balance low](#confidential_balance_low)) and a destination that
  accepts public credits. Never on a confidential-only mint (`mints.confidential_only`):
  it has no public balance and Token-2022 refuses its `MoveToPublic`, so a row parked
  `public_balance_low` would never settle. Remint instead.
  ```sql
  UPDATE transactions
     SET confidential = FALSE, status = 'pending', recovery_requeue_attempts = 0,
         not_before = NULL, wait_started_at = NULL, confidential_park_label = NULL,
         updated_at = NOW()
   WHERE id = :transaction_id
     AND status = 'manual_review'
     AND landed_remint_signature IS NULL
     AND NOT EXISTS (SELECT 1 FROM mints m
                      WHERE m.mint_address = transactions.mint AND m.confidential_only);
  ```
- **Remint**: give the user back their channel tokens and mark the row terminal, as in
  [withdrawal_failed_reminted.md](withdrawal_failed_reminted.md). Use only when the
  nonce must never settle.

A parked row is not terminal, so it holds bitmap rotation for the instance; see
[Park age](#confidential-park-age) and [Generation near full](#bitmap-generation-near-full).

### confidential_mint_not_configured

The mint's escrow account is not configured for confidential transfers (pre-flight,
or escrow error 22 on-chain). Nothing was sent, or what was sent failed.

- If the mint should take confidential transfers, run the per-mint ceremony, then
  re-arm.
- Otherwise release publicly.

### confidential_mint_poisoned

The mint has a confidential deposit the key could not decrypt, or its escrow
account is configured under a key that is not ours. Custody of the mint is held and
nothing is minted for its undecrypted deposits.

1. Find the cause:
   ```sql
   SELECT id, signature, slot, created_at
     FROM transactions
    WHERE mint = :mint AND confidential_undecrypted
    ORDER BY slot;
   ```
   Then compare the escrow account's key (`solana account <escrow-ata>`, or the
   configure transaction) with the instance key.
   - **Account under the instance key**, rows present: the key loaded when they
     were indexed was wrong or missing. Continue with step 2.
   - **Account under any other key** (a foreign-key mint): it is never recovered
     into the channel. `confidential-recover` refuses it, its confidential
     deposits stay undecrypted and are never credited, its custody stays held and
     its confidential withdrawals keep parking (release them publicly if the
     public balance covers them, or remint on a confidential-only mint). No instance key can move or verify its
     confidential balance. [Escalate](_escalation.md) (Tier 1) and stop here.
2. Load the right key (`ESCROW_CONFIDENTIAL_KEYS`, the instance key), restart the
   services.
3. Stop every indexer and operator on the database (`indexer-solana`,
   `indexer-private-channel`, `operator-solana` and `operator-private-channel`): each
   holds the live-state lock shared, and recover takes it exclusively and refuses during
   a resync. Then run `indexer confidential-recover --mint <mint>`. It first checks
   the mint's escrow account is configured under the loaded key and refuses
   otherwise; then it re-decrypts every flagged row from its raw bytes in one
   database transaction and clears the flags; it exits non-zero if any row still
   does not decrypt. Rerunning is safe.
4. Start the indexers and operators again. The recovered deposits are minted on the
   channel, the gauge drops to 0 and reconciliation counts the mint again.
5. Re-arm the parked withdrawals.

**Resync.** An escrow resync decrypts with the instance key only, so it requires
every configured escrow account to be under that key. A poisoned mint is rebuilt
poisoned: its undecrypted deposits come back flagged with amount 0 and are never
minted, and since no channel mint pays them the consumed-set check accepts them.
The mint stays held after the resync exactly as before it. See
[resync_consumed_mint_mismatch.md](resync_consumed_mint_mismatch.md#confidential-mints).

### destination_not_ready

The user's Token-2022 account on Solana cannot take a confidential release: it is
missing, not configured for confidential transfers, not approved, has confidential
credits off, its pending credit counter is full, or it is frozen (pre-flight, escrow
25 to 29, or Token-2022 25 or 39 on-chain). The escrow checks these states itself before
it calls Token-2022, so a Token-2022 17 or 24 is about the escrow's own account; see
[Escrow account not ready](#confidential_escrow_account_not_ready).

- The user fixes their account (create and configure it, apply their pending
  balance, ask the issuer to approve or thaw it), then re-arm. `confidential_withdraw`
  with `SOLANA_RPC_URL` set checks the same conditions.
- If they cannot, release publicly to their public balance, or remint (the only end on a
  confidential-only mint).

### confidential_balance_low

The escrow's verified available balance is below the amount even after applying
pending. Public funds of the mint may cover it.

1. Move public funds into the confidential balance with a rebalance request:
   `indexer rebalance --mint <mint> --to-confidential <amount> --id <id>`. Pick a new id
   for each move you mean, and keep it when you rerun the same move: a second request
   with an id already used is refused, whatever happened to the first, so a rerun never
   moves the amount twice. The sender executes it, journaled like a release, and refuses
   it only if the public balance no longer covers the amount. A `--to-public` request
   also records the available balance it was made against (`expected_available`) and is
   refused if that changed before it ran, for example after a release or an apply on a
   busy mint; check the balance and request again with a new id. A request left `sent`
   becomes `completed` once its transaction finalizes, and `failed` only after its
   blockhash expired and the signature is absent from history; until then it stays `sent`.
2. Re-arm the row. Or release publicly if the public balance covers it.

On a confidential-only mint the public balance is always 0, so neither step works: all
custody is confidential, and this label means a shortfall or a pending balance not yet
applied. Check that the last apply landed and that
`private_channel_reconciliation_confidential_mismatch{mint}` is 0, then re-arm. Otherwise
remint and [escalate](_escalation.md).

### public_balance_low

A public release on a confidential-configured mint has more than the escrow's
public balance to pay, and nothing proves the custody short of what the ledger owes. Either
the whole custody (public plus the verified confidential balance) covers the ledger, so the
funds sit in the confidential balance, or the confidential part could not be counted: the
mint is poisoned, its custody does not open (`confidential custody is not proven`), or a wait
below ran out. Only a custody proven short of the ledger parks `escrow_drained` instead
(`insufficient escrow balance:`), see
[withdrawal_manual_review.md](withdrawal_manual_review.md#triage---dispatch-by-error_message).
A foreign-key mint is judged on its public balance alone.
The row is not judged while another release of the mint was sent but is not yet recorded
(`processing` or `pending_remint` with a journaled signature and no observed release), or
while the confidential balance lags in a way the sender clears (`stale_aes`,
`pending_too_large`): it goes back to `Pending` with the wait backoff. A row with its own
journal (re-armed by recovery) takes that wait only once every journaled signature is past
its last valid block height at confirmed commitment and none landed; until then it stays
`processing`. After 30 minutes from the first wait the row parks `public_balance_low`
(`... after 30 min; rule out a drain`). Waiting rows show on
`private_channel_operator_public_withdrawals_waiting`, and the
`public-withdrawal-wait-age` alert fires when the oldest has waited over 15 minutes.

1. Rule out a drain first: the mint's permanent delegate (if any) can move public funds
   but not confidential ones. Check `private_channel_reconciliation_confidential_mismatch{mint}`
   and the reconciliation alerts for the mint; if either fires, or the public balance fell
   without a release or rebalance to explain it, do not rebalance or re-arm, and
   [escalate](_escalation.md).
2. `indexer rebalance --mint <mint> --to-public <amount> --id <id>`.
3. Re-arm the row.

On a confidential-only mint (`mints.confidential_only`) the `--to-public` move fails on
chain (`IllegalMintBurnConversion`), so this row can never be paid: remint it. Such a row
only exists if the mint's config lacked the flag when it was burned (a mint allowed before
the flag, or a `mints` backfill without it). Run `allow_mint` again to set it. The channel's
withdraw config only picks it up from the next deposit, so public burns still go through
until one lands.

### confidential_stale_proof

The escrow (30, 31 or 33) or Token-2022 (26, 27, 32 or 33) refused the release's
proofs on every rebuild of the stale-proof budget. Something other than the sender
changed the escrow's available balance, or the RPC keeps serving an old account.

1. Check `operator-private-channel` logs for the mint around the row's
   `updated_at`, and that only one withdraw operator runs against the database.
2. Check `private_channel_reconciliation_confidential_mismatch{mint}`. If it is 1,
   follow the reconciliation runbook first; do not re-arm.
3. Otherwise re-arm. The sender reads the account again and rebuilds.

### confidential_transient_failure

A release with an earlier journaled attempt could not send its next attempt for 30
minutes: every try failed before broadcast (the escrow account or mint could not be read,
no candidate available balance opened, a blockhash fetch or signing failed, the
simulation could not answer, or the RPC kept answering "minimum context slot not
reached"). The sender held the row `processing`, refreshing its lease so recovery left it
alone, and retried with backoff the whole time, spending no proof rebuild or retry; the journal gate then proved no attempt landed. Nothing
refused the release on chain.

1. Check `operator-private-channel` logs for the nonce: `Confidential release held in the
   sender` gives each failure.
2. Fix the cause: usually the Solana RPC (down, rate limited, or a load balancer with a
   node far behind), or the signer. If no candidate opened, see
   [Confidential release waiting](#confidential-release-waiting).
3. Re-arm.

### confidential_tx_too_large

The release does not fit one v1 transaction (4,096 bytes or 64 accounts), usually
a fee mint with many hook accounts. It will never fit. Release publicly, or remint on a
confidential-only mint.

### confidential_escrow_account_not_ready

Token-2022 refused the release with 17 (account frozen) or 24 (not approved). The
escrow refuses a frozen or unapproved destination itself before calling Token-2022, so
these can only be about the escrow's own token account for the mint. Nothing moved.

1. Check the escrow account: `solana account <escrow-ata>` (state and the confidential
   extension's `approved`). A freeze or an unapproved account is the issuer's action.
2. Ask the issuer to thaw it, or to approve it with the mint's confidential authority.
   Background applies and repairs on a frozen escrow account are held until it is
   thawed (the operator logs `Escrow account is frozen` once).
3. Re-arm. If the issuer will not, [escalate](_escalation.md) (Tier 1): no withdrawal of
   the mint, public or confidential, can be paid from a frozen escrow account.

### confidential_compute_exceeded

The release ran out of compute at 1,400,000 compute units, the transaction maximum, in
the escrow or in the mint's transfer hook. The sender simulates every release at the
maximum and sends it with what it used plus a fifth; a send that then runs out of compute
on chain is resent once at the maximum. It parks only when the simulation or a send at
the maximum runs out, so the release does not fit one transaction at all, usually
because the issuer's hook is too expensive. A resend cannot pass it.

- Release publicly: rebalance to public if needed, then clear `confidential` and
  re-arm. A public release runs the hook with the real amount and may cost less.
  On a confidential-only mint, remint.
- If the hook was changed since, re-arm once to try again.

### hook_unresolvable (on a confidential row)

Token-2022 calls the hook with `u64::MAX` on confidential transfers, and this
mint's hook refuses it or its extra accounts cannot be resolved for it. A hook that
runs out of compute parks [`confidential_compute_exceeded`](#confidential_compute_exceeded)
instead.
Release publicly: rebalance to public if needed, then clear `confidential` and
re-arm. On a confidential-only mint no withdrawal can be paid while the hook refuses
`u64::MAX`: remint, and [escalate](_escalation.md) to the issuer.

## Alerts

### Confidential mint poisoned

Rule `confidential-mint-poisoned`: `private_channel_confidential_mint_poisoned{mint} == 1`.
See [confidential_mint_poisoned](#confidential_mint_poisoned). Two rules report the same
cause from other angles: `confidential-decrypt-failures`
(`private_channel_confidential_decrypt_failures_total{kind}` rose in the last 10 minutes,
per event) and `confidential-undecrypted-deposits-held`
(`private_channel_operator_undecrypted_deposits_held`, the escrow operator's count of
pending deposits it cannot mint). Held deposits are not counted as backlog, so they do
not turn the operator's `/health` red.

### Reconciliation confidential mismatch

The AES copy does not open against the available ciphertext and the credit counters
match, so the copy is really wrong. Each tick is an inputs-dark tick; three in a row
halt. See
[reconciliation_halt_runbook.md](reconciliation_halt_runbook.md#confidential-custody).

### Reconciliation confidential lagging

The mint is held, alert only. `reason` says why: `stale_aes` (a deposit raced an
apply and the copy's gap to the ciphertext is past the bounded search; a raced copy
whose gap decrypts is counted exactly and never held; the sender repairs the copy) or `pending_too_large` (the
pending high part is beyond the bounded search; the sender's next apply fixes it).
Fires after 5 minutes; a stale copy that lasts that long means the repair is
failing, see [AES repair deferred](#aes-repair-deferred).

### Reconciliation confidential held

The mint is held with `reason` `poisoned` (see
[confidential_mint_poisoned](#confidential_mint_poisoned)) or `key_mismatch` (the
escrow account is under a key that is not `ESCROW_CONFIDENTIAL_KEYS`). Fires at once;
nothing clears it automatically. A `key_mismatch` mint is still compared on its public
balance, which must cover its liabilities, and halts like any mint when it does not.

### Confidential hold age

A mint has been held for any reason for over an hour. Its custody is not checked
while held, except under `key_mismatch`, where no confidential deposit is ever credited
and the mint is still reconciled on its public balance alone. Find the reason on the
lagging gauge and follow its entry.

### AES repair deferred

`private_channel_operator_confidential_aes_repair_deferred{mint}`: five repairs in a row raced a deposit.
The sender keeps retrying one repair per background check with backoff, and any
release, `MoveToPublic` or apply also fixes the copy. Sustained deposits on the
mint cause it; it clears when they pause. If it persists for an hour, check that
`operator-private-channel` is running and its signer has SOL.

### Pending not verified

`private_channel_operator_confidential_pending_unverified{mint}`: the escrow's pending
balance is too large to decrypt (past the 40-bit search), so the sender takes the sum of
the indexed confidential deposits and moves to confidential since the mint's last apply,
and applies only if the pending ciphertext opens to exactly that sum. It does not, so
nothing is applied. Check that `indexer-solana` is running and caught up; the gauge
clears on the next background check once it is. If the indexer is current, look for an
undecrypted deposit on the mint ([confidential_mint_poisoned](#confidential_mint_poisoned)).

### Apply undecided

Every apply and AES repair is journaled in `confidential_operator_attempts` before it is
sent. While one is `sent` (its outcome unknown) no release, apply, repair or rebalance
goes out on that mint; releases wait in `pending`. It settles on its own: `landed` once
found finalized, `dead` once its blockhash expired and history shows it absent (about
two minutes). The sender settles them at boot too. One stuck `sent` for long means the
RPC cannot answer status or history; check it.

### Pending credit counter high

The escrow's pending credit counter is near its cap of 65,536. The sender applies at
half the cap, so a high value means it is not applying: check that
`operator-private-channel` is up and not halted, that the mint is not blocked, and that
the escrow account is not frozen (Token-2022 refuses an apply on a frozen account, so
the sender holds them until it is thawed).
At the cap, new confidential deposits fail for users (nothing is lost).

### Confidential park age

A confidential park is older than an hour. Parks hold bitmap rotation; resolve it
through its label above.

### Bitmap generation near full

The bitmap generation is close to full while an unresolved row holds rotation. Every
withdrawal past the boundary waits. Resolve the oldest `manual_review` row first; see
[An unresolved row holds the bitmap rotation](withdrawal_manual_review.md#an-unresolved-row-holds-the-bitmap-rotation).

### Confidential release waiting

A confidential row has waited (`wait_started_at` set) for a verified available
balance for a long time. It is back in `pending` with a growing `not_before`, so
other rows keep flowing. Usually a stale AES copy the repair has not fixed yet; see
[AES repair deferred](#aes-repair-deferred). If the copy is fine, check the RPC the
operator reads from is not behind.

### Dust deposits

Not an alert of its own. Confidential deposits have no minimum, and the escrow indexer
decrypts each one with two or three 32-bit discrete-log searches (about 20 a second); a
slot checkpoints only once all of its deposits are decrypted. 1-unit deposits sent
faster than that delay every deposit behind them, public ones included, and the indexer
lag alert (`indexer-lag-10s`) fires. No funds are at risk and the backlog drains once
the flood stops. Do not resync to clear it: a resync decrypts the whole history twice
while holding the exclusive live-state lock, so every service stays down longer.
