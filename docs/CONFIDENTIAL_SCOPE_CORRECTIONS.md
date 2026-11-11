# Confidential transfers: corrections to the scope document

The scope document "Confidential transfers for the escrow" (28 October 2026) described the
plan before it was built. These are the places where the shipped design differs from it, or
where it was wrong. Where they disagree, this note and the reference docs
([ESCROW_PROGRAM.md](ESCROW_PROGRAM.md), [WITHDRAW_PROGRAM.md](WITHDRAW_PROGRAM.md),
[INDEXER.md](INDEXER.md), [confidential_transfers.md](runbooks/confidential_transfers.md))
are right.

## Withdraw program

- **Scope:** an optional trailing `confidential` byte on `WithdrawFunds`, relying on the program
  and indexer ignoring trailing bytes.
  **Shipped:** a new instruction, `ConfidentialWithdrawFunds` (discriminator 2), with the same
  accounts and data as `WithdrawFunds` and an amount cap of 2^48 - 1. Indexers roll out before
  the program: the 0.1.0 withdraw indexer parses an unknown discriminator as nothing, so it would
  silently skip a `ConfidentialWithdrawFunds` and drop the user's burn. From this release the
  indexer allow-lists 0, 1 and 2 and fails the slot on anything else.

## Keys

- **Scope:** the escrow operator needs the key only for reconciliation, the withdraw operator for
  applies and proofs.
  **Shipped:** `ESCROW_CONFIDENTIAL_KEYS` is loaded by the escrow indexer (run, resync,
  reconcile, confidential-recover) and both operators. At boot the escrow indexer, both
  operators and a resync compare it with every configured escrow account's key and refuse to
  start if any account is configured and the key is missing or matches none. A configure event under another key poisons that mint
  at index time. The secret is optional until an account is configured, and
  `check-required-env.sh` and the deploy refuse a malformed value.
- **Scope:** generated offline and backed up like `Instance.admin`.
  **Shipped:** the setup ceremony verifies every backup copy with `confidential_verify`: against
  the pinned pubkey before the services load a restored copy, and offline against the escrow
  account's data (ElGamal and AES halves) before the first mint's deposits open, then on a
  quarterly drill. See the [setup ceremony](runbooks/confidential_transfers.md#setup-ceremony).

## Indexing

- **Scope:** a deposit that fails to decrypt fails its slot and stops the indexer.
  **Shipped:** it is stored as a deposit row flagged `confidential_undecrypted`, amount 0, with
  the raw event bytes. The mint is **poisoned**: nothing is minted for that row, custody is
  held, and confidential withdrawals of the mint park `confidential_mint_poisoned`. Public
  deposits and other mints keep flowing. `indexer confidential-recover --mint` re-decrypts the
  rows from their raw bytes under the live-state lock, in one database transaction.
- **Scope:** one new column, `transactions.confidential`.
  **Shipped:** `confidential`, `confidential_undecrypted`, `confidential_raw`, `not_before`
  and `wait_started_at` for confidential rows waiting on a verified balance, and
  `confidential_park_label`, plus the tables `rebalance_requests`, `confidential_sender_state`,
  `confidential_operator_attempts` and `confidential_mint_state`.
- **Scope:** the escrow indexer records observed releases with the decrypted amount.
  **Shipped:** the observed release is written even when the mint is poisoned or the amount does
  not decrypt (amount NULL), so the remint double-pay guard always sees it.

## Reconciliation

- **Scope:** an AES copy that fails the opening check is unreadable custody (a dark tick); this
  was an open point.
  **Shipped:** Token-2022's expected and actual pending credit counters tell the two cases
  apart. When they differ, a deposit raced an apply and the copy is known stale: the gap between
  the copy and the ciphertext is the raced deposits, which reconciliation decrypts in the same
  bounded search as pending and counts exactly. Only when that gap does not decrypt is the mint
  *lagging* (held, alert only) until the withdraw sender repairs the copy. When the counters
  match, the copy is really wrong: a dark tick, three in a row halt. A pending high part beyond the
  bounded search is also lagging. Held mints are never counted as zero.
- **Scope:** pending "stays small because the operator applies it regularly".
  **Shipped:** pending low is always inside a 32-bit search; pending high uses a 32-bit search,
  then a bounded 40-bit one. The sender applies when pending reaches 2^47 base units, which keeps
  the high part inside the 32-bit search.

## Applying the pending balance

- **Scope:** the escrow's `ApplyPendingBalance` refuses an `expected_counter` that differs from
  the account's counter.
  **Wrong:** it does not refuse. Token-2022 applies every credit that arrived and only records the
  counter the operator expected, so a racing deposit leaves the AES copy stale. That is why the
  sender repairs the copy after each of its applies when the counters differ (up to five
  back-to-back repairs, then an alert and repairs with backoff).
- **Scope:** the withdraw operator applies on a timer whenever pending is not empty.
  **Shipped:** every 60 s the sender only *reads* each configured escrow account; it applies when
  the pending credit counter reaches half its cap or pending reaches 2^47, and before a release
  whose verified available balance is short. It never applies while halted, for a blocked mint,
  or while the escrow account is frozen.

## Releases

- **Scope:** the available balance comes from the checked AES copy.
  **Shipped:** the sender proves a release only from an available balance that opens against the
  on-chain ciphertext, from an account read at confirmed commitment at or past its own last
  transaction: its own running count, the AES copy, or its last apply's event. If none opens, a
  row that has never been sent waits in `pending` with a backoff, without spending retries; one
  that was sent stays `processing` in the sender and retries with backoff, like any other
  error before its next attempt goes out, and after 30 minutes parks
  `confidential_transient_failure` through the journal gate.
- **Scope:** a stale proof is "not a failure", rebuild and retry; paused mint, frozen account and
  hook refusal take the permanent-failure path to a remint.
  **Shipped:** Token-2022 26, 27, 32 and 33, and escrow 30, 31 and 33, rebuild on a budget of
  five, then park `confidential_stale_proof`; Token-2022 25 and 39, and escrow 25 to 29, park
  `destination_not_ready`; Token-2022 17 and 24 can only be about the escrow's own account,
  since the escrow checks the destination first, and park `confidential_escrow_account_not_ready`;
  running out of compute at the maximum parks `confidential_compute_exceeded` (a send below it
  is resent once at the maximum first); escrow 22 parks `confidential_mint_not_configured`. A
  custom error whose program cannot be read from the log is retried on the retry budget, then
  refunded through the journal gate like the public path. Errors are classified by origin from
  the logs, since escrow and Token-2022 codes collide. Every park after a send goes through the
  journal and finality gate, so a late-landing attempt is seen before any remint. A timeout
  resend reuses its proofs. Each release is simulated before it is journaled: a refusal there
  on a row that never broadcast is refunded at once with no signature to wait on, while a row
  with an earlier attempt still goes through the gate; a simulation that cannot answer (node
  behind, unknown blockhash) sends a never-sent row back to `Pending` with its backoff and
  holds a sent one in the sender.
- **Scope:** "transfer-hook mints: supported, for deposits and releases".
  **Shipped with a limit:** Token-2022 calls the hook with `u64::MAX`; a hook that refuses it, or
  whose extras cannot be resolved for it, parks `hook_unresolvable`, and the withdrawal settles
  publicly. A release over 4,096 bytes or 64 accounts parks `confidential_tx_too_large`.
- **Scope:** the public-balance check on "confidential-capable" mints.
  **Shipped:** on confidential-*configured* mints (`public_balance_low`), only while public plus
  verified confidential custody covers the mint's liabilities; otherwise, or when the
  confidential part is held, the row parks `escrow_drained`. The public pre-flights
  `destination_refuses_credits` and `escrow_drained` skip confidential rows.

## Rebalancing

- **Scope:** a human runs a rebalance CLI that signs `MoveToConfidential` and `MoveToPublic`.
  **Shipped:** `indexer rebalance --mint M --to-public N | --to-confidential N --id ID` inserts a
  request row under the operator's id, unique in the table: a second request with the same id is
  refused whatever the first one's state, so a rerun moves the amount once. The withdraw sender
  executes it, journaled like a release. `--to-confidential` is refused only when the public
  balance no longer covers the amount; `--to-public` is refused when the available balance differs
  from the one it was made against. No second signer process.

## Open points the scope listed, now settled

- AES mismatch: lagging or dark tick by the credit counters, as above.
- Reading the confidential fields: decoded from the raw (base64) account bytes on both the startup
  and runtime paths; jsonParsed renders ciphertexts as strings.
- Remote signers and v1: confidential releases and `MoveToPublic` rebalances are v1 messages
  signed per signer with `sign_message`, on all four keychain backends. Applies, repairs and
  `MoveToConfidential` carry no proofs and stay legacy transactions.
- Who signs rebalances: the operator, through the sender.
- Minimum deposit for confidential deposits: still not enforced.

## Tools

- The devnet tools are `confidential_keygen`, `confidential_verify`, `escrow_account_data`,
  `configure_confidential`, `confidential_deposit` and `confidential_withdraw` in
  `scripts/devnet`. `confidential_deposit` sends inline proofs in a v1 transaction and does not
  support transfer-hook mints. Rebalancing is the indexer's `rebalance` command, not a devnet
  tool.
