# Runbook - Corrupt Account Row

This runbook covers a **corrupt row in the node's `accounts` table**: a row whose
stored bytes will not deserialize into an account. The executor refuses to run a
batch that references such an account, so the node stops rather than settling
state derived from an account it could not read.

This is a **node** condition, not an operator one. It marks no transaction row
`failed` or `manual_review`, so it is **not routed by the webhook dispatch table**
in [`README.md`](README.md). It follows the same non-paged-halt shape as
[`indexer_block_unavailable.md`](indexer_block_unavailable.md): recognize it from
the metric plus the log marker, not from a webhook payload.

---

## Why the node stops instead of continuing

Before executing a batch, the node preloads every account the batch references.
A read that fails is not the same as an account that does not exist, and the
difference matters: if a failed read were treated as absence, the SVM would run
against an account it believes is empty, and the settler would write that result
back over the real row. The account balance stored there would be gone.

Nothing downstream can catch this on the node's behalf. The lamport-conservation
check compares post-execution balances against the same in-memory cache the
failed preload left empty, so it sees a real funded account as one this
transaction just created and lets it through.

So a read failure aborts the batch instead. The abort happens before any
execution and before any write, so nothing in the batch was settled and its
transactions remain resubmittable after the restart.

Two failure shapes reach this point, and they need different responses:

- **Backend**: the query itself failed. A transient failure is retried, under a
  total time budget of a few seconds so a stalled connection cannot hold the
  executor indefinitely, and an error the database itself returned (a missing
  table, a rejected permission) is not retried at all because it will say the
  same thing every time. What reaches here is a database that is genuinely
  unreachable or misconfigured; for the unreachable case the node recovers on its
  own once the database returns.
- **Corrupt**: a row came back and will not deserialize. Retrying cannot change
  the bytes, so this is fatal on the first read and **will not clear on its own**.
  That is the condition this runbook is for.

## Symptom

- The node exits non-zero shortly after start and is restarted by its supervisor,
  producing a crash loop.
- The executor logs, once per attempt, naming the account:

  ```
  execution: aborting batch, account preload failed: stored account <PUBKEY> could not be deserialized
  Executor stopping: stored account <PUBKEY> could not be deserialized
  ```

- Block production stops. `getSlot` stops advancing, and submitted transactions
  stop confirming.
- RPC reads of that same account return a JSON-RPC server error rather than a
  null account.

## Detection

```
increase(private_channel_executor_corrupt_account_total[5m]) > 0
```

This counter increments only for a row that will not deserialize, so any non-zero
rate is this condition. Its sibling counts both shapes together:

```
increase(private_channel_executor_preload_fatal_total[5m]) > 0
```

If `preload_fatal` is climbing while `corrupt_account` stays flat, the node is
failing on a **Backend** error, not corruption. That is a database availability
problem: check that Postgres is reachable and healthy, and expect the node to
recover by itself once it is. Do not run any step below.

Take the pubkey from the log line. Every step that follows needs it.

## Confirm before touching anything

Read the row and confirm it is genuinely undeserializable rather than merely
unexpected. A well-formed account row is a bincode-encoded account, so a healthy
row is far larger than a handful of bytes and its length is consistent with its
data field.

```sql
SELECT length(data) AS byte_len, encode(substring(data from 1 for 32), 'hex') AS head
FROM accounts
WHERE pubkey = decode('<PUBKEY_HEX>', 'hex');
```

Compare against a known-good row for scale:

```sql
SELECT length(data) FROM accounts ORDER BY length(data) DESC LIMIT 5;
```

Then check whether the account is referenced by settled history, which decides
how much is at stake:

```sql
SELECT count(*) FROM address_signatures
WHERE address = decode('<PUBKEY_HEX>', 'hex');
```

A row with signature history had a real balance. A row with none may be a write
that was interrupted before it ever mattered.

## Recovery

As with every runbook here, the SQL below is **bookkeeping, not fund movement**.
See [`README.md`](README.md) § "Recovery SQL is bookkeeping; fund restoration is
human-in-the-loop".

### Preferred: restore the row from PITR

The row's correct contents are recoverable if the corruption is recent. Follow
[`../PITR.md`](../PITR.md) to stand up a restored copy at a timestamp before the
corruption, read the single row out of it, and write it back to the live database.
This is the only option that preserves the account's balance.

Confirm the restored bytes deserialize by reading the account through the node's
`getAccountInfo` **after** the write and before declaring the incident closed.

### Last resort: delete the row

Deleting the row makes the account read as absent. Do this **only** when PITR
cannot reach a clean copy and the confirmation step showed no signature history,
because for an account that held a balance this destroys it.

```sql
DELETE FROM accounts WHERE pubkey = decode('<PUBKEY_HEX>', 'hex');
```

This needs the same human sign-off as any other fund-affecting action. Record the
pubkey, the byte length observed, and the signature count in the incident notes.

### Restart and verify

The node has been crash-looping, so it picks the repair up on its next start with
no further action. Verify:

1. `private_channel_executor_corrupt_account_total` stops increasing.
2. `getSlot` advances again.
3. `getAccountInfo` on the pubkey returns either the restored account or a null
   account, and **not** a server error.

## If more than one account is corrupt

The executor reports the first corrupt account it meets in a batch, so a second
one surfaces only after the first is repaired. Multiple corrupt rows point at
storage-level damage rather than a single bad write. Stop repairing them one at a
time and treat it as a database integrity incident: check the Postgres logs for
I/O errors, and prefer a full PITR restore over row-by-row repair.
