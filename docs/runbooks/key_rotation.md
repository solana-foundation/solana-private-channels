# Runbook - Key Rotation

**Status: untested guidance.** The sequences below are derived from the code. Rehearse
each on devnet before following it on a shared environment.

How to rotate the three signing keys of a stack deployed with `private-channel-deploy`.
Passwords, tokens and other deploy secrets are in
[`secret_rotation.md`](secret_rotation.md).

| Key | What it is | Where it lives |
|---|---|---|
| Withdraw operator | Signs `ReleaseFunds` and `RotateBitmap`; needs an Operator PDA on the escrow instance | `operator_signer` in the deploy, used only by `operator-private-channel` |
| Channel admin | Fee payer for releases, mint and freeze authority of every channel receipt mint, withdraw fee treasury, remint signer | `admin_signer` in the deploy, used by both operators; its pubkey is `PRIVATE_CHANNEL_ADMIN_KEYS` |
| `Instance.admin` | Escrow instance admin: `AllowMint`, `BlockMint`, `AddOperator`, `RemoveOperator`, whitelist, `SetNewAdmin` | Offline; never on the host |

The commands below run from `scripts/devnet/` (`cargo run --bin <name> -- ...`):

- `<solana-rpc>`: the Solana RPC of the escrow (devnet or mainnet).
- `<escrow-admin-keypair>`: the `Instance.admin` keypair file.
- `<channel-gateway-rpc>`: the gateway's public port on the host
  (`http://127.0.0.1:8899` by default), which takes transactions without a JWT.
- `<channel-read-rpc>`: the read node on the host (`http://127.0.0.1:8901`), which serves
  account reads without a JWT. The commands set `PRIVATE_CHANNEL_READ_RPC_URL` to it.
- Run them on the host or through an SSH tunnel to those loopback ports.

## Withdraw operator rotation

The old and new operator keys are both valid on chain between steps 1 and 5, so no
release is refused during the swap.

1. **Add the new operator** with `Instance.admin`:

   ```bash
   cargo run --bin add_operator -- <solana-rpc> <escrow-admin-keypair> <instance-id> <new-operator-pubkey>
   ```

2. **Stop the withdraw operator** and let in-flight releases settle. Wait two minutes so
   the last release's blockhash expires:

   ```bash
   docker stop private-channel-operator-private-channel
   ```

   You do not have to judge "settled" by eye: `remove_operator` in step 5 reads the
   indexer database and refuses while any withdrawal is not `completed`, `failed` or
   `failed_reminted`, printing the count and the oldest row. Withdrawals left pending
   here are released by the new operator after step 4.

3. **Swap the operator signer** in `vars/<env>.yml` (or the `-e @` signer file):
   - memory: point `operator_keypair_path` at the new keypair, or replace Doppler
     `OPERATOR_KEYPAIR` (not on mainnet);
   - remote: set `operator_pubkey` and the backend vars of the new key, and its secret in
     `secrets.yml` or Doppler if it changed.

   The new key must differ from the channel admin's.
4. **Deploy.** Preflight checks the new key's Operator PDA and refuses
   to continue if step 1 did not land. The deploy starts the operator with the new key.

   ```bash
   cd private-channel-deploy && ansible-playbook deploy.yml -l <group>
   ```

   Confirm a withdrawal completes and its `ReleaseFunds` is signed by the new pubkey.
5. **Remove the old operator** with `Instance.admin`, once every withdrawal has settled.
   The database password goes in `DATABASE_URL`, not on the command line:

   ```bash
   export DATABASE_URL="postgres://<postgres_indexer_user>:$(sudo cat {{ host_data_dir }}/secrets/postgres-indexer/POSTGRES_PASSWORD)@127.0.0.1:5434/<postgres_indexer_db>"
   cargo run --bin remove_operator -- <solana-rpc> <escrow-admin-keypair> <instance-id> <old-operator-pubkey>
   unset DATABASE_URL
   ```

   It refuses while a withdrawal is unsettled; wait and rerun. A row stuck in
   `manual_review` also blocks it until resolved (see the withdrawal runbooks). The
   closed PDA's rent goes to the `Instance.admin`. A release signed by the old key now
   fails on chain.

If the old key is compromised, run step 2 and then step 5 at once, before the rest,
with `--force` on `remove_operator` (it then only warns about unsettled withdrawals):
the removal makes every release signed with the old key fail on chain, and with the
operator stopped, withdrawals wait as pending until step 4.

## Channel admin rotation

The channel admin is the mint and freeze authority of every receipt mint, so its rotation
moves those authorities on the channel, inside an overlap where the node accepts both
keys.

1. **Sweep withdraw fees from the old admin.** The old admin is the withdraw fee treasury,
   and its channel ATA per mint holds the collected fees. The treasury withdraws with no
   fee and no minimum, but that exemption follows the configured treasury, so sweep now.
   For each mint with a balance, find the old admin's channel ATA and balance, then
   withdraw all of it as the old admin:

   ```bash
   ata=$(solana find-program-derived-address ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL \
     pubkey:<old-admin-pubkey> pubkey:TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb pubkey:<mint>)
   curl -s <channel-read-rpc> -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":1,"method":"getTokenAccountBalance","params":["'"$ata"'"]}'
   PRIVATE_CHANNEL_READ_RPC_URL=<channel-read-rpc> cargo run --bin withdraw -- \
     <channel-gateway-rpc> <old-admin-keypair> <mint> <amount> [<solana-destination>]
   ```

   `withdraw` prints `Withdraw fee: 0` when it runs as the treasury. The operator releases
   the amount on Solana to the destination (default: the old admin's own address). The
   destination must already hold a token account for the mint on Solana (create it with
   `spl-token create-account --owner <destination> <mint>`); otherwise the release fails
   on chain and the operator remints the amount to the old admin's channel ATA
   (`failed_reminted`), so check and sweep again.

   For a remote old admin, pass `env:OLD_ADMIN` instead of the keypair path and set
   `OLD_ADMIN_SIGNER` plus that backend's vars, named like the operators' `ADMIN_*` ones
   with `OLD_ADMIN_` in front: `_VAULT_ADDR`, `_VAULT_TOKEN`, `_VAULT_KEY_NAME`,
   `_VAULT_PUBKEY` (optional `_VAULT_CA_CERT`, a PEM file for a private CA);
   `_TURNKEY_API_PUBLIC_KEY`, `_TURNKEY_API_PRIVATE_KEY`, `_TURNKEY_ORGANIZATION_ID`,
   `_TURNKEY_PRIVATE_KEY_ID`, `_TURNKEY_PUBKEY`; `_PRIVY_APP_ID`, `_PRIVY_APP_SECRET`,
   `_PRIVY_WALLET_ID`; or `_PRIVATE_KEY` for memory. Export the secret ones from a file
   or your shell's history-free input, never on the command line:

   ```bash
   export OLD_ADMIN_SIGNER=vault OLD_ADMIN_VAULT_ADDR=https://vault.example:8200 \
     OLD_ADMIN_VAULT_KEY_NAME=<key> OLD_ADMIN_VAULT_PUBKEY=<old-admin-pubkey>
   read -rs OLD_ADMIN_VAULT_TOKEN && export OLD_ADMIN_VAULT_TOKEN
   PRIVATE_CHANNEL_READ_RPC_URL=<channel-read-rpc> cargo run --bin withdraw -- \
     <channel-gateway-rpc> env:OLD_ADMIN <mint> <amount>
   ```
2. **Fund the new admin on Solana.** It pays every release and rotation once it takes
   over; the Grafana fee payer alerts warn below 1 SOL.
3. **Open the overlap.** Set `extra_admin_keys: [<new-admin-pubkey>]` in
   `vars/<env>.yml` and deploy. The node now accepts admin instructions signed by either
   key (`PRIVATE_CHANNEL_ADMIN_KEYS` lists both).
4. **Stop both operators** so nothing mints, remints or writes a withdraw config with the
   old key during the move. Wait two minutes for in-flight transactions to settle.

   ```bash
   docker stop private-channel-operator-solana private-channel-operator-private-channel
   ```

5. **Move the mint authorities** with `rotate_channel_admin`. It lists every receipt mint
   from the indexer database, sends `SetAuthority` (MintTokens and FreezeAccount) for each
   one the old admin still holds, re-reads them all and exits non-zero if any still has
   the old admin. It refuses when the old admin is the permanent delegate. The database
   password goes in `DATABASE_URL`, not on the command line:

   ```bash
   export DATABASE_URL="postgres://<postgres_indexer_user>:$(sudo cat {{ host_data_dir }}/secrets/postgres-indexer/POSTGRES_PASSWORD)@127.0.0.1:5434/<postgres_indexer_db>"
   PRIVATE_CHANNEL_READ_RPC_URL=<channel-read-rpc> cargo run --bin rotate_channel_admin -- \
     <channel-gateway-rpc> <old-admin-keypair> <new-admin-pubkey> --permanent-delegate <delegate-pubkey>
   unset DATABASE_URL
   ```

   `--indexer-db-url <url>` also works but puts the password on argv. Rerun until it
   prints `Done: no mint has <old> as mint or freeze authority`. A remote old admin works
   as in step 1: `env:OLD_ADMIN` in place of `<old-admin-keypair>`. It refuses when the
   database lists no mint (usually the wrong `DATABASE_URL`); pass `--allow-empty` only
   when the channel really has none.
6. **Swap the admin signer and close the overlap.** Point `admin_keypair_path` (or the
   remote signer's `admin_pubkey` and backend vars) at the new key, set
   `extra_admin_keys: []`, and deploy. The deploy starts both operators with the new
   admin, and the node stops accepting the old one.
7. **Check the new treasury.** Make a test deposit of each allowed mint. The operator
   rewrites the mint's withdraw config in that deposit, naming the new admin as treasury
   (the config's `allow_mint_slot` guard lets it, since the slot is unchanged). Read the
   treasury back:

   ```bash
   pda=$(solana find-program-derived-address J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi string:withdraw_config pubkey:<mint>)
   curl -s <channel-read-rpc> -H 'Content-Type: application/json' \
     -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"$pda\",{\"encoding\":\"base64\"}]}" \
     | python3 -c 'import json,sys,base64;d=base64.b64decode(json.load(sys.stdin)["result"]["value"]["data"][0])[17:49];A="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";n=int.from_bytes(d,"big");print("1"*(len(d)-len(d.lstrip(b"\0")))+"".join(A[n//58**i%58] for i in range(50) if n//58**i)[::-1])'
   ```

   It must print the new admin. Fees collected between step 1 and this deposit went to
   the old admin; withdraw them like any balance (they now pay the fee and minimum).
8. **Remove the old admin's Operator PDA**, if it held one (the old single-key setup):

   ```bash
   export DATABASE_URL="postgres://<postgres_indexer_user>:$(sudo cat {{ host_data_dir }}/secrets/postgres-indexer/POSTGRES_PASSWORD)@127.0.0.1:5434/<postgres_indexer_db>"
   cargo run --bin remove_operator -- <solana-rpc> <escrow-admin-keypair> <instance-id> <old-admin-pubkey>
   unset DATABASE_URL
   ```

   Like operator step 5, it refuses while a withdrawal is not settled.

If the old admin key is compromised, the overlap of step 3 lets it keep signing admin
instructions on the channel until step 6, and it can mint until step 5. Run steps 3 to 6
back to back.

## `Instance.admin` (SetNewAdmin)

There is no script; `SetNewAdmin` is built with the escrow client
(`private-channel-escrow-program/clients/rust`, `SetNewAdmin`, or the TypeScript client).
Both the current and the new admin must sign. Accounts: `payer`, `current_admin`,
`instance`, `new_admin`, `event_authority`, `private_channel_escrow_program`.

- `SetNewAdmin` only rewrites `Instance.admin`. Existing Operator PDAs stay valid, and
  nothing on the host changes, so no redeploy is needed.
- Keep `Instance.admin` a separate offline key, never the channel admin or the operator: a
  key in both roles keeps its mint authority and Operator PDA after `SetNewAdmin`.
- After the handover, use the new key for every `add_operator`, `remove_operator` and
  `allow_mint`. The old key is refused with `InvalidAdmin`.

## Vault signers

- solana-keychain signs with the latest version of the `transit/` key named by
  `<role>_vault_key_name` and never compares it with the configured pubkey at startup. A
  wrong `<role>_pubkey` surfaces only when signing fails.
- Vault prints the public key base64. Convert it to base58 and compare it with
  `<role>_pubkey` before every deploy that changes a Vault key; the one-liner is in
  `private-channel-deploy/examples/signers/vault.yml`.
- Never run `vault write -f transit/keys/<name>/rotate` on a signing key. The new version
  is a different ed25519 key, so signing stops matching the pubkey the chain knows. To
  rotate, create a new key name and follow the operator or channel admin rotation above
  with it.
- Rotating only the Vault token is a secret rotation, with nothing on chain: see
  [`secret_rotation.md`](secret_rotation.md#signer-secrets).
