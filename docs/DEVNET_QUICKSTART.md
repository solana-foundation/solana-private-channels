# Devnet Quick Start Guide

This guide walks you through deploying and running Solana Private Channels on Solana Devnet. By the end, you'll have a fully operational Solana Private Channels payment channel with real-time monitoring and deposit/withdrawal access to Solana Devnet.

## What is Solana Private Channels?

Solana Private Channels is a private payment channel with direct access to Solana liquidity. Users deposit tokens into an escrow on Solana Mainnet (or Devnet), which mints equivalent tokens on the Solana Private Channels payment channel. When withdrawing, tokens are burned on Solana Private Channels and released from escrow on Solana.

**Architecture Overview:**

- **Escrow Program**: On-chain Solana program that holds deposited tokens (Devnet Program ID: `GokvZqD2yP696rzNBNbQvcZ4VsLW7jNvFXU1kW9m7k83`)
- **Solana Private Channels Payment Channel**: Private execution environment (write node, read node, gateway)
- **Indexer** (2 instances): `indexer-solana` watches the Escrow program on Solana for deposits; `indexer-private-channel` watches the Withdraw program on the Solana Private Channels payment channel for withdrawals
- **Operator** (2 instances): `operator-solana` processes deposits (mints on the Solana Private Channels payment channel); `operator-private-channel` processes withdrawals (releases from escrow on Solana)

## Prerequisites

Before starting, ensure you have:

- **Docker Engine ≥ 26** (Engine or Desktop) — required for the BuildKit cache mounts the Dockerfiles use  
  - macOS Apple Silicon: Enable "Docker VMM" in Docker settings (configurable in "Settings" \-\> "Virtual Machine Options")  
- **Node.js 24.7.0** and **pnpm 10.15.1**  
- **Solana CLI 3.1.13** (Agave)  
- **Rust 1.91.0** (Agave v3.1.13 requires ≥ 1.86)  
- **Solana Wallet** that supports localhost or custom RPC (e.g., Backpack, Phantom, Solflare)  
- **Solana Devnet RPC** endpoint  
- **Yellowstone gRPC (Devnet)** endpoint (for real-time Solana event streaming)  
  - **Note**: You can use public Devnet RPC but need a Devnet Yellowstone gRPC node from a service provider for real-time indexing (e.g., Helius LazerStream, Triton, QuickNode).

> **Pinned versions.** Match these on the host to avoid drift from the images:
>
> - Solana, Node, and pnpm are the values in [`versions.env`](../versions.env), used as Docker build args — the images build with exactly these.
> - Rust is pinned in [`rust-toolchain.toml`](../rust-toolchain.toml).
> - Docker's `≥ 26` floor is enforced by `scripts/check-docker.sh` and the deploy preflight.

> **One-time setup for the `make` targets.** The `make docker-devnet-*` commands below enforce a BuildKit cache cap; install it once per host with `sudo make install-buildkit-cache` (writes `/etc/docker/daemon.json`, caps build cache at ~50 GB). Skip only if you run the raw `docker compose` commands instead.

## Step 0 (Optional): Use a Different Program ID

The images are **pinned to the canonical program IDs at compile time** (the IDs above) — `ESCROW_PROGRAM_ID` is *not* read on devnet, so an env var can't repoint them. You only need this if the escrow/withdraw program has been **redeployed to a new address**, or you're running your own. To switch:

1. Update the program ID everywhere it's compiled in: `declare_id!` in `private-channel-{escrow,withdraw}-program/program/src/lib.rs`, the constants in `indexer/src/indexer/datasource/common/parser/{escrow,withdraw}.rs`, and `core/src/bin/streamer.rs`.
2. Rebuild the program `.so` **and** the images: `make build-devnet && make docker-devnet-build`.
3. Deploy your program to devnet at that address: `make deploy-devnet DEPLOYER_KEY=<your-deployer-keypair>`.

Otherwise skip this — the canonical defaults work out of the box.

## Step 1: Build Docker Images

From the project root:

```shell
make docker-devnet-build
```

> Wraps `docker compose -f docker-compose.devnet.yml --env-file versions.env --env-file .env.devnet build` with preflight guards (Docker ≥ 26, BuildKit cache, env checks). Run the raw command directly if you prefer.

This builds all Solana Private Channels services (gateway, nodes, indexer, operator). This will take a long time (30min to an hour or so depending on your system), so it's recommended to run this in the background while you configure the rest of the stack (or go to the gym).

> ### Deploying to a remote host?
>
> **This guide builds and runs the stack *locally* with Docker Compose.** For an **automated single-host remote deployment** — Ansible-driven, pulls prebuilt images from GHCR, and self-verifies in one `ansible-playbook deploy.yml` command — follow the **[Operator Runbook → `private-channel-deploy/README.md`](../private-channel-deploy/README.md)** instead.
> The remaining steps below are for local setup only.

## Step 2: Set Up Admin UI

The Admin UI lets you create and configure your escrow instance via a web interface.

If you prefer, you can also use the [scripts](../scripts/devnet/README.md) or the [Escrow](../private-channel-escrow-program/clients/typescript) and [Withdrawal](../private-channel-withdraw-program/clients/typescript) clients to interact with the programs.

**Note:** The CLI scripts in `scripts/devnet/` may reference port 8898 for the gateway. This guide uses the Docker Compose default of 8899. Ensure your port configuration is consistent.

```shell
cd admin-ui
pnpm install
```

Create an environment file for the Admin UI:

```shell
# admin-ui/.env
PRIVATE_CHANNEL_RPC_URL=http://localhost:8899
```

Start the development server:

```shell
pnpm dev
```

Open [http://localhost:5173](http://localhost:5173) in your browser.

## Step 3: Create an Escrow Instance

1. **Connect Wallet**  

   - Set your browser wallet to **Devnet** network  
   - Ensure you have Devnet SOL for transaction fees (use the [Solana Faucet](https://faucet.solana.com/) if needed)

2. **Create Instance**  

   - In the Admin UI, click **"Create New Instance"**  
   - Approve the transaction in your wallet  
   - **Copy the Instance Address** — you'll need this for configuration

![Create Instance](./assets/create-instance.png)

## Step 4: Generate Operator Keypair

The operator keypair signs transactions for minting on the Solana Private Channels payment channel and releasing from escrow.

```shell
# Generate a new keypair
solana-keygen new -o operator-keypair.json -s --no-bip39-passphrase

# Get the public key
solana-keygen pubkey operator-keypair.json
```

## Step 5: Configure the Instance

Back in the Admin UI:

### Whitelist a Token Mint

1. Go to **Admin Functions** → **Mint Management**  
2. Enter the mint address you want to support (e.g., Devnet USDC: `4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU` – you can get some at the [USDC Faucet](https://faucet.circle.com/) or use your own devnet token mint)  
3. Click **"Allow Mint"** and approve the transaction

![Allow Mint](./assets/allow-mint.png)

### Add Operator

1. Go to **Admin Functions** → **Operator Management**  
2. Enter your operator's public key (from Step 4)  
3. Click **"Add Operator"** and approve the transaction

## Step 6: Configure Environment Variables

Update `.env.devnet` (tracked template) for non-secret values, and put all secrets in the gitignored `.env` in the project root — it is loaded last and overrides the templates, so live keys never touch a tracked file. `make build-devnet` writes `ADMIN_PRIVATE_KEY` to `.env` for you.

> **Required secrets — no defaults are shipped.** `POSTGRES_PASSWORD`, `POSTGRES_REPLICATION_PASSWORD`, and `ADMIN_PRIVATE_KEY` (and `JWT_SECRET` if you enable auth) MUST be set or the services fail to start. Generate strong passwords with `openssl rand -hex 32`.

```shell
# Required secrets: put these in the gitignored `.env`, NOT in .env.devnet
POSTGRES_PASSWORD=<openssl rand -hex 32>
POSTGRES_REPLICATION_PASSWORD=<openssl rand -hex 32>
# Operator keypair (written to `.env` automatically by `make build-devnet`)
ADMIN_PRIVATE_KEY=<your_operator_private_key_u8array_or_b58>

# Non-secret values below go in .env.devnet
# Escrow instance (from Step 3)
ESCROW_INSTANCE_ID=<your_instance_address>

# Keys allowed to mint on the Solana Private Channels payment channel (comma-separated public keys)
# For testing, use your operator's public key
PRIVATE_CHANNEL_ADMIN_KEYS=<operator_pubkey>

# Solana Devnet RPC
DEVNET_RPC_URL=https://api.devnet.solana.com

# Yellowstone gRPC (required for real-time indexing)
DEVNET_YELLOWSTONE_ENDPOINT=<your_yellowstone_grpc_endpoint>
INDEXER_YELLOWSTONE_TOKEN=<your_yellowstone_auth_token>

# Optional: Grafana alert webhook (defaults to empty if not set)
# ALERT_WEBHOOK_URL=<your_webhook_url>
```

**Make sure** to update each of these environment variables and ensure there are no duplicate keys before proceeding.

## Step 7: Start All Services

Once your docker build (Step 1) is complete, run:

```shell
make docker-devnet-up
```

You should see all services in a healthy/running state:

```shell
[+] Running 21/21a The requested image's platform (linux/amd64) does not match the detected host platfo
 ✔ Network private-channel_private-channel-network Created0.0s 
 ✔ Container private-channel-cadvisor Started2.4s 
 ✔ Container private-channel-postgres-primary Healthy13.1s                                
 ✔ Container private-channel-postgres-indexer Healthy14.1s                                
 ✔ Container private-channel-grafana Started2.5s  ✔ Container private-channel-prometheus Started2.5s  ✔ Container private-channel-indexer-solana Started12.2s
 ✔ Container private-channel-operator-solana Started12.2s
 ✔ Container private-channel-postgres-replica Started12.7s
 ✔ Container private-channel-write-node Started2.2s   ✔ Container private-channel-read-node Started12.4s 
 ✔ Container private-channel-operator-private-channel Started11.9s 
 ✔ Container private-channel-indexer-private-channel Started12.8s 
 ✔ Container private-channel-gateway Started12.3s 
```

Check logs if needed:

```shell
# All services
make docker-devnet-logs

# Specific service (no make target — use raw compose)
docker compose -f docker-compose.devnet.yml --env-file versions.env --env-file .env.devnet logs -f indexer-solana
```

For reference, here are the ports and endpoints that are now running:

| Service | Port | Description |
| :---- | :---- | :---- |
| Gateway | `8899` | Main RPC endpoint (routes to read/write nodes) |
| Write Node | `8900` | Handles transaction submissions |
| Read Node | `8901` | Handles read requests (getAccountInfo, etc.) |
| PostgreSQL Primary | `5432` | State database (write) — bound to `127.0.0.1` (loopback-only), not externally reachable |
| PostgreSQL Replica | `5433` | State database (read) — bound to `127.0.0.1` (loopback-only), not externally reachable |
| PostgreSQL Indexer | `5434` | Indexer/operator database — bound to `127.0.0.1` (loopback-only), not externally reachable |
| Admin UI | `5173` | Web interface for instance management |
| Grafana | `37429` | Metrics dashboard (default password: `admin`) |
| Prometheus | `9090` | Metrics collection |
| cAdvisor | `8080` | Container metrics |

### Node RPC ports and the RBAC boundary

The gateway (`8899`) is the only port that enforces RBAC (account-gating and operator-only methods). The
write-node (`8900`) and read-node (`8901`) RPC ports have **no node-side authentication**. Because of that, the
reference compose binds these node ports to loopback (`127.0.0.1`), so they are reachable from the host
but not from other machines. RBAC is an application-layer control on the gateway, not a network boundary.

##

## Step 8: Test Deposits and Withdrawals

### Deposit (Solana → Solana Private Channels)

1. In the Admin UI, scroll down to **User Functions**
2. Enter your whitelisted token that you are holding in the connected wallet
3. Enter an amount and click **"Deposit"** (make sure to include decimals for precision, e.g., 1 USDC should be 1000000)
4. Approve the transaction in your wallet

The indexer will detect the deposit and the operator will mint equivalent tokens on the Solana Private Channels payment channel.

![Deposit](./assets/deposit-funds.png)

### Verify Deposit on Solana Private Channels

You can verify your token is on the Solana Private Channels instance by navigating to **Solana Private Channels Management** at the top of the screen. Paste the mint’s address and click “Check Balance”. You should see that your tokens have landed on Solana Private Channels!

### Transfer (Within the Solana Private Channels Payment Channel)

After your balance has been verified on Solana Private Channels, you should now have an option to Transfer funds to another user. This is a simple way to demonstrate using the Solana Private Channels payment channel.

1. **Important**: Since we are working on the Solana Private Channels payment channel, you must switch your wallet’s RPC before transferring. Change it to **Localnet** or **Custom** (varies by wallet provider) and enter `http://localhost:8899` (the local gateway for your Solana Private Channels RPC)
2. Enter a user destination address and amount (with decimal precision)
3. Click send and confirm the transaction in your wallet!
4. You can check your Solana Private Channels balance again and notice that the funds have been debited by your transfer amount.

### Withdraw (Solana Private Channels → Solana)

1. In the Admin UI, go back to **Escrow Management**
2. Paste the token mint address and enter withdrawal amount
3. **Important**: Before withdrawing, make sure your wallet’s RPC is connected to **Localnet** or **Custom** and enter `http://localhost:8899` (the local gateway for your Solana Private Channels RPC)
4. Click **"Withdraw"** and approve the transaction
5. (Make sure to switch your wallet back to Devnet when you’re ready to do more devnet activity)

The indexer detects the burn on Solana Private Channels, builds a Merkle proof, and the operator releases funds from the Solana escrow. You should be able to check your balance in your wallet or on Solana explorer to see the withdrawal.

## Stopping Services

```shell
make docker-devnet-down
```

You should see something like this:

```shell
[+] Running 14/14
 ✔ Container private-channel-indexer-private-channel    Removed         10.7s 
 ✔ Container private-channel-gateway           Removed          0.7s 
 ✔ Container private-channel-operator-private-channel   Removed         10.7s 
 ✔ Container private-channel-grafana           Removed          0.5s 
 ✔ Container private-channel-operator-solana   Removed         10.5s 
 ✔ Container private-channel-cadvisor          Removed          0.7s 
 ✔ Container private-channel-indexer-solana    Removed         10.7s 
 ✔ Container private-channel-prometheus        Removed          0.6s 
 ✔ Container private-channel-read-node         Removed          0.6s 
 ✔ Container private-channel-postgres-indexer  Removed          0.8s 
 ✔ Container private-channel-postgres-replica  Removed          0.9s 
 ✔ Container private-channel-write-node        Removed          0.6s 
 ✔ Container private-channel-postgres-primary  Removed          0.5s 
 ✔ Network private-channel_private-channel-network      Removed          0.2s
```

To also remove volumes (reset all state):

```shell
make docker-devnet-clean
```

## Troubleshooting

### Services won't start

- Ensure Docker has enough resources allocated (4GB+ RAM recommended)  
- Check that all required environment variables are set  
- Verify your Yellowstone endpoint is accessible and enabled for Devnet

### Transactions failing

- Ensure operator has Devnet SOL for fees  
- Verify the mint is whitelisted on the instance  
- Try using CLI tools in `scripts/devnet/` instead of the Admin UI  
- Check operator logs: `docker compose -f docker-compose.devnet.yml --env-file versions.env --env-file .env.devnet logs operator-solana`  
  - *Transaction failed: InstructionError(1, Custom(4))* error suggests that the admin environment variable is misconfigured. Check your ENV vars and restart your services. You may need to initialize a new instance/mint afterwards. Or, remove the volumes and start fresh with `make docker-devnet-clean`.
- If using the Admin UI, ensure your wallet is on the correct cluster for the correct task (instructions relating to instance management and deposits should use Devnet, and transfers/withdrawals should use your Solana Private Channels RPC URL (localhost:8899 in our example))

### Indexer not detecting events

- Confirm Yellowstone endpoint and token are correct  
- Ensure environment variables are properly configured  
- For debugging, check if backfill is needed (see config files in `scripts/devnet/config/`)

## Get Help

Solana Private Channels is still in the early stages of development. If you run into issues or bugs, please [create an issue](https://github.com/solana-foundation/solana-private-channels/issues) and outline your steps to reproduce it.

## Configuration Reference

The TOML config files in `scripts/devnet/config/` allow fine-tuning:

| File | Purpose |
| :---- | :---- |
| `indexer-solana.toml` | Solana chain indexer (Yellowstone) |
| `indexer-private-channel.toml` | Solana Private Channels payment channel indexer (RPC polling) |
| `operator-solana.toml` | Processes deposits → mints on Solana Private Channels |
| `operator-private-channel.toml` | Processes withdrawals → releases on Solana |

**Note:** The TOML files contain placeholder values. When running via Docker Compose, the environment variables from `.env.devnet` override these values at runtime. You do not need to edit the TOML files directly — configure everything through `.env.devnet`.

*Note: for the demo, we have disabled backfills — if your use case requires it, we recommend the `start_slot` be just before the slot you created your instance to avoid unnecessary polling.*

## Learn More

- [Escrow Interaction Guide](./ESCROW_INTERACTION_GUIDE.md) — Programmatic escrow interactions  
- [Withdrawing Guide](./WITHDRAWING_GUIDE.md) — Deep dive on the withdrawal flow
