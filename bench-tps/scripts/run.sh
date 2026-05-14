#!/usr/bin/env bash
# =============================================================================
# run.sh — Full-stack private_channel bench-tps orchestration script
#
# What this script does (in order):
#   1.  Parse script-level flags (--rebuild, --clean).
#   2.  Sanity-check that the bench binary and .env file exist.
#   3.  Load environment variables from .env.
#   4.  Compute CPU affinity splits so services and the bench binary don't
#       compete for the same cores.
#   5.  Generate (or reuse) a persistent admin keypair and patch it into .env
#       so the write-node whitelists the admin for privileged transactions.
#   6.  Build Solana programs (.so files) if missing or --rebuild was passed.
#   7.  Build Docker service images if missing or --rebuild was passed.
#   8.  Optionally wipe data volumes (--clean) to start from a clean state.
#   9.  Fix WAL archive volume permissions that Docker creates as root.
#   10. Start all Docker Compose services.
#   11. Pin all private_channel containers to the service CPU set.
#   12. Wait for every service to reach a stable/healthy state before
#       proceeding — this prevents the bench from hitting a half-started node.
#   13. Run the bench binary (with optional CPU pinning) and forward any
#       extra CLI arguments passed to this script.
#   14. Stop all services on exit (via trap).
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"

# Release binary produced by: cargo build --release -p private-channel-bench-tps
# bench-tps is a workspace member so Cargo outputs to the workspace target dir.
BENCH_BIN="${REPO_ROOT}/target/release/private-channel-bench-tps"

# .env file loaded by both this script and docker compose.
# Copy from .env.sample and fill in values before first run.
BENCH_ENV="${BENCH_DIR}/.env"

# ---------------------------------------------------------------------------
# Step 1 — Parse script-level flags
#
# --rebuild  Force-rebuild the Rust binary, Solana programs, and Docker images.
#            Also regenerates the admin keypair.  Use this after code changes.
#
# --no-clean          Skip wiping Docker volumes.  By default volumes are wiped
#                     on every run (the validator ledger resets, so stale DB
#                     state from a prior run will cause the indexer's startup
#                     reconciliation to fail).  Pass --no-clean only when you
#                     intentionally want to resume from existing state, e.g.
#                     after a crash where the validator did not reset.
#
# --no-refresh-metrics  Skip recreating Prometheus + Grafana. By default the
#                       script refreshes them to reload scrape config and
#                       dashboards (safe; does not delete data volumes).
#
# --no-teardown         Skip `docker compose down` at exit.  Containers keep
#                       running after the bench finishes (or is interrupted)
#                       so logs and metrics can be collected for offline
#                       analysis.  Stop them manually afterwards with
#                       `docker compose -f <repo>/docker-compose.yml down`.
#
# --private-channel-threads N  Pin Solana Private Channels service containers to the first N CPU cores
#                     and the bench binary to the remaining cores.  When
#                     omitted the default 75% / 25% split is used.
#
# Any other flags are collected into BENCH_ARGS and forwarded verbatim to the
# bench binary at the end of the script (e.g. --threads 20 --duration 120).
# ---------------------------------------------------------------------------
REBUILD=0
CLEAN=1          # default: always wipe volumes because validator resets each run
REFRESH_METRICS=1
TEARDOWN=1       # default: tear down containers on exit
PRIVATE_CHANNEL_THREADS=""   # explicit core count for services (optional)
BENCH_ARGS=()
SKIP_NEXT=0
for arg in "$@"; do
    if [ "${SKIP_NEXT}" -eq 1 ]; then
        PRIVATE_CHANNEL_THREADS="${arg}"
        SKIP_NEXT=0
        continue
    fi
    case "${arg}" in
        --rebuild)        REBUILD=1 ;;
        --no-clean)       CLEAN=0 ;;
        --no-refresh-metrics) REFRESH_METRICS=0 ;;
        --no-teardown)    TEARDOWN=0 ;;
        --private-channel-threads) SKIP_NEXT=1 ;;  # value is the next token
        *)                BENCH_ARGS+=("${arg}") ;;
    esac
done

# ---------------------------------------------------------------------------
# Step 2 — Build bench binary (if needed) and ensure .env exists
#
# With --rebuild the binary is always recompiled.  On first run (no binary
# yet) it is built automatically so `run.sh --rebuild` works from scratch.
# The .env file is auto-seeded from .env.sample if it does not exist yet.
# ---------------------------------------------------------------------------
if [ "${REBUILD}" -eq 1 ]; then
    echo "Building bench binary (--rebuild flag set)..."
    cargo build --release --manifest-path "${BENCH_DIR}/Cargo.toml"
elif [ ! -f "${BENCH_BIN}" ]; then
    echo "Bench binary not found — building (this may take a few minutes)..."
    cargo build --release --manifest-path "${BENCH_DIR}/Cargo.toml"
else
    echo "Bench binary found — skipping build"
fi

if [ ! -f "${BENCH_ENV}" ]; then
    if [ -f "${BENCH_DIR}/.env.sample" ]; then
        cp "${BENCH_DIR}/.env.sample" "${BENCH_ENV}"
        echo "Created ${BENCH_ENV} from .env.sample (review and adjust values if needed)"
    else
        echo "ERROR: ${BENCH_ENV} not found and no .env.sample to seed from" >&2
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Step 3 — Load environment variables from .env
#
# set -a exports every variable defined while the file is sourced so that
# child processes (docker compose, the bench binary) inherit them.
# ---------------------------------------------------------------------------
# shellcheck disable=SC1091
set -a; source "${BENCH_ENV}"; set +a

WRITE_PORT="${PRIVATE_CHANNEL_WRITE_PORT:-8899}"
GATEWAY_PORT="${GATEWAY_PORT:-8898}"

# ---------------------------------------------------------------------------
# Step 4 — CPU affinity split
#
# Assigning separate cores to Docker services and the bench binary eliminates
# CPU competition that would artificially inflate RTT measurements.
#
# Two modes:
#
#   --private-channel-threads N  (explicit)
#     Pins services to cores 0..N-1 and the bench to the remaining cores
#     N..TOTAL-1.  Use this when you know exactly how many cores the Solana Private Channels
#     stack needs (e.g. you profiled it and it saturates 6 cores).
#     Example: --private-channel-threads 6 on a 10-core machine gives bench cores 6-9.
#
#   (default — 75% / 25% rule)
#     Allocates floor(TOTAL * 0.75) cores to services and the remainder to
#     the bench.  Enforces a minimum of 1 core on each side.
#     Example: 8 cores → services 0-5, bench 6-7.
#
# On a single-core machine CPU pinning is skipped entirely.
# ---------------------------------------------------------------------------
TOTAL_CORES=$(nproc)

if [ "${TOTAL_CORES}" -lt 2 ]; then
    echo "WARNING: only ${TOTAL_CORES} core(s) detected — skipping CPU pinning" >&2
    SERVICE_CPUSET=""
    BENCH_CPUSET=""
else
    if [ -n "${PRIVATE_CHANNEL_THREADS}" ]; then
        # Explicit mode: caller specified exactly how many cores go to services.
        SERVICE_COUNT="${PRIVATE_CHANNEL_THREADS}"
        if [ "${SERVICE_COUNT}" -lt 1 ] || [ "${SERVICE_COUNT}" -ge "${TOTAL_CORES}" ]; then
            echo "ERROR: --private-channel-threads must be between 1 and $(( TOTAL_CORES - 1 ))" >&2
            exit 1
        fi
    else
        # Default mode: 75% to services (rounded down, minimum 1).
        SERVICE_COUNT=$(( TOTAL_CORES * 3 / 4 ))
        [ "${SERVICE_COUNT}" -lt 1 ] && SERVICE_COUNT=1
        [ "${SERVICE_COUNT}" -ge "${TOTAL_CORES}" ] && SERVICE_COUNT=$(( TOTAL_CORES - 1 ))
    fi

    BENCH_START="${SERVICE_COUNT}"
    BENCH_END=$(( TOTAL_CORES - 1 ))

    # cpuset strings accepted by taskset -c and docker update --cpuset-cpus.
    SERVICE_CPUSET="0-$(( SERVICE_COUNT - 1 ))"
    BENCH_CPUSET="${BENCH_START}-${BENCH_END}"

    echo "CPUs: total=${TOTAL_CORES}  services=[${SERVICE_CPUSET}]  bench=[${BENCH_CPUSET}]"
fi

# ---------------------------------------------------------------------------
# Step 5 — Build / reuse admin keypair and patch .env
#
# The admin keypair serves two purposes:
#   a. The bench binary uses it to initialise the SPL mint, create ATAs, and
#      mint initial token balances to every account (setup phase).
#   b. The write-node reads PRIVATE_CHANNEL_ADMIN_KEYS at startup to decide which
#      public keys are allowed to submit privileged admin transactions.
#
# To avoid a chicken-and-egg problem the keypair is generated here, before
# any Docker service starts, and the two env vars are patched into .env:
#   PRIVATE_CHANNEL_ADMIN_KEYS   — space-separated base58 public key(s)
#   ADMIN_PRIVATE_KEY   — the full private key JSON, one line (no whitespace)
#
# The keypair file is reused across runs so the write-node config remains
# valid without needing a full restart.  Pass --rebuild to regenerate it.
# ---------------------------------------------------------------------------
if ! command -v solana-keygen > /dev/null 2>&1; then
    echo "ERROR: solana-keygen not found in PATH" >&2
    echo "       Install the Solana CLI or add it to PATH before running run.sh" >&2
    exit 1
fi

ADMIN_KEYPAIR_FILE="${BENCH_DIR}/admin-keypair.json"

if [ "${REBUILD}" -eq 1 ] || [ ! -f "${ADMIN_KEYPAIR_FILE}" ]; then
    # --force overwrites an existing file without prompting.
    # --no-bip39-passphrase / --silent suppress interactive prompts.
    solana-keygen new --no-bip39-passphrase --silent --force --outfile "${ADMIN_KEYPAIR_FILE}"
    echo "Generated admin keypair: ${ADMIN_KEYPAIR_FILE}"
else
    echo "Reusing existing admin keypair: ${ADMIN_KEYPAIR_FILE}"
fi

ADMIN_PUBKEY=$(solana-keygen pubkey "${ADMIN_KEYPAIR_FILE}")
# Strip all whitespace from the JSON bytes array so it fits on a single line
# and can be embedded as an env var value without quoting issues.
ADMIN_PRIVKEY_JSON=$(tr -d '[:space:]' < "${ADMIN_KEYPAIR_FILE}")

echo "Admin pubkey: ${ADMIN_PUBKEY}"

# patch_env KEY VALUE — updates or appends a KEY=VALUE line in .env in-place.
# Using sed with the | delimiter avoids breakage if VALUE contains slashes.
patch_env() {
    local key="$1"
    local value="$2"
    if grep -q "^${key}=" "${BENCH_ENV}"; then
        sed -i "s|^${key}=.*|${key}=${value}|" "${BENCH_ENV}"
    else
        echo "${key}=${value}" >> "${BENCH_ENV}"
    fi
}

patch_env "PRIVATE_CHANNEL_ADMIN_KEYS" "${ADMIN_PUBKEY}"
patch_env "ADMIN_PRIVATE_KEY" "${ADMIN_PRIVKEY_JSON}"

echo "Patched PRIVATE_CHANNEL_ADMIN_KEYS and ADMIN_PRIVATE_KEY in ${BENCH_ENV}"

# ---------------------------------------------------------------------------
# Step 5b — Generate / reuse the deposit instance-seed keypair and derive PDA
#
# The bench deposit subcommand creates an escrow instance on Solana during setup.
# indexer-solana and operator-solana must be pre-configured with the matching
# instance PDA so they can observe deposits as they land.
#
# Strategy:
#   1. Generate (or reuse) a persistent instance-seed keypair file alongside
#      the admin keypair.  Using a fixed file means the same instance PDA is
#      produced on every run as long as the file is not deleted.
#   2. Derive the instance PDA using the bench binary's `derive-pda` subcommand
#      (no RPC call needed — pure local computation).
#   3. Patch COMMON_ESCROW_INSTANCE_ID into .env so docker-compose passes it
#      to both indexer-solana and operator-solana.
#
# On --rebuild the keypair is regenerated and the PDA changes accordingly.
# ---------------------------------------------------------------------------
INSTANCE_SEED_FILE="${BENCH_DIR}/deposit-instance-seed.json"

if [ "${REBUILD}" -eq 1 ] || [ ! -f "${INSTANCE_SEED_FILE}" ]; then
    solana-keygen new --no-bip39-passphrase --silent --force --outfile "${INSTANCE_SEED_FILE}"
    echo "Generated deposit instance-seed keypair: ${INSTANCE_SEED_FILE}"
else
    echo "Reusing existing deposit instance-seed keypair: ${INSTANCE_SEED_FILE}"
fi

# Derive the instance PDA via the bench binary (builds fast, no RPC needed).
BENCH_DEPOSIT_INSTANCE_PDA=$("${BENCH_BIN}" derive-pda \
    --instance-seed-keypair "${INSTANCE_SEED_FILE}")
echo "Deposit instance PDA: ${BENCH_DEPOSIT_INSTANCE_PDA}"

# Write as COMMON_ESCROW_INSTANCE_ID so docker-compose can pass it through to
# both indexer-solana and operator-solana without an explicit mapping.  Also
# keep BENCH_DEPOSIT_INSTANCE_PDA for reference / debugging.
patch_env "COMMON_ESCROW_INSTANCE_ID" "${BENCH_DEPOSIT_INSTANCE_PDA}"

# Re-source so the shell environment reflects the patched values before
# `docker compose up` (Step 10).  Shell env vars take precedence over
# --env-file in docker compose, so without this re-source the operator
# containers would inherit the stale empty ADMIN_PRIVATE_KEY that was
# exported during the initial Step 3 source.
# shellcheck disable=SC1091
set -a; source "${BENCH_ENV}"; set +a

# (BENCH_METRICS_TARGET is set in Step 10b after the Docker network exists)

# ---------------------------------------------------------------------------
# Step 6 — Build Solana programs (.so files)
#
# The solana-test-validator mounts the compiled program .so files at startup
# via --bpf-program flags in docker-compose.yml.  If the files are missing
# the validator will fail to start.
#
# Programs are compiled with Anchor.  Build times are 3–10 minutes on first
# run; subsequent builds are cached by Cargo.
# ---------------------------------------------------------------------------
ESCROW_SO="${REPO_ROOT}/target/deploy/private_channel_escrow_program.so"
WITHDRAW_SO="${REPO_ROOT}/target/deploy/private_channel_withdraw_program.so"

programs_exist() {
    [ -f "${ESCROW_SO}" ] && [ -f "${WITHDRAW_SO}" ]
}

if [ "${REBUILD}" -eq 1 ]; then
    echo "Building Solana programs (--rebuild flag set)..."
    make -C "${REPO_ROOT}/private-channel-escrow-program" build
    make -C "${REPO_ROOT}/private-channel-withdraw-program" build
elif ! programs_exist; then
    echo "Solana .so files not found — building programs (this takes a few minutes)..."
    make -C "${REPO_ROOT}/private-channel-escrow-program" build
    make -C "${REPO_ROOT}/private-channel-withdraw-program" build
else
    echo "Solana .so files found — skipping program build"
fi

# ---------------------------------------------------------------------------
# Step 7 — Build Docker service images
#
# Docker Compose project name defaults to the repo directory name ("private-channel"),
# so images are tagged private-channel-<service>.  All service images are checked as a
# group: if any is missing the entire set is rebuilt to ensure consistency.
#
# The COMPOSE array is built as an array (not a string) to safely handle paths
# that might contain spaces.
#
# versions.env is layered under BENCH_ENV so SOLANA_VERSION / YELLOWSTONE_TAG
# reach the Dockerfile ARGs while bench-tps/.env values still win on conflict.
# ---------------------------------------------------------------------------
COMPOSE=(docker compose -f "${REPO_ROOT}/docker-compose.yml" --env-file "${REPO_ROOT}/versions.env" --env-file "${BENCH_ENV}")

# Only 4 distinct images get built: 8 of the 11 services share the single
# private-channel-app image (via the x-private-channel-app YAML anchor in
# docker-compose.yml). The validator, prometheus, and grafana services have
# their own Dockerfiles and therefore their own images. Listing the merged
# services here would make images_exist() always return false, forcing a
# full rebuild every run.
BUILT_IMAGES=(private-channel-app private-channel-validator private-channel-prometheus private-channel-grafana)
BUILT_SERVICES=(write-node read-node gateway streamer validator indexer-solana indexer-private-channel operator-solana operator-private-channel prometheus grafana)

images_exist() {
    # docker image inspect exits non-zero if any image in the list is missing.
    docker image inspect "${BUILT_IMAGES[@]}" > /dev/null 2>&1
}

if [ "${REBUILD}" -eq 1 ]; then
    echo "Rebuilding images (--rebuild flag set)..."
    "${COMPOSE[@]}" build "${BUILT_SERVICES[@]}"
elif ! images_exist; then
    echo "Images not found — building for the first time (this takes a few minutes)..."
    "${COMPOSE[@]}" build "${BUILT_SERVICES[@]}"
else
    echo "Images found — skipping build"
fi

# ---------------------------------------------------------------------------
# Step 8 — Wipe data volumes (default; skip with --no-clean)
#
# Volumes are wiped by default because the local Solana validator resets its
# ledger on every run, leaving the indexer DB in a state that is inconsistent
# with on-chain reality.  The startup reconciliation will detect this mismatch
# and crash the indexer unless the DB is also reset.
#
# Pass --no-clean only when you intentionally want to resume from existing
# state (e.g. after a CTRL+C where the validator was not restarted).
#
# WARNING: this permanently deletes all data in those volumes.
# ---------------------------------------------------------------------------
if [ "${CLEAN}" -eq 1 ]; then
    echo "Removing data volumes (pass --no-clean to skip)..."
    "${COMPOSE[@]}" down -v 2>/dev/null || true
    echo "Data volumes removed."
fi

# ---------------------------------------------------------------------------
# Step 9 — Fix WAL archive volume permissions
#
# Docker creates named volumes owned by root.  The postgres containers run as
# the "postgres" user (uid 70 on Alpine), which cannot write to a root-owned
# directory.  We fix ownership by running a one-off alpine container that
# mounts each volume and chowns it before the postgres containers start.
#
# If a volume does not yet exist (first run after --clean) the chown still
# succeeds because Docker auto-creates the volume when the container starts.
# ---------------------------------------------------------------------------
for vol in postgres-indexer-wal-archive postgres-primary-wal-archive; do
    docker run --rm -v "${vol}:/vol" postgres:16-alpine \
        chown postgres:postgres /vol 2>/dev/null \
        && echo "Fixed permissions on volume ${vol}" \
        || echo "WARNING: could not fix permissions on ${vol} (may not exist yet)"
done

# ---------------------------------------------------------------------------
# Step 10 — Start all services
#
# --no-build skips Docker's build-context re-evaluation (we already built
# above) which makes startup faster and avoids redundant layer checks.
# Services run in detached mode (-d); logs are not tailed here.
# ---------------------------------------------------------------------------
echo "Starting all services..."
"${COMPOSE[@]}" up -d --no-build

# ---------------------------------------------------------------------------
# Step 10b — Detect Docker gateway IP and refresh Prometheus + Grafana
#
# The private-channel-network bridge is only created by `docker compose up` (Step 10),
# so gateway detection must happen here — after the network exists — not before.
#
# On Linux, `host.docker.internal` is not automatically resolvable inside
# containers, so we use the Docker bridge gateway IP (e.g. 172.18.0.1) as the
# Prometheus scrape target for the bench binary running on the host.
#
# After patching .env, Prometheus is always recreated so it reads the updated
# prometheus.yml with the correct BENCH_METRICS_TARGET value.
# ---------------------------------------------------------------------------
DEFAULT_BENCH_METRICS_TARGET="host.docker.internal:9101"

# Only auto-detect if the caller hasn't already set BENCH_METRICS_TARGET.
if [ -z "${BENCH_METRICS_TARGET:-}" ]; then
    NET_ID=$(docker network ls \
        --filter label=com.docker.compose.network=private-channel-network \
        -q | head -n 1)
    if [ -n "${NET_ID}" ]; then
        GW_IP=$(docker network inspect "${NET_ID}" \
            -f '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || true)
    fi
    if [ -n "${GW_IP:-}" ]; then
        BENCH_METRICS_TARGET="${GW_IP}:9101"
    else
        BENCH_METRICS_TARGET="${DEFAULT_BENCH_METRICS_TARGET}"
    fi
fi

patch_env "BENCH_METRICS_TARGET" "${BENCH_METRICS_TARGET}"
echo "Patched BENCH_METRICS_TARGET=${BENCH_METRICS_TARGET} in ${BENCH_ENV}"

# Re-source so the recreated Prometheus container inherits the updated value.
# shellcheck disable=SC1091
set -a; source "${BENCH_ENV}"; set +a

# Always recreate Prometheus (and Grafana if REFRESH_METRICS) so they pick up
# the correct scrape target and latest dashboard files.
if [ "${REFRESH_METRICS}" -eq 1 ]; then
    echo "Refreshing Prometheus + Grafana..."
    "${COMPOSE[@]}" up -d --force-recreate prometheus grafana
else
    echo "Refreshing Prometheus (target update)..."
    "${COMPOSE[@]}" up -d --force-recreate prometheus
fi

# ---------------------------------------------------------------------------
# Step 10c — Open host firewall for bench metrics scraping
#
# The bench binary listens on 0.0.0.0:9101 (host). Prometheus runs inside
# Docker and reaches the host via the bridge gateway (172.18.0.1). On many
# Linux hosts the INPUT chain policy is DROP, which silently blocks this.
# Insert a rule allowing TCP 9101 from the Docker subnet if it isn't there.
# ---------------------------------------------------------------------------
DOCKER_SUBNET="172.18.0.0/16"
if ! sudo iptables -C INPUT -p tcp --dport 9101 -s "${DOCKER_SUBNET}" -j ACCEPT 2>/dev/null; then
    sudo iptables -I INPUT -p tcp --dport 9101 -s "${DOCKER_SUBNET}" -j ACCEPT \
        && echo "Opened host port 9101 for Docker subnet ${DOCKER_SUBNET}" \
        || echo "WARNING: could not add iptables rule — Prometheus may not scrape bench metrics"
fi

# ---------------------------------------------------------------------------
# Step 11 — Pin private_channel containers to service CPU cores
#
# docker update --cpuset-cpus applies cgroup CPU affinity after a container
# has already started.  We query running containers by name prefix rather than
# hardcoding a list so new services are pinned automatically.
# ---------------------------------------------------------------------------
if [ -n "${SERVICE_CPUSET}" ]; then
    echo "Pinning containers to cores [${SERVICE_CPUSET}]..."
    while IFS= read -r container; do
        docker update --cpuset-cpus="${SERVICE_CPUSET}" "${container}" 2>/dev/null \
            && echo "  pinned ${container}" \
            || echo "  WARNING: could not pin ${container}"
    done < <(docker ps --filter "name=private-channel-" --format "{{.Names}}")
fi

# ---------------------------------------------------------------------------
# Step 12 — Wait for every service to reach a stable state
#
# Three wait strategies are used depending on what each service exposes:
#
#   wait_healthy   — polls Docker's built-in healthcheck status (requires a
#                    HEALTHCHECK in the Dockerfile).  Used for postgres
#                    instances (pg_isready) and the validator (cluster-version).
#
#   wait_rpc       — sends a getLatestBlockhash JSON-RPC request and waits for
#                    an HTTP 200 response.  Used for write-node and read-node
#                    because it proves the full path (DB → migration → RPC) is
#                    live, not just that the process started.  Fails fast if
#                    the container crashes rather than waiting the full timeout.
#
#   wait_http_health — sends a GET request to a /health HTTP endpoint.  Used
#                    for the gateway which exposes its own health check.
#
#   wait_running   — checks that the container State.Status = "running".  Used
#                    for services that have no healthcheck or RPC endpoint
#                    (streamer, indexers, operators, observability stack).
#
# All waits poll every 2 seconds and print a dot per attempt so the operator
# can see progress.  Services with a fixed maximum wait time will error out
# and print the docker logs command to use for debugging.
# ---------------------------------------------------------------------------

# --- wait helper functions -------------------------------------------------

wait_healthy() {
    local container="$1"
    local max_wait=120
    local elapsed=0
    printf "Waiting for %s to be healthy..." "${container}"
    while [ "${elapsed}" -lt "${max_wait}" ]; do
        status=$(docker inspect --format='{{.State.Health.Status}}' "${container}" 2>/dev/null || echo "missing")
        if [ "${status}" = "healthy" ]; then
            echo " ok"
            return 0
        fi
        sleep 2
        elapsed=$(( elapsed + 2 ))
        printf "."
    done
    echo ""
    echo "ERROR: ${container} did not become healthy within ${max_wait}s" >&2
    return 1
}

# Probes a Solana JSON-RPC endpoint directly with a POST request.
# Succeeds as soon as the node responds to getLatestBlockhash (any HTTP 200).
# Also fails fast if the container exits or crashes during the wait.
wait_rpc() {
    local label="$1"
    local url="$2"
    local container="$3"
    local max_wait=180
    local elapsed=0
    printf "Waiting for %s at %s..." "${label}" "${url}"
    while [ "${elapsed}" -lt "${max_wait}" ]; do
        if curl -sf -X POST -H "Content-Type: application/json" \
            -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash"}' \
            "${url}" > /dev/null 2>&1; then
            echo " ok"
            return 0
        fi
        # Fail fast rather than waiting the full timeout if the container died.
        local state
        state=$(docker inspect --format='{{.State.Status}}' "${container}" 2>/dev/null || echo "missing")
        if [ "${state}" = "exited" ] || [ "${state}" = "dead" ] || [ "${state}" = "missing" ]; then
            echo ""
            echo "ERROR: ${container} has stopped (state=${state})" >&2
            echo "       Run: docker logs ${container}" >&2
            return 1
        fi
        sleep 2
        elapsed=$(( elapsed + 2 ))
        printf "."
    done
    echo ""
    echo "ERROR: ${label} did not respond within ${max_wait}s" >&2
    echo "       Run: docker logs ${container}" >&2
    return 1
}

# Waits for a container to reach the "running" state (no Docker healthcheck).
# Fails fast if the container exits or crashes.
wait_running() {
    local container="$1"
    local max_wait="${2:-60}"
    local elapsed=0
    printf "Waiting for %s to be running..." "${container}"
    while [ "${elapsed}" -lt "${max_wait}" ]; do
        state=$(docker inspect --format='{{.State.Status}}' "${container}" 2>/dev/null || echo "missing")
        if [ "${state}" = "running" ]; then
            echo " ok"
            return 0
        fi
        if [ "${state}" = "exited" ] || [ "${state}" = "dead" ]; then
            echo ""
            echo "ERROR: ${container} exited unexpectedly" >&2
            echo "       Run: docker logs ${container}" >&2
            return 1
        fi
        sleep 2
        elapsed=$(( elapsed + 2 ))
        printf "."
    done
    echo ""
    echo "ERROR: ${container} did not reach running state within ${max_wait}s" >&2
    return 1
}

# Waits for an HTTP endpoint to return any successful response.
# Used for the gateway's /health endpoint which checks the gateway process
# itself, not a proxied backend — so it responds independently of write/read
# node availability.
wait_http_health() {
    local label="$1"
    local url="$2"
    local max_wait=60
    local elapsed=0
    printf "Waiting for %s at %s..." "${label}" "${url}"
    while [ "${elapsed}" -lt "${max_wait}" ]; do
        if curl -sf "${url}" > /dev/null 2>&1; then
            echo " ok"
            return 0
        fi
        sleep 2
        elapsed=$(( elapsed + 2 ))
        printf "."
    done
    echo ""
    echo "ERROR: ${label} did not respond within ${max_wait}s" >&2
    return 1
}

READ_PORT="${PRIVATE_CHANNEL_READ_PORT:-8900}"

# --- Wait for each service group in dependency order ----------------------

# Databases must be healthy before write-node/read-node attempt migrations.
wait_healthy "private-channel-postgres-primary"
wait_healthy "private-channel-postgres-replica"
wait_healthy "private-channel-postgres-indexer"

# Validator must be healthy (confirmed via solana cluster-version healthcheck)
# before write-node and read-node attempt to connect to it.
wait_healthy "private-channel-validator"

# Write-node and read-node: probe via JSON-RPC to confirm the full startup path
# (DB connection, schema migration, RPC listener) is complete.
wait_rpc "write-node" "http://localhost:${WRITE_PORT}" "private-channel-write-node"
wait_rpc "read-node"  "http://localhost:${READ_PORT}"  "private-channel-read-node"

# Gateway: check its own /health endpoint (not a proxied backend call).
wait_http_health "gateway" "http://localhost:${GATEWAY_PORT}/health"

# Remaining services have no healthcheck or RPC endpoint; just confirm they
# are running and haven't crashed immediately on startup.
wait_running "private-channel-streamer"
wait_running "private-channel-indexer-solana"
wait_running "private-channel-indexer-private-channel"
wait_running "private-channel-operator-solana"
wait_running "private-channel-operator-private-channel"
wait_running "private-channel-prometheus"
wait_running "private-channel-grafana"
wait_running "private-channel-cadvisor"
wait_running "private-channel-blackbox-exporter"
wait_running "private-channel-pg-backup-primary"
wait_running "private-channel-pg-backup-indexer"

echo "All services stable."

# ---------------------------------------------------------------------------
# Step 13 — Run the bench binary
#
# Mandatory arguments always injected by this script:
#   --admin-keypair  path to the keypair generated in step 5
#   --metrics-port   fixed at 9101 (scraped by Prometheus)
#
# Subcommand-specific RPC injection:
#   transfer/withdraw → --rpc-url  (Solana Private Channels gateway)
#   deposit           → --solana-rpc-url (Solana validator, host port 18899)
#
# Any extra arguments passed to run.sh (i.e. those not consumed in step 1)
# are forwarded verbatim, allowing callers to override defaults:
#   ./scripts/run.sh transfer --threads 20 --duration 120
#   ./scripts/run.sh deposit  --duration 60
#
# When CPU pinning is active, taskset -c restricts the bench process to the
# bench CPU set so it does not compete with service containers.
# ---------------------------------------------------------------------------

# Register a cleanup function that tears down all Docker services when this
# script exits for any reason: normal completion, unhandled error, Ctrl+C, or
# SIGTERM from an external process.
#
# Using a _CLEANUP_DONE guard prevents the function from running twice when a
# caught signal causes bash to both run the signal trap and then re-trigger
# EXIT (bash fires EXIT after every signal trap that doesn't call `exit`
# explicitly, so without the guard the compose down would run twice).
_CLEANUP_DONE=0
cleanup() {
    [ "${_CLEANUP_DONE}" -eq 1 ] && return
    _CLEANUP_DONE=1
    echo ""
    if [ "${TEARDOWN}" -eq 0 ]; then
        echo "Skipping teardown (--no-teardown). Containers are still running."
        echo "  Inspect logs:  docker logs <container>"
        echo "  Stop later:    docker compose -f ${REPO_ROOT}/docker-compose.yml down"
        return
    fi
    echo "Tearing down all services..."
    "${COMPOSE[@]}" down 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT INT TERM ERR

echo ""
echo "Running bench on cores [${BENCH_CPUSET:-any}]..."
echo "-------------------------------------------------------"

# Determine the subcommand.  The first element of BENCH_ARGS that does not
# start with "--" is treated as the subcommand; if none is found, default to
# "transfer" to preserve backwards compatibility.
SUBCOMMAND="transfer"
REMAINING_ARGS=()
for arg in "${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"}"; do
    if [[ "${arg}" != --* ]] && [ "${SUBCOMMAND}" = "transfer" ] && \
       [[ "${arg}" =~ ^(transfer|deposit|withdraw)$ ]]; then
        SUBCOMMAND="${arg}"
    else
        REMAINING_ARGS+=("${arg}")
    fi
done

# Base mandatory flags injected by this script.
BASE_FLAGS=(
    --admin-keypair "${ADMIN_KEYPAIR_FILE}"
    --metrics-port 9101
)

# --rpc-url is used by transfer and withdraw (Solana Private Channels gateway); deposit and withdraw
# also need --solana-rpc-url for Solana escrow setup.
SOLANA_RPC="${BENCH_SOLANA_RPC_URL:-http://localhost:${PRIVATE_CHANNEL_VALIDATOR_PORT:-18899}}"

if [ "${SUBCOMMAND}" = "deposit" ]; then
    BASE_FLAGS+=(--solana-rpc-url "${SOLANA_RPC}")
    # Pass the persistent instance-seed keypair so the bench reuses the same
    # instance PDA that indexer-solana and operator-solana are watching.
    if [ -f "${INSTANCE_SEED_FILE}" ]; then
        BASE_FLAGS+=(--instance-seed-keypair "${INSTANCE_SEED_FILE}")
    fi
elif [ "${SUBCOMMAND}" = "withdraw" ]; then
    BASE_FLAGS+=(--rpc-url "http://localhost:${GATEWAY_PORT}")
    BASE_FLAGS+=(--solana-rpc-url "${SOLANA_RPC}")
    # Reuse the same instance-seed as deposit so COMMON_ESCROW_INSTANCE_ID matches
    # the PDA that operator-private-channel is watching.
    if [ -f "${INSTANCE_SEED_FILE}" ]; then
        BASE_FLAGS+=(--instance-seed-keypair "${INSTANCE_SEED_FILE}")
    fi
else
    BASE_FLAGS+=(--rpc-url "http://localhost:${GATEWAY_PORT}")
fi

if [ -n "${BENCH_CPUSET}" ]; then
    taskset -c "${BENCH_CPUSET}" "${BENCH_BIN}" \
        "${SUBCOMMAND}" \
        "${BASE_FLAGS[@]}" \
        "${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}"
else
    "${BENCH_BIN}" \
        "${SUBCOMMAND}" \
        "${BASE_FLAGS[@]}" \
        "${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}"
fi

echo "-------------------------------------------------------"
echo "Bench complete."
