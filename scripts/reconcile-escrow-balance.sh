#!/usr/bin/env bash
# reconcile-escrow-balance.sh — Reconcile on-chain escrow balance vs DB expected balance per mint
#
# Usage: ./scripts/reconcile-escrow-balance.sh <ESCROW_OWNER> <MINT>
#
# Arguments:
#   ESCROW_OWNER       — Instance PDA that owns escrow token accounts
#   MINT               — Token mint address to reconcile
#
# Environment variables:
#   SOLANA_RPC_URL  — RPC endpoint (default: http://localhost:8899)
#   ALERT_WEBHOOK   — Optional webhook URL for mismatch alerts
#   PGSERVICE, or PGHOST/PGPORT/PGDATABASE/PGUSER: indexer DB connection (libpq)
#   PGPASSFILE: mode-0600 password file (default: ~/.pgpass)
#
# The DB password never goes in any argv: argv is visible to every local user.
#
# Requirements: spl-token CLI, psql, jq, curl
#
# Exit codes:
#   0 — balances match
#   1 — mismatch detected (ALERT)
#   2 — usage/connection error

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <ESCROW_OWNER> <MINT>"
    echo ""
    echo "Example:"
    echo "  PGSERVICE=indexer $0 5xYz...PDA So11...mint"
    echo ""
    echo "Environment variables:"
    echo "  SOLANA_RPC_URL  — RPC endpoint (default: http://localhost:8899)"
    echo "  ALERT_WEBHOOK   — Optional webhook URL for mismatch alerts"
    echo "  PGSERVICE, or PGHOST/PGPORT/PGDATABASE/PGUSER: indexer DB connection"
    echo "  PGPASSFILE: mode-0600 password file (default: ~/.pgpass)"
    exit 2
fi

ESCROW_OWNER="$1"
MINT="$2"
RPC_URL="${SOLANA_RPC_URL:-http://localhost:8899}"

# Without either, libpq quietly falls back to a default database.
if [ -z "${PGSERVICE:-}" ] && [ -z "${PGDATABASE:-}" ]; then
    echo "ERROR: set PGSERVICE or PGDATABASE to the indexer DB."
    exit 2
fi

echo "=== Solana Private Channels Escrow Balance Reconciliation ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Escrow owner: ${ESCROW_OWNER}"
echo "Mint:         ${MINT}"
echo "RPC:          ${RPC_URL}"
echo "DB:           ${PGSERVICE:-${PGHOST:-local}/${PGDATABASE}}"
echo ""

# Step 1: Get on-chain token balance (raw units)
echo "Fetching on-chain balance..."
SPL_OUTPUT=$(spl-token balance --owner "$ESCROW_OWNER" "$MINT" --url "$RPC_URL" --output json 2>&1) || {
    echo "ERROR: Failed to fetch on-chain token balance for mint ${MINT}."
    echo "Detail: ${SPL_OUTPUT}"
    exit 2
}

ONCHAIN_BALANCE=$(echo "$SPL_OUTPUT" | jq -r '.amount') || {
    echo "ERROR: Failed to parse spl-token JSON output."
    echo "Raw output: ${SPL_OUTPUT}"
    exit 2
}

if [ -z "$ONCHAIN_BALANCE" ] || [ "$ONCHAIN_BALANCE" = "null" ]; then
    echo "ERROR: No token account found for owner ${ESCROW_OWNER}, mint ${MINT}."
    exit 2
fi

echo "On-chain balance (raw): ${ONCHAIN_BALANCE}"

# Step 2: Query database for expected balance (raw units)
echo "Querying database for expected balance..."
# -w fails instead of prompting when libpq skips the pgpass file; -X ignores ~/.psqlrc.
# psql does not interpolate :'mint' inside -c, so the query goes on stdin.
DB_EXPECTED=$(psql -X -w -t -A -v ON_ERROR_STOP=1 -v "mint=${MINT}" 2>&1 <<'SQL'
    SELECT
        COALESCE(SUM(CASE WHEN transaction_type = 'deposit' THEN amount ELSE 0 END), 0) -
        COALESCE(SUM(CASE WHEN transaction_type = 'withdrawal' THEN amount ELSE 0 END), 0)
        AS expected_balance
    FROM transactions
    WHERE mint = :'mint' AND status = 'completed';
SQL
) || {
    echo "ERROR: Failed to query database."
    echo "Detail: ${DB_EXPECTED}"
    exit 2
}
DB_EXPECTED=$(echo "$DB_EXPECTED" | tr -d '[:space:]')

echo "DB expected balance (raw): ${DB_EXPECTED}"

# Step 3: Compare (both in raw token units)
if ! [[ "$ONCHAIN_BALANCE" =~ ^[0-9]+$ ]]; then
    echo "ERROR: On-chain balance is not a valid integer: '${ONCHAIN_BALANCE}'"
    exit 2
fi
if ! [[ "$DB_EXPECTED" =~ ^-?[0-9]+$ ]]; then
    echo "ERROR: DB expected balance is not a valid integer: '${DB_EXPECTED}'"
    exit 2
fi

DELTA=$((ONCHAIN_BALANCE - DB_EXPECTED))

echo ""
echo "=== Result ==="
echo "On-chain: ${ONCHAIN_BALANCE}"
echo "Expected: ${DB_EXPECTED}"
echo "Delta:    ${DELTA}"

if [ "$DELTA" -eq 0 ]; then
    echo ""
    echo "PASS — Balances reconcile."
    exit 0
else
    echo ""
    echo "FAIL — Mismatch detected!"

    if [ -n "${ALERT_WEBHOOK:-}" ]; then
        PAYLOAD=$(jq -n \
            --arg text "Escrow balance mismatch! On-chain: ${ONCHAIN_BALANCE}, Expected: ${DB_EXPECTED}, Delta: ${DELTA}, Owner: ${ESCROW_OWNER}, Mint: ${MINT}" \
            --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            '{text: $text, timestamp: $ts}')
        HTTP_CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${ALERT_WEBHOOK}" \
            -H "Content-Type: application/json" \
            -d "$PAYLOAD") || {
            echo "ERROR: Alert webhook request failed (curl error)."
            exit 2
        }
        if [ "$HTTP_CODE" -lt 200 ] || [ "$HTTP_CODE" -ge 300 ]; then
            echo "ERROR: Alert webhook returned HTTP ${HTTP_CODE}."
            exit 2
        fi
        echo "Alert sent (HTTP ${HTTP_CODE})."
    fi

    exit 1
fi
