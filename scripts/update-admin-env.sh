#!/usr/bin/env bash
set -euo pipefail

# Takes the CHANNEL admin keypair, the service identity the containers run as.
# Never pass the escrow Instance.admin keypair here: sharing one key across both
# roles means SetNewAdmin revokes nothing, because the rotated-out key keeps its
# receipt-mint authority and its Operator PDA.
#
# Public admin key goes to the tracked template; the private key goes only to a
# gitignored runtime env file, so `make build-*` never puts a live key in git.

# --operator-keypair also writes the withdraw operator's own release key.
operator_keypair=""
if [[ "${1:-}" == --operator-keypair ]]; then
  operator_keypair="${2:-}"
  shift 2
fi

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: $0 [--operator-keypair <path>] <env-file> <channel-admin-keypair-path> [runtime-env-file]" >&2
  exit 1
fi

env_file="$1"
admin_keypair="$2"
# Defaults to the gitignored `.env`, loaded last in the compose env-file chain.
runtime_env_file="${3:-.env}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

admin_pubkey="$(solana-keygen pubkey "$admin_keypair")"
admin_private_key="$(tr -d '\n' < "$admin_keypair")"

if [[ -n "$operator_keypair" ]]; then
  operator_pubkey="$(solana-keygen pubkey "$operator_keypair")"
  if [[ "$operator_pubkey" == "$admin_pubkey" ]]; then
    echo "ERROR: the operator keypair is the channel admin key ($admin_pubkey); use a separate one." >&2
    exit 1
  fi
fi

# Public key -> tracked template (safe to commit).
"$script_dir/upsert-env.sh" "$env_file" "PRIVATE_CHANNEL_ADMIN_KEYS" "$admin_pubkey"

# The node needs an explicit delegate. A blank one gets the admin pubkey once; a set one
# is kept, so replacing the admin key later never silently changes the policed delegate.
delegate=""
if [[ -f "$env_file" ]]; then
  delegate="$(awk -F= '/^PRIVATE_CHANNEL_PERMANENT_DELEGATE=/ { v = $2 } END { sub(/[ \t#].*/, "", v); print v }' "$env_file")"
fi
if [[ -z "$delegate" ]]; then
  "$script_dir/upsert-env.sh" "$env_file" "PRIVATE_CHANNEL_PERMANENT_DELEGATE" "$admin_pubkey"
  echo "Set PRIVATE_CHANNEL_PERMANENT_DELEGATE=$admin_pubkey in $env_file"
fi

# Private key -> gitignored runtime file only, on stdin via the printf builtin
# so it never sits in a child argv.
printf '%s' "$admin_private_key" | "$script_dir/upsert-env.sh" "$runtime_env_file" "ADMIN_PRIVATE_KEY"

if [[ -n "$operator_keypair" ]]; then
  tr -d '\n' < "$operator_keypair" | "$script_dir/upsert-env.sh" "$runtime_env_file" "OPERATOR_PRIVATE_KEY"
  echo "Wrote OPERATOR_PRIVATE_KEY ($operator_pubkey) to $runtime_env_file (gitignored)"
fi

echo "Updated $env_file with PRIVATE_CHANNEL_ADMIN_KEYS=$admin_pubkey"
echo "Wrote ADMIN_PRIVATE_KEY to $runtime_env_file (gitignored)"
