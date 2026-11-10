#!/usr/bin/env bash
# Opens ad hoc access to a k3s deploy target: a deployer token (1h, namespace-scoped)
# through an SSH tunnel. Prints the export line; close with the printed ssh -O exit.
#   scripts/k3s-connect.sh <inventory host> [extra ansible-playbook args]
set -euo pipefail

host="${1:?usage: k3s-connect.sh <inventory host> [ansible args]}"
shift
cd "$(dirname "${BASH_SOURCE[0]}")/../private-channel-deploy"
out="$(ansible-playbook k3s-connect.yml -l "$host" "$@" 2>&1)" || { echo "$out" >&2; exit 1; }
line="$(grep -o 'PC_KUBECONFIG=[^"]*' <<<"$out" | tail -1)"
[[ -n "$line" ]] || { echo "$out" >&2; echo "no kubeconfig printed" >&2; exit 1; }
kc="${line#PC_KUBECONFIG=}"; kc="${kc%% *}"
sock="${line##*PC_TUNNEL_SOCKET=}"
echo "export KUBECONFIG=$kc"
echo "# when done: ssh -S $sock -O exit pc-k3s; rm -rf $(dirname "$kc")"
