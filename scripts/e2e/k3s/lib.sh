#!/usr/bin/env bash
# Shared helpers for the k3s e2e steps; each step asserts, exits non-zero on failure and
# logs to $RUN. E2E_CTL and E2E_HOST are the commands that reach the control node and the
# host, so the same steps run against Docker test hosts or real VMs.
set -euo pipefail

E2E_PROFILE="${E2E_PROFILE:-local}"
RUN="${RUN:-${TMPDIR:-/tmp}/k3s-e2e-run}"
REPO="${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
E2E_CTL="${E2E_CTL:-docker exec -i k3s-e2e-ctl}"
E2E_HOST="${E2E_HOST:-docker exec -i k3s-e2e-$E2E_PROFILE}"
E2E_HOST_IP="${E2E_HOST_IP:-$([[ $E2E_PROFILE == local ]] && echo 172.30.77.10 || echo 172.30.77.11)}"
E2E_INV_HOST="${E2E_INV_HOST:-k3s-e2e-$E2E_PROFILE}"
E2E_GROUP="${E2E_GROUP:-$([[ $E2E_PROFILE == local ]] && echo dev || echo devnet)}"
NS=private-channel
# The devnet profile deploys from a copy with the temporary devnet program ids.
E2E_SRC="${E2E_SRC:-$([[ $E2E_PROFILE == local ]] && echo /work/repo || echo /work/run/devnet-src)}"
CTL_REPO="${CTL_REPO:-$([[ $E2E_PROFILE == local ]] && echo /root/repo || echo /root/repo-devnet)}"
STEP="${STEP:-$(basename "$0" .sh)}"
# Logs and progress lines of the two profiles never collide.
[[ $E2E_PROFILE == local || $STEP == "$E2E_PROFILE-"* ]] || STEP="$E2E_PROFILE-$STEP"
LOG="$RUN/logs/e2e-$E2E_PROFILE-$STEP.log"
mkdir -p "$RUN/logs"

ts() { date -u +%FT%TZ; }
progress() { echo "$(ts) $*" >>"$RUN/progress.log"; }
say() { echo "$(ts) [$STEP] $*" | tee -a "$LOG"; }
finding() { printf -- '- %s [%s %s]: %s\n' "$(ts)" "$E2E_PROFILE" "$STEP" "$*" >>"$RUN/FINDINGS.md"; }
die() { say "FAIL: $*"; progress "FAIL $STEP ($E2E_PROFILE) $*"; exit 1; }
ok() { say "ok: $*"; }
pass() { say "PASS"; progress "PASS $STEP ($E2E_PROFILE) ${*:-}"; }
blocked() { say "BLOCKED: $*"; progress "BLOCKED $STEP ($E2E_PROFILE) $*"; exit 3; }

ctl() { $E2E_CTL bash -lc "$*"; }
onhost() { $E2E_HOST bash -lc "$*"; }
# Admin kubectl on the host: for test assertions only, never what the deploy uses.
hk() { $E2E_HOST k3s kubectl "$@"; }
# Records a transaction signature for the report.
sig() { echo "$(ts) $E2E_PROFILE $STEP $1 $2" >>"$RUN/signatures.log"; say "signature $1: $2"; }

# Copies the worktree into the control node (the repo mount is read-only there) and
# writes the inventory, the e2e secrets and the e2e vars next to the playbook.
sync_repo() {
  [[ "$E2E_SRC" != /work/repo ]] && "$RUN/devnet-sync.sh" >/dev/null
  ctl "mkdir -p $CTL_REPO && rsync -a --delete --exclude .git --exclude target --exclude 'private-channel-deploy/secrets.yml' \
      --exclude 'private-channel-deploy/inventory.ini' --exclude 'private-channel-deploy/keypairs' --exclude keypairs \
      $E2E_SRC/ $CTL_REPO/"
  if [[ $E2E_PROFILE == local && -d "$REPO/target/deploy" ]]; then
    ctl "mkdir -p $CTL_REPO/target/deploy && cp /work/repo/target/deploy/*.so $CTL_REPO/target/deploy/"
  fi
  # The repo ships only vars/devnet.example.yml; the e2e vars fill its placeholders.
  [[ $E2E_PROFILE == local ]] || ctl "cp $CTL_REPO/private-channel-deploy/vars/devnet.example.yml $CTL_REPO/private-channel-deploy/vars/devnet.yml"
  ctl "cat > $CTL_REPO/private-channel-deploy/inventory.ini" <<INV
[$E2E_GROUP]
$E2E_INV_HOST ansible_host=$E2E_HOST_IP ansible_user=root ansible_ssh_private_key_file=/root/.ssh/id_ed25519 e2e_host=true
INV
  local sec="$RUN/e2e-$E2E_PROFILE-secrets.yml"
  if [[ ! -f "$sec" ]]; then
    ( umask 077
      for k in postgres_password postgres_replication_password postgres_runtime_password postgres_auth_runtime_password \
        postgres_auth_owner_password postgres_gateway_password postgres_monitoring_password postgres_grafana_password \
        postgres_indexer_password grafana_admin_password; do
        echo "$k: e2e$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
      done
      echo "jwt_secret: e2e$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
      echo "ghcr_user: e2e"
      echo "ghcr_token: unused"
    ) >"$sec"
  fi
  ctl "umask 077; cat > $CTL_REPO/private-channel-deploy/secrets.yml" <"$sec"
  # Provider RPC and Yellowstone credentials (devnet), kept apart from step overrides.
  if [[ -f "$RUN/e2e-$E2E_PROFILE-secrets.rpc.yml" ]]; then
    ctl "cat >> $CTL_REPO/private-channel-deploy/secrets.yml" <"$RUN/e2e-$E2E_PROFILE-secrets.rpc.yml"
  fi
  if [[ -f "$RUN/e2e-$E2E_PROFILE-secrets.extra.yml" ]]; then
    ctl "cat >> $CTL_REPO/private-channel-deploy/secrets.yml" <"$RUN/e2e-$E2E_PROFILE-secrets.extra.yml"
  fi
  # Fixed keys per profile, so the instance and the delegate survive redeploys.
  ctl "mkdir -p $CTL_REPO/keypairs && cd $CTL_REPO/keypairs && for k in admin withdraw-operator; do
         [ -f \$k.json ] || solana-keygen new --no-bip39-passphrase --silent --outfile \$k.json >/dev/null; done"
  ADMIN_PUB="$(ctl "solana-keygen pubkey $CTL_REPO/keypairs/admin.json")"
  export OPERATOR_PUB ADMIN_PUB
  OPERATOR_PUB="$(ctl "solana-keygen pubkey $CTL_REPO/keypairs/withdraw-operator.json")"
  { cat "$RUN/e2e-$E2E_PROFILE-vars.yml"; echo "permanent_delegate: $ADMIN_PUB"
    cat "$RUN/e2e-$E2E_PROFILE-instance.yml" 2>/dev/null || true
    # After a PITR drill the env keeps its recovered Cluster names, as the runbook says.
    cat "$RUN/e2e-$E2E_PROFILE-pitr.yml" 2>/dev/null || true; } \
    | ctl "cat > $CTL_REPO/private-channel-deploy/e2e-vars.yml"
}

# Test hosts are containers whose / is a private mount; node-exporter's rootfs mount
# needs it shared, as it is on a real VM. Persisted as a unit so a restart keeps it.
prepare_host() {
  onhost "cat > /etc/systemd/system/pc-e2e-rshared.service <<'U'
[Unit]
Description=e2e only: make / a shared mount like a real VM
DefaultDependencies=no
Before=k3s.service
[Service]
Type=oneshot
ExecStart=/bin/mount --make-rshared /
[Install]
WantedBy=multi-user.target
U
systemctl daemon-reload && systemctl enable --now pc-e2e-rshared.service"
}

# Runs a playbook on the control node; output goes to $RUN/logs/<name>.log.
playbook() {
  local name="$1"
  shift
  say "ansible-playbook $* (log: logs/$name.log)"
  # PB_ENV: extra env evaluated on the control node (e.g. a token read from a file there).
  ctl "cd $CTL_REPO/private-channel-deploy && ${PB_ENV:-} ANSIBLE_LOG_PATH=/work/run/logs/$name.ansible.log \
      ansible-playbook -l $E2E_INV_HOST -e @e2e-vars.yml $*" >"$RUN/logs/$name.log" 2>&1
}

# Value of one e2e secret, for asserts that must not print it.
# YAML-parsed: rotate-secret.sh writes quoted values.
e2e_secret() { python3 -c 'import sys, yaml; print(yaml.safe_load(open(sys.argv[1])).get(sys.argv[2]) or "")' "$RUN/e2e-$E2E_PROFILE-secrets.yml" "$1"; }

# Waits until a command succeeds: wait_for <seconds> <cmd...>
wait_for() {
  local t="$1"
  shift
  local end=$((SECONDS + t))
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < end)) || return 1
    sleep 3
  done
}

helm_revision() { onhost "KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm -n $NS status private-channel -o json 2>/dev/null | jq -r .version" || echo none; }

# Deployer access from the control node, for the bins and port-forwards a human would use.
connect_ctl() {
  local out
  out="$(ctl "cd $CTL_REPO && scripts/k3s-connect.sh $E2E_INV_HOST -e @$CTL_REPO/private-channel-deploy/e2e-vars.yml")" || die "k3s-connect.sh failed"
  KC="$(sed -n 's/^export KUBECONFIG=//p' <<<"$out")"
  [[ -n "$KC" ]] || die "no kubeconfig"
  export KC
}
# pf <ns> <target> <local:remote>: a port-forward on the control node that lives past this call.
pf() {
  local s="$CTL_REPO/private-channel-deploy/scripts/k3s-port-forward.sh" pidf="/tmp/pf-${3%%:*}.pid"
  ctl "$s stop $pidf; KUBECONFIG=$KC $s start $1 $2 $3 $pidf"
}
BINS="$E2E_SRC/target/debug"
export LRPC=http://127.0.0.1:18899
# Runs a scripts/devnet bin on the control node.
cbin() { ctl "cd $CTL_REPO && $BINS/$*"; }
key() { echo "$CTL_REPO/keypairs/$1.json"; }
cpub() { ctl "solana-keygen pubkey $(key "$1")"; }
ckeygen() { ctl "[ -f $(key "$1") ] || solana-keygen new --no-bip39-passphrase --silent --outfile $(key "$1") >/dev/null"; }
STATE="$RUN/e2e-$E2E_PROFILE-state.env"
touch "$STATE"
# shellcheck disable=SC1090
. "$STATE"
save() { printf '%s=%q\n' "$1" "$2" >>"$STATE"; eval "$1=\$2"; }
