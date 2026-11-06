#!/usr/bin/env bash
set -euo pipefail

# Upgrade path on localnet: a stack deployed by the pre-change playbook (admin key is
# the operator, bootstrap wrote a GHCR entry) moves to the secrets-managed deploy.
#   1. deploy the old playbook (E2E_OLD_REPO_ROOT, or `git archive` of E2E_OLD_REF,
#      default: the merge-base with origin/main);
#   2. the Operator PDA check refuses a new operator key before add_operator;
#   3. add_operator, then the new playbook succeeds;
#   4. data survives, the old GHCR entry is removed, a personal entry is kept;
#   5. withdrawals work with the new operator; 6. remove_operator(admin).
# Runs every ansible and docker CLI call with HOME=$E2E_WORK/home, so the user's
# ~/.docker/config.json is never touched. Knobs: see scripts/e2e/e2e-lib.sh.

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/e2e/e2e-lib.sh
. "$REPO/scripts/e2e/e2e-lib.sh"
mkdir -p "$E2E_WORK/home/.docker"
export HOME="$E2E_WORK/home"

OLD="${E2E_OLD_REPO_ROOT:-}"
if [[ -z "$OLD" ]]; then
  # The pre-change playbook: where this branch left main, unless E2E_OLD_REF says otherwise.
  ref="${E2E_OLD_REF:-$(git -C "$REPO" merge-base HEAD origin/main)}"
  if [[ "$(git -C "$REPO" rev-parse "$ref^{commit}")" == "$(git -C "$REPO" rev-parse HEAD)" ]]; then
    echo "ERROR: $ref is HEAD, so the old phase would deploy the new playbook. Set E2E_OLD_REF or E2E_OLD_REPO_ROOT." >&2
    exit 2
  fi
  OLD="$E2E_WORK/old-repo"
  mkdir -p "$OLD"
  git -C "$REPO" archive "$ref" | tar -x -C "$OLD"
  mkdir -p "$OLD/target/deploy"
  cp "$E2E_REPO_ROOT"/target/deploy/*.so "$OLD/target/deploy/"
fi

# old_deploy <log> [args]: the pre-change playbook, admin key doubling as operator.
old_deploy() {
  local log="$1"
  shift
  local reset=()
  [[ "${E2E_SKIP_RESET:-0}" == 1 ]] && reset=(--skip-tags reset_state -e reset_state=false)
  rm -rf "$E2E_WORK/old-deploy"
  cp -r "$OLD/private-channel-deploy" "$E2E_WORK/old-deploy"
  cp "$E2E_WORK/deploy/secrets.yml" "$E2E_WORK/old-deploy/secrets.yml"
  (cd "$E2E_WORK/old-deploy" && ANSIBLE_CONFIG="$E2E_WORK/old-deploy/ansible.cfg" ansible-playbook -i localhost, -c local deploy.yml \
    -e env=dev -e "repo_root=$OLD" -e "host_data_dir=$E2E_HOST_DATA_DIR" \
    -e "image_registry=$E2E_REGISTRY" -e "image_tag=$E2E_TAG" -e "admin_keypair_path=$E2E_WORK/keys/admin.json" \
    -e "permanent_delegate=$(pub delegate)" "${reset[@]}" "$@") >"$E2E_WORK/logs/$log.log" 2>&1
}

ghcr_user_in_config() {
  python3 -c 'import base64, json, os, sys
p = os.path.expanduser("~/.docker/config.json")
a = json.load(open(p)).get("auths", {}).get("ghcr.io", {}).get("auth", "") if os.path.exists(p) else ""
print(base64.b64decode(a).decode().split(":", 1)[0] if a else "")'
}

say "upgrade step 1: deploy the pre-change playbook"
prepare_deploy_copy
for k in admin operator escrow-admin user delegate; do keygen "$k"; done
write_secrets
sed -i "s/^ghcr_user: .*/ghcr_user: e2e-deploy-bot/; s/^ghcr_token: .*/ghcr_token: e2e-not-a-real-token/" "$E2E_WORK/deploy/secrets.yml"
old_deploy old1 || die "old deploy failed (logs/old1.log)"
for k in admin escrow-admin user; do airdrop "$k"; done
out="$(bin create_instance "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json")"
save INSTANCE "$(echo "$out" | grep escrow_instance_id | sed 's/.*"\(.*\)".*/\1/')"
bin add_operator "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" "$(pub admin)" >>"$E2E_WORK/logs/txs.log" 2>&1
old_deploy old2 -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false || die "old deploy 2 failed"
setup_mint
sig="$(deposit_and_withdraw)"
check "old stack: the admin key signs releases" grep -qw "$(pub admin)" <<<"$(tx_signers "$LOCAL_RPC" "$sig")"
check "old bootstrap left a GHCR entry" test "$(ghcr_user_in_config)" = e2e-deploy-bot
rows_before="$(psql_indexer -c 'SELECT count(*) FROM transactions')"

say "upgrade step 2: the Operator PDA check refuses an operator never added"
# A localnet preflight checks the PDA on the local validator once an instance is set.
pda_check() {
  (cd "$E2E_WORK/deploy" && ansible-playbook -i localhost, -c local deploy.yml --tags remote_checks \
    -e env=dev -e "repo_root=$E2E_REPO_ROOT" -e "host_data_dir=$E2E_HOST_DATA_DIR" \
    -e "rpc_url=$LOCAL_RPC" -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false \
    -e "image_registry=$E2E_REGISTRY" -e "admin_keypair_path=$E2E_WORK/keys/admin.json" \
    -e "operator_keypair_path=$E2E_WORK/keys/operator.json" -e "permanent_delegate=$(pub delegate)") \
    >"$E2E_WORK/logs/$1.log" 2>&1
}
if pda_check pda-before; then die "PDA check passed before add_operator"; fi
check "refused naming the new operator" grep -q "No Operator PDA for operator $(pub operator)" "$E2E_WORK/logs/pda-before.log"

say "upgrade step 3: add_operator, then the new playbook"
bin add_operator "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" "$(pub operator)" >>"$E2E_WORK/logs/txs.log" 2>&1
check "PDA check passes after add_operator" pda_check pda-after
deploy new1 -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false || die "new deploy failed (logs/new1.log)"

say "upgrade step 4: data, GHCR entry"
check "indexer rows survived" test "$(psql_indexer -c 'SELECT count(*) FROM transactions')" -ge "$rows_before"
check "the old GHCR entry is gone" test -z "$(ghcr_user_in_config)"
python3 -c 'import base64, json, os
p = os.path.expanduser("~/.docker/config.json")
c = json.load(open(p))
c.setdefault("auths", {})["ghcr.io"] = {"auth": base64.b64encode(b"someone-else:personal").decode()}
json.dump(c, open(p, "w"))'
deploy new2 -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false || die "new deploy 2 failed"
check "a personal GHCR entry is kept" test "$(ghcr_user_in_config)" = someone-else

say "upgrade step 5: withdrawals work with the new operator"
sig="$(deposit_and_withdraw)"
check "the new operator signs releases" grep -qw "$(pub operator)" <<<"$(tx_signers "$LOCAL_RPC" "$sig")"

say "upgrade step 6: remove_operator(admin)"
wait_for 120 "every withdrawal settled" no_unsettled_withdrawal
DATABASE_URL="$(indexer_db_url)" bin remove_operator "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" "$(pub admin)" \
  >>"$E2E_WORK/logs/txs.log" 2>&1 || die "remove_operator failed"
pda="$(solana find-program-derived-address "$ESCROW_PROGRAM_ID" string:operator "pubkey:$INSTANCE" "pubkey:$(pub admin)")"
check "the admin's Operator PDA is closed" bash -c "curl -s -X POST $LOCAL_RPC -H 'Content-Type: application/json' \
  -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"$pda\"]}' | grep -q '\"value\":null'"
say "upgrade e2e passed (work dir $E2E_WORK)"
