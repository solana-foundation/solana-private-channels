#!/usr/bin/env bash
# Step 11 (Privy part, localnet): the withdraw operator signs through Privy. The deploy is
# refused before add_operator for the Privy wallet, passes after, and a release is signed
# by the Privy wallet and paid by the channel admin. The app secret is piped into the
# control node's deploy copy only and removed after; it is never printed or kept here.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) Privy operator signer"
creds="${E2E_CREDS_FILE:-}"
[[ -n "$creds" && -f "$creds" ]] || blocked "set E2E_CREDS_FILE to the throwaway test creds env file"
[[ -n "${INSTANCE:-}" && -n "${MINT:-}" ]] || die "needs steps 2 and 17"
pub="$(grep -E '^OPERATOR_PRIVY_PUBKEY=' "$creds" | cut -d= -f2)"
args=(-e reset_state=false -e validator_reset=false -e "escrow_instance_id=$INSTANCE" -e operator_signer=privy
  -e "operator_pubkey=$pub"
  -e "operator_privy_app_id=$(grep -E '^PRIVY_APP_ID=' "$creds" | cut -d= -f2)"
  -e "operator_privy_wallet_id=$(grep -E '^OPERATOR_PRIVY_WALLET_ID=' "$creds" | cut -d= -f2)")
# shellcheck source=/dev/null
add_secret() {
  ( set -a; . "$creds"; set +a; printf "operator_privy_app_secret: '%s'\n" "$PRIVY_APP_SECRET" ) \
    | ctl "cat >> $CTL_REPO/private-channel-deploy/secrets.yml"
}
cleanup() { sync_repo >/dev/null 2>&1 || true; }
trap cleanup EXIT
sync_repo; add_secret
rev0="$(helm_revision)"
if playbook "$STEP-before-add" k3s.yml "${args[@]}"; then die "deploy passed before add_operator for the Privy wallet"; fi
grep -q 'No Operator PDA for operator' "$RUN/logs/$STEP-before-add.log" || die "refused for another reason"
[[ "$(helm_revision)" == "$rev0" ]] || die "revision moved on a refused deploy"
ok "refused before add_operator for $pub"
connect_ctl
pf $NS svc/validator 18899:8899
pf $NS svc/validator 18900:8900
out="$(cbin add_operator $LRPC "$(key escrow-admin)" "$INSTANCE" "$pub")" || die "add_operator failed"
sig add_operator_privy "$(grep -o '[1-9A-HJ-NP-Za-km-z]\{80,\}' <<<"$out" | head -1)"
ctl "solana airdrop -u $LRPC 2 $pub >/dev/null" || true
sleep 15
sync_repo; add_secret
playbook "$STEP-privy" k3s.yml "${args[@]}" || die "deploy with the Privy operator failed"
hk -n $NS get deploy operator-private-channel -o json | jq -r '.spec.template.spec.containers[0].env[] | select(.name|test("^OPERATOR_")) | .name' | sort | tr '\n' ' ' | tee -a "$LOG"; echo
pf $NS svc/read-node 18901:8901
idx() { hk -n $NS exec "$(hk -n $NS get pods -l private-channel.io/db=indexer,cnpg.io/instanceRole=primary -o name)" -c postgres -- psql -U postgres -d indexer -tAc "$1"; }
w0="$(idx "SELECT count(*) FROM transactions WHERE transaction_type='withdrawal' AND status='completed'")"
out="$(ctl "cd $CTL_REPO && PRIVATE_CHANNEL_READ_RPC_URL=http://127.0.0.1:18901 $BINS/withdraw http://$E2E_HOST_IP:8899 $(key user) $MINT 2000")" || die "withdraw failed"
sig channel_burn_privy "$(grep 'Transaction signature' <<<"$out" | awk '{print $NF}' | tail -1)"
wait_for 360 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS exec \$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=indexer,cnpg.io/instanceRole=primary -o name) -c postgres -- psql -U postgres -d indexer -tAc \"SELECT count(*) FROM transactions WHERE transaction_type='withdrawal' AND status='completed'\") -gt $w0 ]]" \
  || die "release through Privy did not complete"
rel="$(idx "SELECT counterpart_signature FROM transactions WHERE transaction_type='withdrawal' AND status='completed' ORDER BY id DESC LIMIT 1")"
sig release_privy "$rel"
signers="$(ctl "curl -s -X POST $LRPC -H 'content-type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransaction\",\"params\":[\"$rel\",{\"encoding\":\"json\",\"maxSupportedTransactionVersion\":0}]}'" \
  | jq -r '.result.transaction.message as $m | $m.accountKeys[:$m.header.numRequiredSignatures] | join(" ")')"
[[ " $signers " == *" $pub "* ]] || die "release not signed by the Privy wallet ($signers)"
ok "release signed by the Privy operator wallet $pub (signers: $signers)"
# Back to the memory operator; the Privy secret leaves the control node with the resync.
sync_repo
playbook "$STEP-revert" k3s.yml -e reset_state=false -e validator_reset=false -e "escrow_instance_id=$INSTANCE" || die "revert deploy failed"
pass
