#!/usr/bin/env bash
# shellcheck disable=SC2090
# Step 17 (subset): deposit, mint on the channel, withdraw, release on Solana, through
# the k3s stack. Records every signature. Needs step 2 (instance and operator).
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) on-chain flows"
OPERATOR_PUB="$(cpub withdraw-operator)"
[[ -n "${INSTANCE:-}" ]] || die "no instance (run step 2 first)"
connect_ctl
if [[ $E2E_PROFILE == local ]]; then
  pf $NS svc/validator 18899:8899
  pf $NS svc/validator 18900:8900
else
  # The provider RPC stays in the staged secrets; it is expanded on the control node only.
  # shellcheck disable=SC2089,SC2090
  LRPC='$(python3 -c "import yaml;print(yaml.safe_load(open(\"'$CTL_REPO'/private-channel-deploy/secrets.yml\"))[\"rpc_url\"])")'
fi
pf $NS svc/read-node 18901:8901
GW="http://$E2E_HOST_IP:8899"
idx() { hk -n $NS exec "$(hk -n $NS get pods -l private-channel.io/db=indexer,cnpg.io/instanceRole=primary -o name)" -c postgres -- psql -U postgres -d indexer -tAc "$1"; }
count() { idx "SELECT count(*) FROM transactions WHERE transaction_type='$1' AND status='completed'"; }
ckeygen mint
if [[ -z "${MINT:-}" ]]; then
  ctl "spl-token create-token -u $LRPC --fee-payer $(key escrow-admin) --mint-authority $(cpub escrow-admin) $(key mint)" >>"$LOG"
  save MINT "$(cpub mint)"
  ata="$(ctl "spl-token create-account -u $LRPC --fee-payer $(key user) --owner $(cpub user) $MINT 2>&1" | awk '/Creating account/ {print $3}')"
  ctl "spl-token mint -u $LRPC --fee-payer $(key escrow-admin) --mint-authority $(key escrow-admin) $MINT 1 $ata" >>"$LOG"
  wait_for 120 bash -c "$E2E_CTL bash -lc 'solana account -u $LRPC --commitment finalized $MINT'" || die "mint not finalized"
  out="$(cbin allow_mint $LRPC "$(key escrow-admin)" "$INSTANCE" "$MINT" 1000 1000 0)" || die "allow_mint failed"
  sig allow_mint "$(grep -o '[1-9A-HJ-NP-Za-km-z]\{80,\}' <<<"$out" | head -1)"
  sleep 20
fi
d0="$(count deposit)"
out="$(cbin deposit $LRPC "$(key user)" "$INSTANCE" "$MINT" 100000)" || die "deposit failed"
sig deposit "$(grep -o '[1-9A-HJ-NP-Za-km-z]\{80,\}' <<<"$out" | head -1)"
wait_for 300 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS exec \$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=indexer,cnpg.io/instanceRole=primary -o name) -c postgres -- psql -U postgres -d indexer -tAc \"SELECT count(*) FROM transactions WHERE transaction_type='deposit' AND status='completed'\") -gt $d0 ]]" \
  || die "deposit not completed (indexer + operator-solana mint)"
mint_sig="$(idx "SELECT counterpart_signature FROM transactions WHERE transaction_type='deposit' AND status='completed' ORDER BY id DESC LIMIT 1")"
sig channel_mint "$mint_sig"
ok "deposit completed and minted on the channel"
w0="$(count withdrawal)"
out="$(ctl "cd $CTL_REPO && PRIVATE_CHANNEL_READ_RPC_URL=http://127.0.0.1:18901 $BINS/withdraw $GW $(key user) $MINT 5000")" || die "withdraw failed"
sig channel_burn "$(grep 'Transaction signature' <<<"$out" | awk '{print $NF}' | tail -1)"
wait_for 360 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS exec \$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=indexer,cnpg.io/instanceRole=primary -o name) -c postgres -- psql -U postgres -d indexer -tAc \"SELECT count(*) FROM transactions WHERE transaction_type='withdrawal' AND status='completed'\") -gt $w0 ]]" \
  || die "withdrawal not released"
rel="$(idx "SELECT counterpart_signature FROM transactions WHERE transaction_type='withdrawal' AND status='completed' ORDER BY id DESC LIMIT 1")"
sig release "$rel"
signers="$(ctl "curl -s -X POST $LRPC -H 'content-type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransaction\",\"params\":[\"$rel\",{\"encoding\":\"json\",\"maxSupportedTransactionVersion\":0}]}'" \
  | jq -r '.result.transaction.message as $m | $m.accountKeys[:$m.header.numRequiredSignatures] | join(" ")')"
[[ " $signers " == *" $OPERATOR_PUB "* ]] || die "release not signed by the operator key ($signers)"
ok "withdrawal released on Solana, signed by the operator key ($OPERATOR_PUB), signers: $signers"
pass
