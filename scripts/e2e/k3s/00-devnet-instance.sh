#!/usr/bin/env bash
# Devnet prerequisite: fresh e2e keys funded from the temp-program authority (DEVNET_FUNDER,
# a keypair path on this machine), a fresh instance on the upgraded temp escrow program,
# the withdraw operator added, and a mint allowed. Records every signature.
. "$(dirname "$0")/lib.sh"
[[ $E2E_PROFILE == devnet ]] || die "devnet only"
progress "STEP $STEP ($E2E_PROFILE) devnet instance"
FUNDER="${DEVNET_FUNDER:?set DEVNET_FUNDER to the devnet funder keypair path}"
sync_repo
for k in escrow-admin user other; do ckeygen "$k"; done
# The bins read the provider RPC from the staged secrets, never from argv on this host.
DRPC='$(python3 -c "import yaml;print(yaml.safe_load(open(\"'$CTL_REPO'/private-channel-deploy/secrets.yml\"))[\"rpc_url\"])")'
for spec in admin:0.15 withdraw-operator:0.03 escrow-admin:0.06 user:0.04; do
  k="${spec%%:*}"; amt="${spec#*:}"; p="$(cpub "$k")"
  bal="$(solana -u devnet balance "$p" | awk '{print $1}')"
  if python3 -c "import sys; sys.exit(0 if float('$bal') >= float('$amt') else 1)"; then continue; fi
  out="$(solana -u devnet transfer -k "$FUNDER" --allow-unfunded-recipient "$p" "$amt" --with-compute-unit-price 1000 2>&1)" || die "funding $k failed: $out"
  sig "fund-$k" "$(awk '/Signature/ {print $2}' <<<"$out")"
done
sleep 20
if [[ -z "${INSTANCE:-}" ]]; then
  out="$(ctl "cd $CTL_REPO && $BINS/create_instance $DRPC $(key escrow-admin)")" || die "create_instance failed"
  inst="$(grep escrow_instance_id <<<"$out" | sed 's/.*"\(.*\)".*/\1/')"
  [[ -n "$inst" ]] || die "no instance id"
  save INSTANCE "$inst"
  sig create_instance "$(grep -o '[1-9A-HJ-NP-Za-km-z]\{80,\}' <<<"$out" | head -1)"
  sleep 20
  out="$(ctl "cd $CTL_REPO && $BINS/add_operator $DRPC $(key escrow-admin) $INSTANCE $OPERATOR_PUB")" || die "add_operator failed"
  sig add_operator "$(grep -o '[1-9A-HJ-NP-Za-km-z]\{80,\}' <<<"$out" | head -1)"
fi
ok "instance $INSTANCE, operator $OPERATOR_PUB added"
if [[ -z "${MINT:-}" ]]; then
  ckeygen mint
  ctl "spl-token create-token -u devnet --fee-payer $(key escrow-admin) --mint-authority $(cpub escrow-admin) $(key mint)" >>"$LOG" 2>&1 || die "create-token failed"
  save MINT "$(cpub mint)"
  sleep 15
  ata="$(ctl "spl-token create-account -u devnet --fee-payer $(key user) --owner $(cpub user) $MINT 2>&1" | awk '/Creating account/ {print $3}')"
  ctl "spl-token mint -u devnet --fee-payer $(key escrow-admin) --mint-authority $(key escrow-admin) $MINT 1 $ata" >>"$LOG" 2>&1 || die "mint-to failed"
  sleep 30
  out="$(ctl "cd $CTL_REPO && $BINS/allow_mint $DRPC $(key escrow-admin) $INSTANCE $MINT 1000 1000 0")" || die "allow_mint failed"
  sig allow_mint "$(grep -o '[1-9A-HJ-NP-Za-km-z]\{80,\}' <<<"$out" | head -1)"
fi
ok "mint $MINT allowed"
echo "escrow_instance_id: $INSTANCE" >"$RUN/e2e-devnet-instance.yml"
pass
