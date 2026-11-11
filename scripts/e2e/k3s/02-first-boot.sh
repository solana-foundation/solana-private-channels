#!/usr/bin/env bash
# Step 2 (localnet): first boot without workers, then the instance over a port-forward;
# the Operator PDA check refuses before add_operator (no side effect) and passes after;
# the redeploy with validator_reset=false starts the workers.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) localnet first boot"
sync_repo
for w in sts/indexer-solana deploy/operator-solana deploy/operator-private-channel; do
  hk -n $NS get "$w" >/dev/null 2>&1 && die "$w deployed without an instance"
done
hk -n $NS rollout status sts/indexer-private-channel --timeout=60s >>"$LOG" || die "indexer-private-channel not running"
ok "no instance: indexer-solana and operators absent, indexer-private-channel running"

[[ -z "$(hk -n $NS get pod validator-0 -o jsonpath='{.spec.securityContext.seccompProfile}{.spec.containers[0].securityContext.seccompProfile}')" ]] || die "validator has a seccomp profile"
sc="$(hk -n $NS exec validator-0 -- grep Seccomp: /proc/1/status | awk '{print $2}')"
[[ "$sc" == 0 ]] || die "validator process runs under seccomp mode $sc"
ok "validator runs with no seccomp profile (Seccomp: 0, so io_uring is allowed)"
iou="$(hk -n $NS exec validator-0 -- sh -c 'ls -l /proc/1/fd 2>/dev/null | grep -c io_uring || true')"
say "validator io_uring fds: $iou"
[[ "$iou" -gt 0 ]] && ok "the validator holds io_uring instances" || finding "validator holds no io_uring fd; solana-test-validator may not use io_uring on this kernel. Seccomp is off, which is what the plan requires."

connect_ctl
pf $NS svc/validator 18899:8899
pf $NS svc/validator 18900:8900
for k in escrow-admin user; do ckeygen "$k"; done
for k in escrow-admin user admin; do ctl "solana airdrop -u $LRPC 10 $(cpub $k) >/dev/null"; done
sleep 15
out="$(cbin create_instance $LRPC "$(key escrow-admin)")" || die "create_instance failed"
inst="$(grep escrow_instance_id <<<"$out" | sed 's/.*"\(.*\)".*/\1/')"
[[ -n "$inst" ]] || die "no instance id: $out"
save INSTANCE "$inst"
sig create_instance "$(grep -io 'signature[^A-Za-z0-9]*[1-9A-HJ-NP-Za-km-z]\{60,\}' <<<"$out" | grep -o '[1-9A-HJ-NP-Za-km-z]\{60,\}' | head -1)"
ok "instance $inst"

rev0="$(helm_revision)"
if playbook "$STEP-no-operator" k3s.yml -e reset_state=false -e validator_reset=false -e "escrow_instance_id=$inst"; then
  die "deploy passed without add_operator"
fi
grep -q 'No Operator PDA for operator' "$RUN/logs/$STEP-no-operator.log" || die "refused for another reason (logs/$STEP-no-operator.log)"
[[ "$(helm_revision)" == "$rev0" ]] || die "helm revision moved on a refused deploy"
hk -n $NS get deploy/operator-private-channel >/dev/null 2>&1 && die "an operator was deployed"
ok "PDA check refused before add_operator; revision $rev0 unchanged, no operator pod"

out="$(cbin add_operator $LRPC "$(key escrow-admin)" "$inst" "$OPERATOR_PUB")" || die "add_operator failed"
sig add_operator "$(grep -o '[1-9A-HJ-NP-Za-km-z]\{80,\}' <<<"$out" | head -1)"
sleep 15
playbook "$STEP-workers" k3s.yml -e reset_state=false -e validator_reset=false -e "escrow_instance_id=$inst" || die "deploy with the instance failed"
for w in sts/indexer-solana deploy/operator-solana deploy/operator-private-channel; do
  hk -n $NS rollout status "$w" --timeout=300s >>"$LOG" || die "$w not ready"
done
ok "workers started after add_operator"
for h in validator validator-fallback; do
  hk -n $NS exec sts/indexer-solana -c indexer-solana -- curl -fsS -m 5 -X POST "http://$h:8899" -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' | grep -q '"ok"' || die "$h does not answer from indexer-solana"
done
ok "validator and validator-fallback answer from indexer-solana"
[[ "$(hk -n $NS get cm pc-deploy-state -o jsonpath='{.data.COMMON_ESCROW_INSTANCE_ID}')" == "$inst" ]] || die "pc-deploy-state does not hold the instance"
ok "pc-deploy-state records the instance"
pass
