#!/usr/bin/env bash
# Step 16: on the node's 8899/8903 from off-host, RBAC answers as on compose (401, 403,
# 200 after a wallet link), and auth's per-IP limit keys on each client's own IP: one client
# exhausting it leaves a second client unaffected (the Q6 go-live gate).
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) public path and per-IP limits"
ckeygen user; ckeygen other
ctl "cp /work/repo/scripts/e2e/k3s/auth_flow.py /tmp/auth_flow.py"
[[ -n "${MINT:-}" ]] || die "no MINT (run step 17 first)"
ata() { ctl "solana find-program-derived-address ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL pubkey:$1 pubkey:TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb pubkey:$MINT"; }
out="$(ctl "python3 /tmp/auth_flow.py $E2E_HOST_IP $(key user) $(ata "$(cpub user)") $(ata "$(cpub other)")")" || die "auth flow failed"
say "auth flow: $out"
r() { jq -r ".results.$1" <<<"$out"; }
fails=0
want() { if [[ "$(r "$1")" == "$2" ]]; then say "ok $1 = $2"; else say "WRONG $1 = $(r "$1"), want $2"; fails=$((fails + 1)); fi; }
want register 200; want login 200; want no_token_getBlock 401; want no_token_getAccountInfo 401; want user_getBlock 403
want link 200; want linked_own_balance 200; want linked_other_balance 403; want bad_token 401
(( fails <= 0 )) || die "$fails public-path codes wrong"
ok "RBAC on the public path matches compose"

# Per-IP: exhaust auth's per-IP rate limit (5/s, burst 10) from the control node, with a
# fresh username per request so the per-username limit is not what trips; then the
# second client must still get a normal answer in the same second.
burst="$(ctl "for i in \$(seq 1 40); do curl -s -o /dev/null -w '%{http_code}\n' -X POST http://$E2E_HOST_IP:8903/auth/login -H 'content-type: application/json' -d '{\"username\":\"burst'\$i'\",\"password\":\"x\"}' & done; wait" | sort | uniq -c | xargs)"
c2="$(docker exec -i k3s-e2e-client2 curl -s -o /dev/null -w '%{http_code}' -X POST "http://$E2E_HOST_IP:8903/auth/login" -H 'content-type: application/json' -d "{\"username\":\"client2$RANDOM\",\"password\":\"x\"}")"
say "control node burst codes: $burst; client2 right after: $c2"
[[ "$burst" == *429* ]] || die "the burst never hit the per-IP limit ($burst)"
[[ "$c2" != 429 ]] || die "client2 was throttled by the control node's burst: the services see one shared IP (Q6 gate fails here)"
ok "per-IP limit keyed by the real client IP (client2 got $c2 while the control node was throttled)"
seen="$(hk -n $NS logs deploy/auth --since=2m 2>/dev/null | grep -oE '172\.30\.77\.(20|21)' | sort -u | xargs || true)"
say "client IPs in auth logs: ${seen:-none logged}"
finding "APPROXIMATED: step 16 runs on a Docker bridge where the node IP is on the host NIC (the case ServiceLB preserves); the NAT'd cloud VM case of the Q6 gate still needs the real host type."
pass
