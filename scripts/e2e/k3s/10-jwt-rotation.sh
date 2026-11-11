#!/usr/bin/env bash
# Step 10: JWT rotation in two deploys. After the stage (rotate-secret.sh jwt_secret) a
# token issued before verifies and new logins work; after --finish the old token is
# rejected (401) and a fresh login works.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) JWT rotation"
sec="$RUN/e2e-$E2E_PROFILE-secrets.yml"
inst_args=(-e reset_state=false -e validator_reset=false)
[[ -n "${INSTANCE:-}" ]] && inst_args+=(-e "escrow_instance_id=$INSTANCE")
ctl "cat > /tmp/jwt.py" <<'PY'
import json, sys, urllib.request, urllib.error, uuid
host, mode = sys.argv[1], sys.argv[2]
def post(url, body, token=None):
    r = urllib.request.Request(url, json.dumps(body).encode(), {"content-type": "application/json"})
    if token: r.add_header("authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(r, timeout=15) as x: return x.status, json.loads(x.read() or b"null")
    except urllib.error.HTTPError as e: return e.code, None
if mode == "login":
    u, p = "jwt" + uuid.uuid4().hex[:8], uuid.uuid4().hex
    post(f"http://{host}:8903/auth/register", {"username": u, "password": p})
    print(post(f"http://{host}:8903/auth/login", {"username": u, "password": p})[1]["token"])
else:
    # A gated read any user role may make: the user role is refused getBlock (403), an
    # invalid token gets 401, so 403 proves the token verified.
    print(post(f"http://{host}:8899", {"jsonrpc": "2.0", "id": 1, "method": "getBlock", "params": [1]}, mode)[0])
PY
deploy() { sync_repo; playbook "$STEP-$1" k3s.yml "${inst_args[@]}" || die "deploy $1 failed"; }
check() { ctl "python3 /tmp/jwt.py $E2E_HOST_IP \$(cat /tmp/$1.tok)"; }
ctl "python3 /tmp/jwt.py $E2E_HOST_IP login > /tmp/old.tok"
[[ "$(check old)" == 403 ]] || die "a fresh token did not verify before the rotation"
bash "$REPO/scripts/rotate-secret.sh" jwt_secret --secrets-file "$sec" >>"$LOG" 2>&1 || die "stage failed"
grep -q '^jwt_secret_previous: .\+' "$sec" || die "stage did not set jwt_secret_previous"
deploy stage
c="$(check old)"; [[ "$c" == 403 ]] || die "old token rejected during the stage ($c)"
ctl "python3 /tmp/jwt.py $E2E_HOST_IP login > /tmp/new.tok"
[[ "$(check new)" == 403 ]] || die "a token issued during the stage did not verify"
ok "stage: the old token still verifies, new logins work"
bash "$REPO/scripts/rotate-secret.sh" jwt_secret --finish --secrets-file "$sec" >>"$LOG" 2>&1 || die "finish failed"
deploy finish
c="$(check old)"; [[ "$c" == 401 ]] || die "old token still accepted after finish ($c)"
[[ "$(check new)" == 403 ]] || die "the staged token stopped verifying after finish"
ok "finish: the old token gets 401, the current one verifies"
pass
