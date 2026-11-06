#!/usr/bin/env bash
set -euo pipefail

# End-to-end check of the secrets-managed deploy on a localnet stack on this machine:
# deploy, no-leak, permissions, withdraw with a separate operator key, Postgres,
# superuser/replicator and JWT rotation, fail-closed entrypoint, operator and
# channel-admin rotation, both signers on an https Vault, teardown. Each step asserts and exits non-zero.
# Needs docker, ansible, solana CLI + spl-token, and the scripts/devnet bins built
# (cargo build -p devnet-scripts --bins). Knobs: see scripts/e2e/e2e-lib.sh, plus
#   E2E_STEPS="1 2 ..."     steps to run (state is kept in E2E_WORK between runs)
#   E2E_OLD_IMAGE           an app image without the with-secrets entrypoint (step 8)
#   E2E_TEARDOWN_ARGS       extra args for teardown.yml (step 12)
# Step 11 runs hashicorp/vault:1.17 with -dev-tls and builds an e2e-only app image that
# trusts its throwaway CA.
# Usage: E2E_REGISTRY=127.0.0.1:5000/me E2E_TAG=dev scripts/e2e/deploy-secrets-e2e.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/e2e/e2e-lib.sh
. "$REPO/scripts/e2e/e2e-lib.sh"

# Grafana reads its DB password and webhook URL from secret files ($__file{...}):
# the datasource must reach Postgres and the contact point must hold the current URL.
grafana_checks() {
  local g="${E2E_GRAFANA_URL:-http://127.0.0.1:3001}" auth
  auth="$(printf 'user = "admin:%s"\n' "$(secret grafana_admin_password)")"
  wait_for 120 "Grafana Postgres datasource healthy ($1)" bash -c "curl -s -K - '$g/api/datasources/uid/postgres-indexer/health' \
    <<<'$auth' | grep -q '\"status\":\"OK\"'"
  check "Grafana contact point holds the current webhook URL ($1)" bash -c "curl -s -K - \
    '$g/api/v1/provisioning/contact-points/export?decrypt=true&format=json' <<<'$auth' | python3 -c '
import json, sys, yaml
want = yaml.safe_load(open(sys.argv[1]))[\"alert_webhook_url\"]
cps = json.load(sys.stdin)[\"contactPoints\"]
sys.exit(not any(r[\"settings\"].get(\"url\") == want for c in cps for r in c[\"receivers\"]))' '$E2E_WORK/deploy/secrets.yml'"
}

step1() {
  say "step 1: deploy with separate admin and operator keys"
  prepare_deploy_copy
  for k in admin operator escrow-admin user delegate; do keygen "$k"; done
  write_secrets
  sha256sum "$HOME/.docker/config.json" 2>/dev/null >"$E2E_WORK/docker-config.sha" || : >"$E2E_WORK/docker-config.sha"
  deploy deploy1 -vvv --diff || die "first deploy failed (logs/deploy1.log)"
  for k in admin escrow-admin user; do airdrop "$k"; done
  local out
  out="$(bin create_instance "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json")"
  save INSTANCE "$(echo "$out" | grep escrow_instance_id | sed 's/.*"\(.*\)".*/\1/')"
  # The localnet preflight checks the Operator PDA once an instance is set.
  if deploy deploy2-no-pda -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false; then
    die "deploy accepted an operator with no Operator PDA"
  fi
  check "refused before add_operator" grep -q "No Operator PDA for operator $(pub operator)" "$E2E_WORK/logs/deploy2-no-pda.log"
  check "no worker started by the refused deploy" test -z "$(docker ps -q --filter name=private-channel-operator-private-channel)"
  bin add_operator "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" "$(pub operator)" >>"$E2E_WORK/logs/txs.log" 2>&1
  deploy deploy2 -vvv --diff -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false \
    || die "second deploy failed (logs/deploy2.log)"
  # After the workers run, so the indexer records the AllowMint.
  setup_mint
  grafana_checks "first deploy"
  assert_no_secret_in "deploy output (-vvv --diff) and ansible.log" "$E2E_WORK/logs"
}

step2() {
  say "step 2: no secret in docker inspect, rendered files, argv or postgres logs"
  local out="$E2E_WORK/logs/nolek"
  mkdir -p "$out"
  for c in $(docker ps --filter "label=com.docker.compose.project=$E2E_PROJECT" -q) \
    $(docker ps --filter label=com.docker.compose.project=private-channel-monitoring -q); do
    docker inspect -f '{{json .Config.Env}} {{json .Config.Cmd}} {{json .Config.Entrypoint}} {{json .Config.Labels}}' "$c" \
      >"$out/inspect-$c.txt"
    docker exec "$c" sh -c 'for p in /proc/[0-9]*; do tr "\0" " " <"$p/cmdline" 2>/dev/null; echo; done' \
      >"$out/ps-$c.txt" 2>/dev/null || true
  done
  cp "$E2E_HOST_DATA_DIR/monitoring/monitoring.compose.yml" "$E2E_HOST_DATA_DIR/config/.env" \
    "$E2E_HOST_DATA_DIR/config/compose.override.yml" "$out/"
  ps -eo args >"$out/host-ps.txt"
  for c in private-channel-postgres-primary private-channel-postgres-replica private-channel-postgres-indexer; do
    docker logs "$c" >"$out/logs-$c.txt" 2>&1
  done
  check "monitoring.compose.yml is 0600" test "$(stat -c %a "$E2E_HOST_DATA_DIR/monitoring/monitoring.compose.yml")" = 600
  assert_no_secret_in "docker inspect, rendered files, argv and postgres logs" "$out"
}

step3() {
  say "step 3: secrets tree permissions and docker config"
  local s="$E2E_HOST_DATA_DIR/secrets"
  check "secrets root is root 0700" test "$(stat -c '%a %U' "$s")" = "700 root"
  check "service dirs are root 0755" test -z "$(find "$s" -mindepth 1 -maxdepth 1 -type d \( ! -perm 0755 -o ! -user root \))"
  check "files are root 0444" test -z "$(find "$s" -mindepth 2 -type f \( ! -perm 0444 -o ! -user root \))"
  check "a non-root user cannot read a secret" bash -c "! sudo -u nobody cat '$s/gateway/JWT_SECRET' 2>/dev/null"
  check "~/.docker/config.json unchanged" bash -c \
    "[[ \"\$(sha256sum '$HOME/.docker/config.json' 2>/dev/null)\" == \"\$(cat '$E2E_WORK/docker-config.sha')\" ]]"
  check "no throwaway docker config left" test -z "$(find "${TMPDIR:-/tmp}" -maxdepth 2 -name '*.pc-docker' 2>/dev/null)"
}

step4() {
  say "step 4: withdraw released by the operator key, admin pays"
  local sig signers
  sig="$(deposit_and_withdraw)"
  signers="$(tx_signers "$LOCAL_RPC" "$sig")"
  say "release $sig signers: $signers"
  check "admin pays the release" test "${signers%% *}" = "$(pub admin)"
  check "operator signs the release" grep -qw "$(pub "${E2E_OPERATOR_KEY:-operator}")" <<<"$signers"
}

step5() {
  say "step 5: rotate the runtime, gateway and Grafana DB passwords and the Grafana webhook"
  local old oldgw w r g p t0
  old="$(secret postgres_runtime_password)" oldgw="$(secret postgres_gateway_password)"
  w="$(cid private-channel-write-node)" r="$(cid private-channel-read-node)"
  g="$(cid private-channel-gateway)" p="$(cid private-channel-postgres-primary)"
  "$REPO/scripts/rotate-secret.sh" postgres_runtime_password --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  "$REPO/scripts/rotate-secret.sh" postgres_gateway_password --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  "$REPO/scripts/rotate-secret.sh" postgres_grafana_password --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  printf 'http://e2e-grafana-hook-%s.invalid/hook' "$(openssl rand -hex 12)" \
    | "$REPO/scripts/rotate-secret.sh" alert_webhook_url --value-stdin --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  t0="$(psql_primary -c 'SELECT now()')"
  deploy deploy5 -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false || die "redeploy failed (logs/deploy5.log)"
  check "write-node recreated" test "$(cid private-channel-write-node)" != "$w"
  check "read-node recreated" test "$(cid private-channel-read-node)" != "$r"
  check "runtime backends started after the rotation" test "$(psql_primary -c \
    "SELECT count(*) FROM pg_stat_activity WHERE usename='private_channel_runtime' AND backend_start > '$t0'")" -gt 0
  check "a DB-touching RPC succeeds" bash -c "curl -s -X POST $READ -H 'Content-Type: application/json' \
    -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"$MINT\"]}' | grep -q '\"result\"'"
  check "the old runtime password is refused" bash -c "! PGPASSWORD='$old' docker exec -e PGPASSWORD private-channel-postgres-primary \
    psql -h 127.0.0.1 -U private_channel_runtime -d private_channel -c 'SELECT 1' >/dev/null 2>&1"
  # Postgres reads only its superuser password after initdb, so role rotations leave it running.
  check "postgres-primary not recreated by role-password rotations" test "$(cid private-channel-postgres-primary)" = "$p"
  check "gateway recreated" test "$(cid private-channel-gateway)" != "$g"
  check "the gateway role logs in with the new password" bash -c "PGPASSWORD='$(secret postgres_gateway_password)' docker exec \
    -e PGPASSWORD private-channel-postgres-primary psql -h 127.0.0.1 -U private_channel_gateway -d private_channel -c 'SELECT 1' >/dev/null"
  check "the old gateway password is refused" bash -c "! PGPASSWORD='$oldgw' docker exec -e PGPASSWORD private-channel-postgres-primary \
    psql -h 127.0.0.1 -U private_channel_gateway -d private_channel -c 'SELECT 1' >/dev/null 2>&1"
  grafana_checks "after rotation"
}

step6() {
  say "step 6: rotate the superuser, replicator and runtime passwords together"
  local pid b p rp
  pid="$(psql_replica -c 'SELECT pid FROM pg_stat_wal_receiver')"
  b="$(cid private-channel-pg-backup-primary)"
  p="$(cid private-channel-postgres-primary)" rp="$(cid private-channel-postgres-replica)"
  "$REPO/scripts/rotate-secret.sh" postgres_password --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  "$REPO/scripts/rotate-secret.sh" postgres_replication_password --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  # With the runtime role too: the read node can log in only once the replica streams again.
  "$REPO/scripts/rotate-secret.sh" postgres_runtime_password --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  deploy deploy6 -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false || die "redeploy failed (logs/deploy6.log)"
  wait_for 90 "replica streaming with a new walreceiver" bash -c \
    "[[ \"\$(docker exec private-channel-postgres-replica psql -U private_channel -d postgres -tAq -c \"SELECT pid || ' ' || status FROM pg_stat_wal_receiver\")\" =~ ^[0-9]+\ streaming$ ]] && \
     [[ \"\$(docker exec private-channel-postgres-replica psql -U private_channel -d postgres -tAq -c 'SELECT pid FROM pg_stat_wal_receiver')\" != '$pid' ]]"
  check "the primary lists a replication client" test "$(psql_primary -c 'SELECT count(*) FROM pg_stat_replication')" -ge 1
  check "primary_conninfo keeps its other fields" bash -c "docker exec private-channel-postgres-replica psql -U private_channel -d postgres -tAq \
    -c 'SHOW primary_conninfo' | grep -q 'user=replicator' && docker exec private-channel-postgres-replica psql -U private_channel \
    -d postgres -tAq -c 'SHOW primary_conninfo' | grep -qE \"host='?postgres-primary'? port=5432\" && docker exec private-channel-postgres-replica \
    psql -U private_channel -d postgres -tAq -c 'SHOW primary_conninfo' | grep -vq 'password='"
  check "pg-backup-primary recreated" test "$(cid private-channel-pg-backup-primary)" != "$b"
  # Both read the superuser password file on every start, so they are recreated.
  check "postgres-primary recreated" test "$(cid private-channel-postgres-primary)" != "$p"
  check "postgres-replica recreated" test "$(cid private-channel-postgres-replica)" != "$rp"
  wait_for 300 "pg-backup-primary first base backup" bash -c "docker logs private-channel-pg-backup-primary 2>&1 | grep -q 'Backup complete'"
  check "superuser logs in with the new password" bash -c "PGPASSWORD='$(secret postgres_password)' docker exec -e PGPASSWORD \
    private-channel-postgres-primary psql -h 127.0.0.1 -U private_channel -d private_channel -c 'SELECT 1' >/dev/null"
  check "a read through the replica succeeds" bash -c "curl -s -X POST $READ -H 'Content-Type: application/json' \
    -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"$MINT\"]}' | grep -q '\"result\"'"
}

auth_token() {
  curl -s -X POST "$AUTH/auth/login" -H 'Content-Type: application/json' \
    -d "{\"username\":\"$1\",\"password\":\"$2\"}" | python3 -c 'import json, sys; print(json.load(sys.stdin)["token"])'
}
gated_status() {
  curl -s -o /dev/null -w '%{http_code}' -X POST "$GW" -H 'Content-Type: application/json' -H "Authorization: Bearer $1" \
    -d '{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[1]}'
}
wallets_status() { curl -s -o /dev/null -w '%{http_code}' "$AUTH/auth/wallets" -H "Authorization: Bearer $1"; }

step7() {
  say "step 7: JWT rotation with a previous key"
  local user pw a b
  user="e2e$(openssl rand -hex 4)" pw="$(openssl rand -hex 16)"
  curl -s -o /dev/null -X POST "$AUTH/auth/register" -H 'Content-Type: application/json' \
    -d "{\"username\":\"$user\",\"password\":\"$pw\"}"
  a="$(auth_token "$user" "$pw")"
  # A valid User-role token gets 403 on getBlock (operator only); an invalid one 401.
  check "token A valid before rotation" test "$(gated_status "$a")" = 403
  "$REPO/scripts/rotate-secret.sh" jwt_secret --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  deploy deploy7a -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false || die "stage deploy failed"
  b="$(auth_token "$user" "$pw")"
  check "token A still valid at the gateway while staged" test "$(gated_status "$a")" = 403
  check "token B valid at the gateway" test "$(gated_status "$b")" = 403
  check "auth accepts token A while staged" test "$(wallets_status "$a")" = 200
  "$REPO/scripts/rotate-secret.sh" jwt_secret --finish --secrets-file "$E2E_WORK/deploy/secrets.yml" >/dev/null
  deploy deploy7b -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false || die "finish deploy failed"
  check "token A refused by the gateway after finish" test "$(gated_status "$a")" = 401
  check "token A refused by auth after finish" test "$(wallets_status "$a")" = 401
  check "token B still valid" test "$(gated_status "$b")" = 403
}

step8() {
  say "step 8: fail closed without the entrypoint wrapper"
  local old="${E2E_OLD_IMAGE:?set E2E_OLD_IMAGE to an app image without with-secrets}"
  docker tag "$old" "$E2E_REGISTRY/private-channel-app:${E2E_TAG}-nowrapper"
  docker push -q "$E2E_REGISTRY/private-channel-app:${E2E_TAG}-nowrapper" >/dev/null
  docker tag "$E2E_REGISTRY/private-channel-validator:$E2E_TAG" "$E2E_REGISTRY/private-channel-validator:${E2E_TAG}-nowrapper"
  docker push -q "$E2E_REGISTRY/private-channel-validator:${E2E_TAG}-nowrapper" >/dev/null
  local gw
  gw="$(cid private-channel-gateway)"
  if deploy deploy8 --tags deploy -e "image_tag=${E2E_TAG}-nowrapper" -e "escrow_instance_id=$INSTANCE" \
    -e validator_reset=false -e reset_state=false; then
    die "deploy accepted an image without the wrapper"
  fi
  check "refused at the entrypoint check" grep -q "with-secrets entrypoint" "$E2E_WORK/logs/deploy8.log"
  check "running services untouched" test "$(cid private-channel-gateway)" = "$gw"
  local rc=0
  docker run --rm --entrypoint /usr/local/bin/gateway -e GATEWAY_REQUIRE_JWT=1 -e JWT_SECRET= \
    -e GATEWAY_PORT=18999 -e GATEWAY_WRITE_URL=http://127.0.0.1:1 -e GATEWAY_READ_URL=http://127.0.0.1:1 \
    "$E2E_REGISTRY/private-channel-app:$E2E_TAG" >"$E2E_WORK/logs/gateway-require-jwt.log" 2>&1 || rc=$?
  check "gateway refuses GATEWAY_REQUIRE_JWT=1 with a blank secret" test "$rc" -ne 0
  # Put the override back on the good tag.
  deploy deploy8b -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false || die "restore deploy failed"
}

step9() {
  say "step 9: operator rotation"
  keygen operator2
  bin add_operator "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" "$(pub operator2)" >>"$E2E_WORK/logs/txs.log" 2>&1
  compose stop operator-private-channel >/dev/null 2>&1
  E2E_OPERATOR_KEY=operator2 deploy deploy9 -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false \
    || die "deploy with the new operator failed"
  save E2E_OPERATOR_KEY operator2
  local sig signers pda
  sig="$(deposit_and_withdraw)"
  signers="$(tx_signers "$LOCAL_RPC" "$sig")"
  check "the new operator signs releases" grep -qw "$(pub operator2)" <<<"$signers"
  pda="$(solana find-program-derived-address "$ESCROW_PROGRAM_ID" string:operator \
    "pubkey:$INSTANCE" "pubkey:$(pub operator)")"
  # An unsettled withdrawal blocks remove_operator until it is released.
  compose stop operator-private-channel >/dev/null 2>&1
  deposit_and_burn
  wait_for 120 "a withdrawal is unsettled" has_unsettled_withdrawal
  if DATABASE_URL="$(indexer_db_url)" bin remove_operator "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" \
    "$(pub operator)" >"$E2E_WORK/logs/remove_operator-refused.log" 2>&1; then
    die "remove_operator ran with an unsettled withdrawal"
  fi
  check "remove_operator refused with the count and oldest row" grep -qE "[0-9]+ withdrawal\(s\) not settled; oldest: id [0-9]+ status" \
    "$E2E_WORK/logs/remove_operator-refused.log"
  check "the refused run left the old PDA" bash -c "curl -s -X POST $LOCAL_RPC -H 'Content-Type: application/json' \
    -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"$pda\"]}' | grep -q '\"owner\"'"
  compose start operator-private-channel >/dev/null 2>&1
  wait_for 360 "the pending withdrawal released" more_withdrawals_than "$BEFORE_W"
  wait_for 120 "every withdrawal settled" no_unsettled_withdrawal
  DATABASE_URL="$(indexer_db_url)" bin remove_operator "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" \
    "$(pub operator)" >"$E2E_WORK/logs/remove_operator.log" 2>&1 || die "remove_operator failed (logs/remove_operator.log)"
  check "remove_operator found nothing in flight" grep -q "No withdrawal in flight" "$E2E_WORK/logs/remove_operator.log"
  # ReleaseFunds requires this PDA, so a release signed by the old key now fails on chain.
  check "the old operator's PDA is closed" bash -c "curl -s -X POST $LOCAL_RPC -H 'Content-Type: application/json' \
    -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",\"params\":[\"$pda\"]}' | grep -q '\"value\":null'"
}

step10() {
  say "step 10: channel admin rotation"
  keygen admin2
  fund admin2
  # 1. Sweep the withdraw fees the old admin holds on the channel.
  sweep_fees "$E2E_WORK/keys/admin.json" "$(pub admin)" sweep-admin
  # 3. Overlap: the node accepts both admins.
  deploy deploy10a -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false \
    -e "{\"extra_admin_keys\": [\"$(pub admin2)\"]}" || die "overlap deploy failed"
  # 4-5. Stop both operators and move every receipt mint's authority.
  compose stop operator-solana operator-private-channel >/dev/null 2>&1
  DATABASE_URL="$(indexer_db_url)" PRIVATE_CHANNEL_READ_RPC_URL=$READ bin rotate_channel_admin "$GW" "$E2E_WORK/keys/admin.json" \
    "$(pub admin2)" --permanent-delegate "$(pub delegate)" >"$E2E_WORK/logs/rotate_channel_admin.log" 2>&1 \
    || die "rotate_channel_admin failed (logs/rotate_channel_admin.log)"
  # 6. Swap the admin, end the overlap.
  E2E_ADMIN_KEY=admin2 deploy deploy10b -e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false \
    || die "deploy with the new admin failed"
  save E2E_ADMIN_KEY admin2
  local info
  info="$(rpc "$READ" getAccountInfo "[\"$MINT\",{\"encoding\":\"jsonParsed\"}]")"
  check "the mint authority is the new admin" grep -q "\"mintAuthority\":\"$(pub admin2)\"" <<<"$info"
  check "the freeze authority is the new admin" grep -q "\"freezeAuthority\":\"$(pub admin2)\"" <<<"$info"
  # 7. A deposit mints with the new admin and rewrites the withdraw config treasury.
  sig="$(deposit_and_withdraw)"
  check "the new admin pays releases" test "$(tx_signers "$LOCAL_RPC" "$sig" | cut -d' ' -f1)" = "$(pub admin2)"
  local cfg
  cfg="$(solana find-program-derived-address "$WITHDRAW_PROGRAM_ID" string:withdraw_config "pubkey:$MINT")"
  check "the withdraw config treasury is the new admin" python3 "$REPO/scripts/e2e/account-has-pubkey.py" "$READ" "$cfg" "$(pub admin2)"
  # Simulated on the read node, which applies the node's admin rules; the new admin is the control.
  check "a MintTo signed by the old admin is refused (NotSignedByAdmin)" \
    python3 "$REPO/scripts/e2e/old-admin-mintto.py" "$READ" "$E2E_WORK/keys/admin.json" "$MINT" "$(pub user)"
  check "the same MintTo by the new admin passes simulation" bash -c \
    "! python3 '$REPO/scripts/e2e/old-admin-mintto.py' '$READ' '$E2E_WORK/keys/admin2.json' '$MINT' '$(pub user)'"
}

# Vault transit key -> base58 pubkey.
vault_pubkey() {
  vault_cli read -format=json "transit/keys/$1" | python3 -c 'import base64, json, sys
k = json.load(sys.stdin)["data"]["keys"]; b = base64.b64decode(k[max(k, key=int)]["public_key"])
A = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"; n = int.from_bytes(b, "big"); s = ""
while n: n, r = divmod(n, 58); s = A[r] + s
print("1" * (len(b) - len(b.lstrip(b"\0"))) + s)'
}
# The token goes through the env, never on docker's argv.
vault_cli() {
  VAULT_TOKEN="$VT" docker exec -e VAULT_TOKEN -e VAULT_ADDR=https://127.0.0.1:8200 -e VAULT_CACERT=/tmp/vault-ca.pem \
    pc-e2e-vault vault "$@"
}

# The app image plus the throwaway Vault CA in its system store (e2e only), tagged <tag>-vaultca.
build_vaultca_image() {
  local src="$E2E_REGISTRY/private-channel-app:$E2E_TAG" dir="$E2E_WORK/vaultca-image" user
  rm -rf "$dir" && mkdir -p "$dir"
  cp "$E2E_WORK/vault-ca.pem" "$dir/pc-e2e-vault.crt"
  docker pull -q "$src" >/dev/null
  user="$(docker inspect -f '{{.Config.User}}' "$src")"
  {
    echo "FROM $src"
    echo "USER root"
    echo "COPY pc-e2e-vault.crt /usr/local/share/ca-certificates/pc-e2e-vault.crt"
    echo "RUN update-ca-certificates"
    [[ -n "$user" ]] && echo "USER $user"
  } >"$dir/Dockerfile"
  docker build -q -t "$E2E_REGISTRY/private-channel-app:${E2E_TAG}-vaultca" "$dir" >/dev/null
  docker push -q "$E2E_REGISTRY/private-channel-app:${E2E_TAG}-vaultca" >/dev/null
  docker tag "$E2E_REGISTRY/private-channel-validator:$E2E_TAG" "$E2E_REGISTRY/private-channel-validator:${E2E_TAG}-vaultca"
  docker push -q "$E2E_REGISTRY/private-channel-validator:${E2E_TAG}-vaultca" >/dev/null
}

step11() {
  say "step 11: both roles on an https Vault, then the old admin on Vault for the sweep and rotation"
  local net="${E2E_PROJECT}_private-channel-network" ca="$E2E_WORK/vault-ca.pem"
  VT="$(openssl rand -hex 16)"
  docker rm -f pc-e2e-vault >/dev/null 2>&1 || true
  # -dev-tls signs the listener with a throwaway CA; the SAN lets the app containers use its name.
  VAULT_DEV_ROOT_TOKEN_ID="$VT" docker run -d --name pc-e2e-vault --network "$net" -p 127.0.0.1:18200:8200 \
    --cap-add IPC_LOCK -e VAULT_DEV_ROOT_TOKEN_ID hashicorp/vault:1.17 server -dev -dev-tls \
    -dev-tls-san=pc-e2e-vault -dev-tls-cert-dir=/tmp -dev-listen-address=0.0.0.0:8200 >/dev/null
  wait_for 60 "vault up (https)" vault_cli status
  docker cp pc-e2e-vault:/tmp/vault-ca.pem "$ca"
  vault_cli secrets enable transit >/dev/null
  vault_cli write -f transit/keys/pc-admin type=ed25519 >/dev/null
  vault_cli write -f transit/keys/pc-operator type=ed25519 >/dev/null
  local vadmin vop
  vadmin="$(vault_pubkey pc-admin)" vop="$(vault_pubkey pc-operator)"
  say "vault admin $vadmin, vault operator $vop"
  bin add_operator "$LOCAL_RPC" "$E2E_WORK/keys/escrow-admin.json" "$INSTANCE" "$vop" >>"$E2E_WORK/logs/txs.log" 2>&1
  # The Vault admin pays releases on Solana.
  fund_addr "$vadmin"
  printf "admin_vault_token: '%s'\noperator_vault_token: '%s'\n" "$VT" "$VT" >>"$E2E_WORK/deploy/secrets.yml"
  local kept=(-e "escrow_instance_id=$INSTANCE" -e validator_reset=false -e reset_state=false)
  # The signer client is https-only, so a plain-http Vault must be refused before any change.
  if deploy deploy11-http "${kept[@]}" -e operator_signer=vault -e operator_vault_addr=http://pc-e2e-vault:8200 \
    -e operator_vault_key_name=pc-operator -e "operator_pubkey=$vop"; then
    die "a plain-http Vault address was accepted"
  fi
  check "plain-http Vault refused by preflight" grep -q "must be an https:// URL" "$E2E_WORK/logs/deploy11-http.log"

  build_vaultca_image
  kept+=(-e "image_tag=${E2E_TAG}-vaultca")
  local vault=(-e admin_signer=vault -e admin_vault_addr=https://pc-e2e-vault:8200 -e admin_vault_key_name=pc-admin
    -e "admin_pubkey=$vadmin" -e operator_signer=vault -e operator_vault_addr=https://pc-e2e-vault:8200
    -e operator_vault_key_name=pc-operator -e "operator_pubkey=$vop")
  # Move the channel admin onto the Vault key: overlap, rotate the mints, swap both roles.
  deploy deploy11a "${kept[@]}" -e "{\"extra_admin_keys\": [\"$vadmin\"]}" || die "overlap deploy failed (logs/deploy11a.log)"
  compose stop operator-solana operator-private-channel >/dev/null 2>&1
  DATABASE_URL="$(indexer_db_url)" PRIVATE_CHANNEL_READ_RPC_URL=$READ bin rotate_channel_admin "$GW" \
    "$E2E_WORK/keys/${E2E_ADMIN_KEY:-admin}.json" "$vadmin" --permanent-delegate "$(pub delegate)" \
    >"$E2E_WORK/logs/rotate-to-vault.log" 2>&1 || die "rotate_channel_admin to the vault key failed (logs/rotate-to-vault.log)"
  deploy deploy11 "${kept[@]}" "${vault[@]}" || die "vault deploy failed (logs/deploy11.log)"
  check "the withdraw operator loaded its Vault signer" bash -c \
    "docker logs private-channel-operator-private-channel 2>&1 | grep -q 'Loaded operator signer (vault): $vop'"
  local sig signers
  sig="$(deposit_and_withdraw)"
  signers="$(tx_signers "$LOCAL_RPC" "$sig")"
  say "release $sig signers: $signers"
  check "the Vault admin pays the release" test "${signers%% *}" = "$vadmin"
  check "the Vault operator signs the release" grep -qw "$vop" <<<"$signers"

  # The old admin on Vault: sweep its fees, then rotate the channel admin to a keypair.
  export OLD_ADMIN_SIGNER=vault OLD_ADMIN_VAULT_ADDR=https://127.0.0.1:18200 OLD_ADMIN_VAULT_KEY_NAME=pc-admin \
    OLD_ADMIN_VAULT_PUBKEY="$vadmin" OLD_ADMIN_VAULT_CA_CERT="$ca" OLD_ADMIN_VAULT_TOKEN="$VT"
  sweep_fees env:OLD_ADMIN "$vadmin" sweep-vault-admin
  keygen admin3
  deploy deploy11c "${kept[@]}" "${vault[@]}" -e "{\"extra_admin_keys\": [\"$(pub admin3)\"]}" \
    || die "second overlap deploy failed (logs/deploy11c.log)"
  compose stop operator-solana operator-private-channel >/dev/null 2>&1
  DATABASE_URL="$(indexer_db_url)" PRIVATE_CHANNEL_READ_RPC_URL=$READ bin rotate_channel_admin "$GW" env:OLD_ADMIN \
    "$(pub admin3)" --permanent-delegate "$(pub delegate)" >"$E2E_WORK/logs/rotate-from-vault.log" 2>&1 \
    || die "rotate_channel_admin with the old admin on Vault failed (logs/rotate-from-vault.log)"
  unset OLD_ADMIN_SIGNER OLD_ADMIN_VAULT_ADDR OLD_ADMIN_VAULT_KEY_NAME OLD_ADMIN_VAULT_PUBKEY OLD_ADMIN_VAULT_CA_CERT OLD_ADMIN_VAULT_TOKEN
  local info
  info="$(rpc "$READ" getAccountInfo "[\"$MINT\",{\"encoding\":\"jsonParsed\"}]")"
  check "the Vault admin moved the mint authority" grep -q "\"mintAuthority\":\"$(pub admin3)\"" <<<"$info"
  check "the Vault admin moved the freeze authority" grep -q "\"freezeAuthority\":\"$(pub admin3)\"" <<<"$info"
  assert_no_secret_in "vault deploy output and CLI logs" "$E2E_WORK/logs/deploy11.log" "$E2E_WORK/logs/deploy11c.log" \
    "$E2E_WORK/logs/rotate-from-vault.log" "$E2E_WORK/logs/sweep-vault-admin.log"
  check "the vault token is not in docker inspect" bash -c "! docker inspect private-channel-operator-private-channel \
    private-channel-operator-solana | grep -qF '$VT'"
}

step12() {
  say "step 12: teardown removes the secrets tree"
  docker rm -f pc-e2e-vault >/dev/null 2>&1 || true
  docker compose -p private-channel-monitoring --env-file "$E2E_HOST_DATA_DIR/config/.env" \
    -f "$E2E_HOST_DATA_DIR/monitoring/monitoring.compose.yml" down -v >/dev/null 2>&1 || true
  compose --profile auth down -v --remove-orphans >/dev/null 2>&1 || true
  # shellcheck disable=SC2086
  (cd "$E2E_WORK/deploy" && ansible-playbook -i localhost, -c local teardown.yml -e env=dev \
    -e "host_data_dir=$E2E_HOST_DATA_DIR" ${E2E_TEARDOWN_ARGS:+"$E2E_TEARDOWN_ARGS"}) >"$E2E_WORK/logs/teardown.log" 2>&1 \
    || die "teardown failed (logs/teardown.log)"
  check "secrets tree removed" test ! -e "$E2E_HOST_DATA_DIR/secrets"
}

for s in ${E2E_STEPS:-1 2 3 4 5 6 7 8 9 10 11 12}; do
  "step$s"
  say "step $s pass"
done
say "deploy-secrets e2e passed (work dir $E2E_WORK)"
