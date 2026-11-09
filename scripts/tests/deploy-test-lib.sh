#!/usr/bin/env bash
# Shared helpers for the deploy contract tests. Source it after setting REPO.
# Gives each test a private copy of private-channel-deploy in $D, so stub
# secrets.yml files and ansible.log never land in the repo.

failures=0
fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

skip() {
  if [[ "${CI:-}" == true ]]; then
    echo "FAIL: CI cannot skip $1" >&2
    exit 1
  fi
  echo "skip: $1"
}

STUB_PIDS=()
deploy_test_cleanup() {
  local pid
  for pid in "${STUB_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  # The secrets render leaves a root-owned tree under TMP, so fall back to sudo.
  [[ -n "${TMP:-}" ]] && { rm -rf "$TMP" 2>/dev/null || sudo -n rm -rf "$TMP"; }
}

# Creates $TMP and the private deploy copy $D.
deploy_test_setup() {
  TMP="$(mktemp -d)"
  trap deploy_test_cleanup EXIT
  cp -r "$REPO/private-channel-deploy" "$TMP/deploy"
  D="$TMP/deploy"
  rm -f "$D/secrets.yml" "$D/ansible.log"
  export ANSIBLE_CONFIG="$D/ansible.cfg" ANSIBLE_LOG_PATH="$TMP/ansible.log"
  # The stub secrets.yml is the file backend; a caller's DOPPLER_TOKEN would switch it.
  unset DOPPLER_TOKEN PC_TEST_GITHUB_API
}

# Ansible refuses non-blocking stdio, so its output always goes to a file.
playbook() {
  local out="$1"
  shift
  ansible-playbook -i localhost, -c local "$@" >"$out" 2>&1 </dev/null
}

# Expect the playbook to fail with `msg`. Defaults to --tags config_asserts.
expect_refused() {
  local name="$1" msg="$2"
  shift 2
  local tags=(--tags config_asserts)
  [[ " $* " == *" --tags "* ]] && tags=()
  if playbook "$TMP/$name.log" "$D/deploy.yml" "${tags[@]}" "$@"; then
    fail "$name: preflight passed, expected refusal"
  elif ! grep -q -- "$msg" "$TMP/$name.log"; then
    fail "$name: refused without '$msg'; see output below"
    tail -40 "$TMP/$name.log" >&2
  else
    echo "ok: $name refused"
  fi
}

# Expect the playbook to pass. Defaults to --tags config_asserts.
expect_pass() {
  local name="$1"
  shift
  local tags=(--tags config_asserts)
  [[ " $* " == *" --tags "* ]] && tags=()
  if ! playbook "$TMP/$name.log" "$D/deploy.yml" "${tags[@]}" "$@"; then
    fail "$name: refused, expected pass"
    tail -40 "$TMP/$name.log" >&2
    return 0
  fi
  echo "ok: $name passed"
}

# Renders one template with the deploy vars: render <template> <out> [ansible args].
render_template() {
  local src="$1" out="$2"
  shift 2
  cat >"$TMP/render.yml" <<EOF
- hosts: localhost
  gather_facts: false
  vars_files: [ "$D/vars/common.yml", "$D/vars/dev.yml", "$D/secrets.yml" ]
  tasks:
  - ansible.builtin.template: { src: "$src", dest: "$out" }
EOF
  playbook "$out.log" "$TMP/render.yml" -e image_tag=t "$@" || {
    fail "render $out"
    tail -20 "$out.log" >&2
  }
}

# Renders env.j2: render <out> [ansible args].
render() {
  local out="$1"
  shift
  render_template "$D/templates/env.j2" "$out" "$@"
}

# Starts an HTTP stub answering every request with a canned reply and sets
# STUB_PORT. Usage: stub_http <status> <body-file|""> [Header: value ...]
stub_http() {
  local status="$1" body="$2"
  shift 2
  cat >"$TMP/stub_http.py" <<'PY'
import http.server, sys
status = int(sys.argv[1])
body = open(sys.argv[2], "rb").read() if sys.argv[2] else b""
headers = [h.split(":", 1) for h in sys.argv[3:]]
class H(http.server.BaseHTTPRequestHandler):
    def reply(self):
        n = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(n)
        self.send_response(status)
        for k, v in headers:
            self.send_header(k.strip(), v.strip())
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    do_GET = do_POST = reply
    def log_message(self, *a):
        pass
srv = http.server.HTTPServer(("127.0.0.1", 0), H)
print(srv.server_address[1], flush=True)
srv.serve_forever()
PY
  local portfile
  portfile="$(mktemp "$TMP/stubport.XXXX")"
  python3 "$TMP/stub_http.py" "$status" "$body" "$@" >"$portfile" 2>/dev/null &
  STUB_PIDS+=($!)
  local _
  for _ in $(seq 1 50); do
    [[ -s "$portfile" ]] && break
    sleep 0.1
  done
  STUB_PORT="$(head -1 "$portfile")"
}

# A port with nothing listening on it.
free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

# Writes secrets.yml to $1 with a unique sentinel per key, URL-shaped for URL keys,
# and records every sentinel in $TMP/sentinels.txt.
sentinel_secrets() {
  local out="$1" tag key val
  tag="$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
  : >"$TMP/sentinels.txt"
  : >"$out"
  for key in postgres_password postgres_replication_password postgres_runtime_password \
    postgres_auth_runtime_password postgres_auth_owner_password postgres_gateway_password \
    postgres_monitoring_password postgres_grafana_password postgres_indexer_password \
    grafana_admin_password ghcr_token yellowstone_token \
    admin_vault_token operator_vault_token admin_turnkey_api_private_key \
    operator_turnkey_api_private_key admin_privy_app_secret operator_privy_app_secret; do
    val="SNTL${tag}x${key}"
    printf '%s: %s\n' "$key" "$val" >>"$out"
    printf '%s\n' "$val" >>"$TMP/sentinels.txt"
  done
  # 32+ bytes so the JWT asserts accept them.
  for key in jwt_secret jwt_secret_previous; do
    val="SNTL${tag}x${key}xxxxxxxxxxxxxxxxxxxxxxxx"
    printf '%s: %s\n' "$key" "$val" >>"$out"
    printf '%s\n' "$val" >>"$TMP/sentinels.txt"
  done
  # JSON with base64-alphabet values, the shape the escrow_confidential_keys assert expects.
  printf "escrow_confidential_keys: '{\"elgamal_secret\":\"SNTL%sxelgamal\",\"aes_key\":\"SNTL%sxaes\"}'\n" \
    "$tag" "$tag" >>"$out"
  printf 'SNTL%sxelgamal\nSNTL%sxaes\n' "$tag" "$tag" >>"$TMP/sentinels.txt"
  for key in notify_webhook alert_webhook_url rpc_url fallback_rpc_url gateway_whitelist_rpc_url; do
    val="https://sntl${tag}x${key//_/-}.invalid/k/SNTL${tag}x${key}"
    printf "%s: '%s'\n" "$key" "$val" >>"$out"
    printf 'SNTL%sx%s\n' "$tag" "$key" >>"$TMP/sentinels.txt"
  done
  printf 'ghcr_user: u\n' >>"$out"
}

# Fails when any sentinel from $TMP/sentinels.txt appears in the given files or dirs.
assert_no_sentinel() {
  local what="$1"
  shift
  local hits
  hits="$(grep -rhoF -f "$TMP/sentinels.txt" "$@" 2>/dev/null | sort -u || true)"
  if [[ -n "$hits" ]]; then
    fail "$what: sentinel(s) leaked: $(echo "$hits" | sed -E 's/^SNTL[0-9a-f]+x//' | tr '\n' ' ')"
  fi
}
