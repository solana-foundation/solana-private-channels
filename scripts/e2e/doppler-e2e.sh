#!/usr/bin/env bash
set -euo pipefail

# The Doppler secrets backend renders exactly what the file backend renders.
#   --fake  a doppler shim on PATH serves the same values as secrets.yml.
#   --real  needs a CLI login (doppler login) with write access to DOPPLER_PROJECT:
#           creates config dev_e2e_<timestamp>, uploads with scripts/doppler-import.sh,
#           mints a read-only service token for it, and deletes the config on exit.
# Runs --tags preflight,render,secrets_render against localhost (devnet shape, so no
# validator assets are built), then diffs the .env, override and secrets tree.
# Needs root or passwordless sudo (the secrets tree is root-owned).

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MODE="${1:-}"
[[ "$MODE" == --fake || "$MODE" == --real ]] || { echo "Usage: $0 --fake | --real" >&2; exit 2; }
export E2E_HOST_DATA_DIR="${E2E_HOST_DATA_DIR:-/tmp/pc-doppler-e2e}"
# shellcheck source=scripts/e2e/e2e-lib.sh
. "$REPO/scripts/e2e/e2e-lib.sh"

prepare_deploy_copy
for k in admin operator delegate; do keygen "$k"; done
write_secrets
cat >>"$E2E_WORK/deploy/secrets.yml" <<EOF
rpc_url: 'https://e2e-rpc-$(openssl rand -hex 8).invalid/?api-key=$(openssl rand -hex 16)'
fallback_rpc_url: 'https://e2e-fallback-$(openssl rand -hex 8).invalid/$(openssl rand -hex 16)'
yellowstone_token: '$(openssl rand -hex 16)'
alert_webhook_url: 'https://e2e-grafana-$(openssl rand -hex 8).invalid/hook'
jwt_secret_previous: '$(openssl rand -hex 32)'
EOF
mkdir -p "$E2E_HOST_DATA_DIR/config"

render_with() {
  local log="$1"
  shift
  (cd "$E2E_WORK/deploy" && "$@" ansible-playbook -i localhost, -c local deploy.yml --tags preflight,render,secrets_render \
    -e env=dev -e network=devnet -e "repo_root=$E2E_REPO_ROOT" -e "host_data_dir=$E2E_HOST_DATA_DIR" \
    -e image_registry=127.0.0.1:5000/doppler-e2e -e image_tag=e2e -e escrow_instance_id=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz \
    -e yellowstone_endpoint=https://ys.invalid -e "admin_keypair_path=$E2E_WORK/keys/admin.json" \
    -e "operator_keypair_path=$E2E_WORK/keys/operator.json" -e "permanent_delegate=$(pub delegate)") \
    >"$E2E_WORK/logs/$log.log" 2>&1
}

snapshot() {
  sudo sh -c "cd '$E2E_HOST_DATA_DIR' && find secrets -type f ! -name .salt | sort | while read -r f; do printf '%s=' \"\$f\"; cat \"\$f\"; echo; done" >"$E2E_WORK/$1.tree"
  cp "$E2E_HOST_DATA_DIR/config/.env" "$E2E_WORK/$1.env"
  cp "$E2E_HOST_DATA_DIR/config/compose.override.yml" "$E2E_WORK/$1.override"
}

render_with file env -u DOPPLER_TOKEN || die "file backend render failed (logs/file.log)"
grep -q 'Secrets backend: file' "$E2E_WORK/logs/file.log" || die "file render did not use the file backend"
snapshot file

if [[ "$MODE" == --fake ]]; then
  python3 - "$E2E_WORK/deploy/secrets.yml" >"$E2E_WORK/doppler.json" <<'PY'
import json, sys, yaml
y = yaml.safe_load(open(sys.argv[1]))
d = {k.upper(): str(v) for k, v in y.items() if v not in (None, "") and k not in ("notify_webhook", "alert_webhook_url")}
d["ALERT_WEBHOOK_URL"] = y["notify_webhook"]
d["GRAFANA_ALERT_WEBHOOK_URL"] = y["alert_webhook_url"]
print(json.dumps(d))
PY
  mkdir -p "$E2E_WORK/bin"
  cat >"$E2E_WORK/bin/doppler" <<EOF
#!/usr/bin/env bash
[[ "\$1" == --version ]] && { echo v3-fake; exit 0; }
cat "$E2E_WORK/doppler.json"
EOF
  chmod +x "$E2E_WORK/bin/doppler"
  rm "$E2E_WORK/deploy/secrets.yml"
  render_with doppler env "PATH=$E2E_WORK/bin:$PATH" DOPPLER_TOKEN=fake-service-token \
    || die "doppler backend render failed (logs/doppler.log)"
else
  : "${DOPPLER_PROJECT:?set DOPPLER_PROJECT for --real}"
  config="dev_e2e_$(date +%Y%m%d%H%M%S)"
  cleanup_config() { env -u DOPPLER_TOKEN doppler configs delete "$config" --project "$DOPPLER_PROJECT" --yes --silent || true; }
  trap cleanup_config EXIT
  env -u DOPPLER_TOKEN doppler configs create "$config" --project "$DOPPLER_PROJECT" --environment dev --silent
  "$REPO/scripts/doppler-import.sh" --project "$DOPPLER_PROJECT" --config "$config" "$E2E_WORK/deploy/secrets.yml"
  token="$(env -u DOPPLER_TOKEN doppler configs tokens create e2e --project "$DOPPLER_PROJECT" --config "$config" --plain)"
  rm "$E2E_WORK/deploy/secrets.yml"
  # Through the environment, so the token never reaches argv.
  DOPPLER_TOKEN="$token" render_with doppler || die "doppler backend render failed (logs/doppler.log)"
fi
grep -q 'Secrets backend: doppler' "$E2E_WORK/logs/doppler.log" || die "second render did not use the doppler backend"
snapshot doppler

for part in tree env override; do
  check "doppler matches file: $part" diff -q "$E2E_WORK/file.$part" "$E2E_WORK/doppler.$part"
done
sudo rm -rf "$E2E_HOST_DATA_DIR"
say "doppler e2e ($MODE) passed (work dir $E2E_WORK)"
