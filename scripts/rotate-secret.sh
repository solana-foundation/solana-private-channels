#!/usr/bin/env bash
set -euo pipefail

# rotate-secret.sh: rotate one deploy secret in secrets.yml or Doppler, then redeploy.
# Usage: rotate-secret.sh <key> [--backend file|doppler --project P --config C]
#                         [--value-stdin] [--finish] [--secrets-file PATH]
# Generated keys get a fresh random value; supplied keys (tokens, webhooks, provider
# URLs) need --value-stdin. No value ever reaches a command line or the output.
# jwt_signing_key stages a new keypair (the old public key keeps verifying) and --finish ends it.

usage() {
  sed -n '4,9p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

key="${1:-}"
[[ -n "$key" && "$key" != -* ]] || usage
shift
backend=file project="" config="" value_stdin=false finish=false
secrets_file="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/private-channel-deploy/secrets.yml"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --backend) backend="${2:-}"; shift 2 ;;
    --project) project="${2:-}"; shift 2 ;;
    --config) config="${2:-}"; shift 2 ;;
    --value-stdin) value_stdin=true; shift ;;
    --finish) finish=true; shift ;;
    --secrets-file) secrets_file="${2:-}"; shift 2 ;;
    *) usage ;;
  esac
done

case "$key" in
  postgres_password | postgres_replication_password | postgres_runtime_password | \
  postgres_auth_runtime_password | postgres_auth_owner_password | postgres_gateway_password | \
  postgres_monitoring_password | postgres_grafana_password | postgres_indexer_password | \
  grafana_admin_password) kind=hex16 ;;
  jwt_signing_key) kind=jwt ;;
  ghcr_token | yellowstone_token | notify_webhook | alert_webhook_url | rpc_url | fallback_rpc_url | \
  gateway_whitelist_rpc_url | admin_vault_token | operator_vault_token | admin_turnkey_api_private_key | \
  operator_turnkey_api_private_key | admin_privy_app_secret | operator_privy_app_secret) kind=supplied ;;
  *) echo "ERROR: unknown secret key '$key'" >&2; exit 2 ;;
esac

if [[ "$finish" == true && "$key" != jwt_signing_key ]]; then
  echo "ERROR: --finish only applies to jwt_signing_key" >&2
  exit 2
fi
if [[ "$kind" == supplied && "$value_stdin" != true ]]; then
  echo "ERROR: $key is supplied by you, not generated: pipe it in with --value-stdin" >&2
  exit 2
fi
if [[ "$kind" == jwt && "$value_stdin" == true ]]; then
  echo "ERROR: $key is always generated as a keypair, so --value-stdin does not apply" >&2
  exit 2
fi

# Doppler names: the upper-case key, except the two webhooks.
doppler_name() {
  case "$1" in
    notify_webhook) echo ALERT_WEBHOOK_URL ;;
    alert_webhook_url) echo GRAFANA_ALERT_WEBHOOK_URL ;;
    *) printf '%s\n' "$1" | tr '[:lower:]' '[:upper:]' ;;
  esac
}

value=""
if [[ "$finish" != true ]]; then
  if [[ "$value_stdin" == true ]]; then
    # read -d '' keeps every byte; a newline anywhere is refused below.
    IFS= read -r -d '' value || true
    value="${value%$'\n'}"
  elif [[ "$kind" == hex16 ]]; then
    value="$(openssl rand -hex 16)"
  else
    pair="$("$(dirname "${BASH_SOURCE[0]}")/jwt-keypair.sh")"
    signing="${pair%%$'\n'*}"
    public="${pair#*$'\n'}"
    # "signing public" as one value; base58 has no spaces.
    value="${signing#JWT_SIGNING_KEY=} ${public#JWT_PUBLIC_KEY=}"
  fi
  if [[ -z "$value" || "$value" == *$'\n'* || "$value" == *$'\r'* ]]; then
    echo "ERROR: the value for $key is empty or contains a newline" >&2
    exit 1
  fi
fi

# Edits secrets.yml in place. Ops arrive on stdin as JSON, so values never hit argv.
# Each value is a single-quoted YAML scalar ('' escapes a quote); only target lines change.
edit_file() {
  python3 -c '
import json, os, re, sys, tempfile
path, mode, key = sys.argv[1], sys.argv[2], sys.argv[3]
new = json.load(sys.stdin)["value"]
lines = open(path).read().splitlines(True)

def find(k):
    for i, l in enumerate(lines):
        if re.match(re.escape(k) + r":(\s|$)", l):
            # Only one-line values are edited; a block or continued value would be half rewritten.
            v = re.sub(r"(^|\s)#.*$", "", l.split(":", 1)[1]).strip()
            nxt = lines[i + 1] if i + 1 < len(lines) else ""
            if re.match(r"[|>]", v) or (nxt[:1] in (" ", "\t") and nxt.strip() and not nxt.strip().startswith("#")):
                sys.exit("ERROR: " + k + " is a multi-line value in secrets.yml; make it one line, then rerun")
            return i
    return None

def get(k):
    i = find(k)
    if i is None:
        return ""
    v = lines[i].split(":", 1)[1].strip()
    # A quoted scalar ends at its own closing quote; a trailing comment may hold more quotes.
    if v.startswith("\x27"):
        m = re.match(r"\x27((?:[^\x27]|\x27\x27)*)\x27", v)
        if not m:
            sys.exit("ERROR: " + k + " has an unterminated quote in secrets.yml")
        return m.group(1).replace("\x27\x27", "\x27")
    if v.startswith("\""):
        return json.JSONDecoder().raw_decode(v)[0]
    return re.sub(r"\s+#.*$", "", v)

def put(k, v):
    line = k + ": \x27" + v.replace("\x27", "\x27\x27") + "\x27\n"
    i = find(k)
    if i is None:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(line)
    else:
        lines[i] = line

if mode == "set":
    put(key, new)
elif mode == "jwt-stage":
    if get("jwt_public_key_previous"):
        sys.exit("ERROR: jwt_public_key_previous is already set: finish that rotation first (--finish)")
    signing, public = new.split(" ")
    # A first keypair has no old key to keep verifying.
    if get("jwt_public_key"):
        put("jwt_public_key_previous", get("jwt_public_key"))
    put("jwt_signing_key", signing)
    put("jwt_public_key", public)
elif mode == "jwt-finish":
    if not get("jwt_public_key_previous"):
        sys.exit("ERROR: jwt_public_key_previous is empty: no rotation to finish")
    put("jwt_public_key_previous", "")
fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(path)), prefix=".rotate-")
try:
    with os.fdopen(fd, "w") as f:
        f.writelines(lines)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)
except BaseException:
    os.unlink(tmp)
    raise
' "$secrets_file" "$1" "$key"
}

# Runs doppler on the operator's own login, never a service token from the env.
doppler_run() {
  env -u DOPPLER_TOKEN doppler "$@" --project "$project" --config "$config"
}

json_value() {
  python3 -c 'import json, sys; print(json.dumps({"value": sys.stdin.read()}))'
}

# A Doppler secret, or nothing when it does not exist. Any other failure stops, or
# staging could overwrite a previous key that is still in use.
doppler_get() {
  local err="${TMPDIR:-/tmp}/.rotate-doppler-err.$$"
  if ! doppler_run secrets get "$1" --plain 2>"$err" && ! grep -qi 'could not find' "$err"; then
    rm -f "$err"
    echo "ERROR: could not read $1 from Doppler" >&2
    exit 1
  fi
  rm -f "$err"
}

# A failed write mid-stage leaves keys that do not match. In this write order,
# --finish and a fresh stage always end in a matching pair.
partial() {
  echo "ERROR: the rotation was only partly written to Doppler. Do not deploy: run $0 jwt_signing_key --finish if JWT_PUBLIC_KEY_PREVIOUS is set, then stage again." >&2
  exit 1
}

case "$backend" in
  file)
    if [[ ! -f "$secrets_file" ]]; then
      echo "ERROR: $secrets_file not found" >&2
      exit 1
    fi
    if [[ "$key" == jwt_signing_key && "$finish" == true ]]; then
      printf '' | json_value | edit_file jwt-finish
    elif [[ "$key" == jwt_signing_key ]]; then
      printf '%s' "$value" | json_value | edit_file jwt-stage
    else
      printf '%s' "$value" | json_value | edit_file set
    fi
    where="$secrets_file"
    ;;
  doppler)
    if [[ -z "$project" || -z "$config" ]]; then
      echo "ERROR: the doppler backend needs --project and --config (no directory-scope fallback)" >&2
      exit 2
    fi
    if [[ "$key" == jwt_signing_key ]]; then
      previous="$(doppler_get JWT_PUBLIC_KEY_PREVIOUS)"
      if [[ "$finish" == true ]]; then
        [[ -n "$previous" ]] || { echo "ERROR: JWT_PUBLIC_KEY_PREVIOUS is empty: no rotation to finish" >&2; exit 1; }
        doppler_run secrets delete JWT_PUBLIC_KEY_PREVIOUS --yes --silent
      else
        [[ -z "$previous" ]] || { echo "ERROR: JWT_PUBLIC_KEY_PREVIOUS is already set: finish that rotation first (--finish)" >&2; exit 1; }
        # A first keypair has no old key to keep verifying.
        current="$(doppler_get JWT_PUBLIC_KEY)"
        if [[ -n "$current" ]]; then
          printf '%s' "$current" | doppler_run secrets set JWT_PUBLIC_KEY_PREVIOUS --silent || partial
        fi
        printf '%s' "${value% *}" | doppler_run secrets set JWT_SIGNING_KEY --silent || partial
        printf '%s' "${value#* }" | doppler_run secrets set JWT_PUBLIC_KEY --silent || partial
      fi
    else
      printf '%s' "$value" | doppler_run secrets set "$(doppler_name "$key")" --silent
    fi
    where="Doppler $project/$config"
    ;;
  *) echo "ERROR: --backend must be file or doppler" >&2; exit 2 ;;
esac

echo "Rotated $key in $where."
if [[ "$key" == jwt_signing_key && "$finish" != true ]]; then
  echo "Tokens for the old key, if any, keep verifying until: $0 jwt_signing_key --finish (then redeploy again)."
fi
echo "Apply it with: cd private-channel-deploy && ansible-playbook deploy.yml -l <group>"
