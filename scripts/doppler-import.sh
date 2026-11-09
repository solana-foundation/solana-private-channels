#!/usr/bin/env bash
set -euo pipefail

# doppler-import.sh: upload a deploy secrets.yml to an explicit Doppler project and config.
# Usage: doppler-import.sh --project P --config C [--create-config] [--dry-run] <secrets.yml>
# Keys are upper-cased (notify_webhook and alert_webhook_url become ALERT_WEBHOOK_URL and
# GRAFANA_ALERT_WEBHOOK_URL) and empty values are skipped. --dry-run prints only the key
# names. Uses your own Doppler login, never DOPPLER_TOKEN. No value is ever printed.

usage() {
  sed -n '4,8p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

project="" config="" create_config=false dry_run=false secrets_file=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) project="${2:-}"; shift 2 ;;
    --config) config="${2:-}"; shift 2 ;;
    --create-config) create_config=true; shift ;;
    --dry-run) dry_run=true; shift ;;
    -h | --help) usage ;;
    -*) usage ;;
    *)
      [[ -z "$secrets_file" ]] || usage
      secrets_file="$1"; shift ;;
  esac
done

[[ -n "$secrets_file" ]] || usage
if [[ ! -f "$secrets_file" ]]; then
  echo "ERROR: $secrets_file not found" >&2
  exit 1
fi
if [[ "$dry_run" != true && ( -z "$project" || -z "$config" ) ]]; then
  echo "ERROR: --project and --config are required (no directory-scope fallback)" >&2
  exit 2
fi
if ! python3 -c 'import yaml' 2>/dev/null; then
  echo "ERROR: python3 with PyYAML is required (e.g. apt install python3-yaml, or pip install pyyaml)" >&2
  exit 1
fi

# mode json: the Doppler upload document on stdout; mode names: one key name per line.
convert() {
  python3 - "$1" "$secrets_file" <<'PY'
import json, re, sys
import yaml

mode, path = sys.argv[1], sys.argv[2]
RENAME = {"notify_webhook": "ALERT_WEBHOOK_URL", "alert_webhook_url": "GRAFANA_ALERT_WEBHOOK_URL"}
try:
    with open(path) as f:
        data = yaml.safe_load(f)
except yaml.YAMLError as e:
    # The parser's own message can quote the offending line, so report only its position.
    mark = getattr(e, "problem_mark", None)
    where = f" at line {mark.line + 1}, column {mark.column + 1}" if mark else ""
    sys.exit(f"ERROR: {path} is not valid YAML{where}")
if data is None:
    data = {}
if not isinstance(data, dict):
    sys.exit(f"ERROR: {path} must be a mapping of key: value")

out = {}
for key, value in data.items():
    if value is None or value == "":
        continue
    if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        sys.exit(f"ERROR: {key!r} is not a valid secret name")
    # YAML turns unquoted 0123, 12e45 or yes into numbers or booleans, mangling the value.
    if not isinstance(value, str):
        sys.exit(f"ERROR: {key} is not a string in {path}: quote its value")
    name = RENAME.get(key, key.upper())
    if name in out:
        sys.exit(f"ERROR: {name} is set twice in {path}")
    out[name] = value

if mode == "names":
    for name in out:
        print(name)
else:
    json.dump(out, sys.stdout)
PY
}

if [[ "$dry_run" == true ]]; then
  convert names
  exit 0
fi

if ! command -v doppler >/dev/null 2>&1; then
  echo "ERROR: the doppler CLI is not installed (https://docs.doppler.com/docs/install-cli)" >&2
  exit 1
fi

umask 077
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
chmod 0700 "$dir"
convert json > "$dir/secrets.json"
count="$(convert names | wc -l | tr -d ' ')"
if [[ "$count" -eq 0 ]]; then
  echo "ERROR: $secrets_file has no non-empty values to upload" >&2
  exit 1
fi

if [[ "$create_config" == true ]]; then
  env -u DOPPLER_TOKEN doppler configs create "$config" --project "$project"
fi
env -u DOPPLER_TOKEN doppler secrets upload "$dir/secrets.json" --project "$project" --config "$config" --silent

echo "Uploaded $count secret(s) to Doppler $project/$config:"
convert names | sed 's/^/  /'
