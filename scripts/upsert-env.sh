#!/usr/bin/env bash
set -euo pipefail

# With two arguments the value comes on stdin; a terminal there means it was forgotten.
if [[ $# -lt 2 || $# -gt 3 ]] || [[ $# -eq 2 && -t 0 ]]; then
  echo "Usage: $0 <env-file> <key> [value]  (omit value to read it from stdin)" >&2
  exit 1
fi

env_file="$1"
key="$2"
# Secrets must come in on stdin: argv is visible to every local user.
if [[ $# -eq 3 ]]; then
  value="$3"
else
  value="$(cat)"
  # A blank would silently overwrite a good secret.
  if [[ -z "$value" ]]; then
    echo "$0: empty value on stdin for $key" >&2
    exit 1
  fi
  # Owner-only, whether we create the file or append to an existing one.
  umask 077
  if [[ -f "$env_file" ]]; then
    chmod 600 "$env_file"
  fi
fi
line="${key}=${value}"

dirname="$(dirname "$env_file")"
if [[ "$dirname" != "." ]]; then
  mkdir -p "$dirname"
fi

if [[ ! -f "$env_file" ]]; then
  printf '%s\n' "$line" > "$env_file"
  exit 0
fi

if grep -q "^${key}=" "$env_file"; then
  tmp_file="$(mktemp)"
  # A failed awk or mv must not leave the value behind in $TMPDIR.
  trap 'rm -f "$tmp_file"' EXIT
  # ENVIRON, not -v: awk's argv is as visible as ours.
  value="$value" awk -v key="$key" '
    $0 ~ "^" key "=" {
      print key "=" ENVIRON["value"]
      next
    }
    { print }
  ' "$env_file" > "$tmp_file"
  mv "$tmp_file" "$env_file"
else
  printf '%s\n' "$line" >> "$env_file"
fi
