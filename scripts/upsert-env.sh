#!/usr/bin/env bash
set -euo pipefail

# Env files can hold secrets, so every write lands as a new 0600 file owned by the caller.
umask 077

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
fi
line="${key}=${value}"

fail() {
  echo "upsert-env: $1" >&2
  exit 1
}

dirname="$(dirname -- "$env_file")"
if [[ "$dirname" != "." ]]; then
  mkdir -p -- "$dirname"
fi

# Writing through a symlink or into someone else's file could put the secret where others can read it.
if [[ -L "$env_file" ]]; then
  fail "refusing to write through a symlink, pass the real file instead: $env_file"
fi
if [[ -e "$env_file" ]]; then
  [[ -f "$env_file" ]] || fail "not a regular file: $env_file"
  [[ -O "$env_file" ]] || fail "not owned by $(id -un), fix its ownership for the user that runs the stack: $env_file"
  chmod 600 -- "$env_file"
fi

# The temp file sits next to the target, so the final mv is an atomic rename on the same filesystem.
tmp_file="$(mktemp "$dirname/.$(basename -- "$env_file").XXXXXX")"
trap 'rm -f -- "$tmp_file"' EXIT

if [[ -f "$env_file" ]] && grep -q "^${key}=" "$env_file"; then
  # ENVIRON, not -v: awk's argv is as visible as ours.
  value="$value" awk -v key="$key" '
    $0 ~ "^" key "=" {
      print key "=" ENVIRON["value"]
      next
    }
    { print }
  ' "$env_file" > "$tmp_file"
else
  if [[ -f "$env_file" ]]; then
    cat -- "$env_file" > "$tmp_file"
  fi
  printf '%s\n' "$line" >> "$tmp_file"
fi

chmod 600 "$tmp_file"
mv -f -- "$tmp_file" "$env_file"
