#!/usr/bin/env bash
set -euo pipefail

# Env files can hold secrets, so every write lands as a new 0600 file owned by the caller.
umask 077

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <env-file> <key> <value>" >&2
  exit 1
fi

env_file="$1"
key="$2"
value="$3"
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
  awk -v key="$key" -v value="$value" '
    $0 ~ "^" key "=" {
      print key "=" value
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
