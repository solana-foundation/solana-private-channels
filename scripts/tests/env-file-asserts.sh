#!/usr/bin/env bash
# Shared asserts for env-file tests. The sourcing test must define fail() and ROOT.

UPSERT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/upsert-env.sh"

# Pick the stat flavour once; on GNU, `stat -f FMT` would print filesystem info instead of failing.
if stat -c %a / >/dev/null 2>&1; then
  file_mode() { stat -c %a "$1"; }
  file_uid() { stat -c %u "$1"; }
else
  file_mode() { stat -f %Lp "$1"; }
  file_uid() { stat -f %u "$1"; }
fi

new_case() {
  mkdir "$ROOT/$1"
  printf '%s\n' "$ROOT/$1"
}

# Writes CONTENT to PATH with mode 0644, so a strict developer umask cannot make a case pass by accident.
seed() {
  printf '%s' "$2" > "$1"
  chmod 644 "$1"
}

# TMPDIR points at a missing dir, so a temp file made anywhere but next to the target fails the run.
NO_TMPDIR="$ROOT/no-tmpdir"

# Runs upsert-env.sh the way the auditor reproduced it, with umask 022.
upsert() {
  (umask 022 && TMPDIR="$NO_TMPDIR" "$UPSERT" "$@")
}

expect_refused() {
  if upsert "$@" 2>/dev/null; then
    fail "upsert-env.sh accepted $1"
  fi
}

assert_private() {
  [[ -f "$1" && ! -L "$1" ]] || fail "$1 is not a regular file"
  [[ "$(file_mode "$1")" == 600 ]] || fail "$1 has mode $(file_mode "$1"), want 600"
  [[ "$(file_uid "$1")" == "$(id -u)" ]] || fail "$1 is owned by uid $(file_uid "$1"), want $(id -u)"
}

assert_mode() {
  [[ "$(file_mode "$1")" == "$2" ]] || fail "$1 has mode $(file_mode "$1"), want $2"
}

assert_content() {
  cmp -s "$1" <(printf '%s' "$2") || fail "unexpected content in $1"
}

# Matches only this file's temp names, so neighbours like .env.devnet are not mistaken for leftovers.
assert_no_temp() {
  local leftover
  leftover="$(find "$1" -maxdepth 1 -name ".$2.??????" | head -n 1)"
  [[ -z "$leftover" ]] || fail "temp file left behind: $leftover"
}
