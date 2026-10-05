#!/usr/bin/env bash
set -euo pipefail

# Tests for upsert-env.sh: every write must leave a 0600 file owned by the caller.
#
# Needs only bash and coreutils. Run from the repo root:
#   ./scripts/tests/upsert-env.test.sh

ROOT="$(mktemp -d)"
trap 'chmod -R u+w "$ROOT"; rm -rf "$ROOT"' EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

# shellcheck source=scripts/tests/env-file-asserts.sh
source "$(dirname "${BASH_SOURCE[0]}")/env-file-asserts.sh"

test_create() {
  local d
  d="$(new_case create)"
  upsert "$d/.env" K '[1,2,3]'
  assert_private "$d/.env"
  assert_content "$d/.env" $'K=[1,2,3]\n'
  assert_no_temp "$d" .env
}

test_append() {
  local d
  d="$(new_case append)"
  seed "$d/.env" $'A=1\n'
  upsert "$d/.env" K v
  assert_private "$d/.env"
  assert_content "$d/.env" $'A=1\nK=v\n'
  assert_no_temp "$d" .env
}

test_replace() {
  local d
  d="$(new_case replace)"
  seed "$d/.env" $'A=1\nK=old\nB=2\nK=dup\n'
  upsert "$d/.env" K new
  upsert "$d/.env" K new
  assert_private "$d/.env"
  assert_content "$d/.env" $'A=1\nK=new\nB=2\nK=new\n'
  assert_no_temp "$d" .env
}

test_refuses_symlink() {
  local d
  d="$(new_case symlink)"
  seed "$d/target" $'A=1\n'
  ln -s target "$d/.env"
  expect_refused "$d/.env" K v
  [[ -L "$d/.env" ]] || fail "symlink was replaced"
  assert_content "$d/target" $'A=1\n'
  assert_mode "$d/target" 644
}

test_refuses_dangling_symlink() {
  local d
  d="$(new_case dangling)"
  ln -s "$d/missing" "$d/.env"
  expect_refused "$d/.env" K v
  [[ ! -e "$d/missing" ]] || fail "wrote through a dangling symlink"
}

test_refuses_directory() {
  local d
  d="$(new_case directory)"
  mkdir "$d/.env"
  expect_refused "$d/.env" K v
  [[ -z "$(ls -A "$d/.env")" ]] || fail "wrote into the directory"
  assert_no_temp "$d" .env
}

# Only root can hand a file to another user, so this case runs as root only.
test_refuses_foreign_owner() {
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "SKIP: foreign owner (needs root)"
    return
  fi
  local d
  d="$(new_case foreign)"
  seed "$d/.env" $'A=1\n'
  chown nobody "$d/.env"
  expect_refused "$d/.env" K v
  assert_content "$d/.env" $'A=1\n'
  [[ "$(file_uid "$d/.env")" == "$(id -u nobody)" ]] || fail "owner changed"
}

# Root ignores directory permissions, so this case runs as non-root only.
test_unwritable_dir_fails_closed() {
  if [[ "$(id -u)" -eq 0 ]]; then
    echo "SKIP: unwritable directory (needs non-root)"
    return
  fi
  local d
  d="$(new_case readonly)"
  seed "$d/.env" $'K=old\n'
  chmod 500 "$d"
  expect_refused "$d/.env" K new
  chmod 700 "$d"
  assert_content "$d/.env" $'K=old\n'
  assert_mode "$d/.env" 600
  assert_no_temp "$d" .env
}

# A failing final mv must not leave the secret behind in a temp file.
test_failed_write_leaves_no_temp() {
  local d
  d="$(new_case failed-write)"
  mkdir "$d/bin"
  printf '#!/bin/sh\nexit 1\n' > "$d/bin/mv"
  chmod +x "$d/bin/mv"
  seed "$d/.env" $'K=old\n'
  PATH="$d/bin:$PATH" expect_refused "$d/.env" K new
  assert_content "$d/.env" $'K=old\n'
  assert_no_temp "$d" .env
}

test_creates_private_parent_dirs() {
  local d
  d="$(new_case nested)"
  upsert "$d/a/b/.env" K v
  assert_private "$d/a/b/.env"
  assert_mode "$d/a" 700
  assert_mode "$d/a/b" 700
}

# Callers pass a bare relative name like `.env`, so the temp must land in the current directory.
test_relative_path() {
  local d name
  d="$(new_case relative)"
  for name in .env 'my env'; do
    (cd "$d" && upsert "$name" K v)
    assert_private "$d/$name"
    assert_content "$d/$name" $'K=v\n'
    assert_no_temp "$d" "$name"
  done
}

all_tests=(test_create test_append test_replace test_refuses_symlink
  test_refuses_dangling_symlink test_refuses_directory test_refuses_foreign_owner
  test_unwritable_dir_fails_closed test_failed_write_leaves_no_temp test_creates_private_parent_dirs
  test_relative_path)

# Pass test names as arguments to run only those cases.
for t in "${@:-${all_tests[@]}}"; do
  "$t"
  echo "ok: $t"
done

echo "PASS: upsert-env.sh writes private env files"
