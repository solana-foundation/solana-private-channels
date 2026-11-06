#!/usr/bin/env bash
set -euo pipefail

# The with-secrets entrypoint turns X_FILE=/run/secrets/... into X, and nothing else.
# Runs a copy of the wrapper with its fixed root rewritten to a temp dir.
# Run from the repo root: ./scripts/tests/with-secrets.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0
fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

ROOT="$TMP/run/secrets"
mkdir -p "$ROOT"
sed "s|^SECRETS_ROOT=/run/secrets/\$|SECRETS_ROOT=$ROOT/|" "$REPO/scripts/with-secrets.sh" >"$TMP/with-secrets"
chmod +x "$TMP/with-secrets"
grep -q "^SECRETS_ROOT=$ROOT/\$" "$TMP/with-secrets" || fail "could not rewrite SECRETS_ROOT in the wrapper copy"

# Runs the wrapper under /bin/sh with exactly the given env.
ws() { env -i PATH=/usr/bin:/bin "$@"; }

# 1. Exactly one trailing newline is stripped; inner newlines stay.
printf 'one\n' >"$ROOT/A"
printf 'two\n\n' >"$ROOT/B"
printf 'l1\nl2\n' >"$ROOT/C"
out="$(ws A_FILE="$ROOT/A" B_FILE="$ROOT/B" C_FILE="$ROOT/C" "$TMP/with-secrets" \
  sh -c 'printf "[%s][%s][%s]" "$A" "$B" "$C"')"
[[ "$out" == $'[one][two\n][l1\nl2]' ]] || fail "newline handling: got $(printf %q "$out")"

# 2. X_FILE overrides a non-empty X.
out="$(ws A=stale A_FILE="$ROOT/A" "$TMP/with-secrets" sh -c 'printf %s "$A"')"
[[ "$out" == one ]] || fail "X_FILE must override X (got '$out')"

# 3. A missing or empty file exits 1, naming X, never the value, and never runs the command.
printf 'SNTLvalue\n' >"$ROOT/S"
: >"$ROOT/E"
for case in missing empty; do
  [[ $case == missing ]] && f="$ROOT/nope" || f="$ROOT/E"
  set +e
  out="$(ws S=SNTLvalue SECRET_X_FILE="$f" "$TMP/with-secrets" sh -c 'echo RAN' 2>&1)"
  rc=$?
  set -e
  [[ $rc == 1 ]] || fail "$case file: exit $rc, want 1"
  [[ "$out" == *SECRET_X* ]] || fail "$case file: error does not name SECRET_X: $out"
  [[ "$out" == *RAN* ]] && fail "$case file: the command ran"
  [[ "$out" == *SNTLvalue* ]] && fail "$case file: a value leaked"
done
if [[ $(id -u) != 0 ]]; then
  printf 'x\n' >"$ROOT/U"
  chmod 000 "$ROOT/U"
  ws U_FILE="$ROOT/U" "$TMP/with-secrets" true 2>/dev/null && fail "unreadable file accepted"
else
  echo "skip: unreadable-file case (running as root)"
fi

# 4. Anything outside the contract is ignored and left as-is.
mkdir -p "$TMP/outside"
printf 'out\n' >"$TMP/outside/O"
out="$(ws O_FILE="$TMP/outside/O" P_FILE="$ROOT/../secrets/A" GF_X__FILE="$ROOT/A" \
  ADMIN_KEYPAIR_FILE=/tmp/x lower_FILE="$ROOT/A" "$TMP/with-secrets" env | sort)"
for want in "O_FILE=$TMP/outside/O" "P_FILE=$ROOT/../secrets/A" "GF_X__FILE=$ROOT/A" \
  "ADMIN_KEYPAIR_FILE=/tmp/x" "lower_FILE=$ROOT/A"; do
  grep -qxF "$want" <<<"$out" || fail "ignored var changed: $want"
done
for unwanted in O= P= GF_X_= GF_X= ADMIN_KEYPAIR= lower=; do
  grep -q "^$unwanted" <<<"$out" && fail "a var outside the contract was exported: $unwanted"
done

# 5. A multi-line value carrying a fake FOO_FILE line injects nothing.
out="$(ws "MULTI=a
FOO_FILE=$ROOT/A" "$TMP/with-secrets" env)"
grep -q '^FOO=' <<<"$out" && fail "a multi-line value injected FOO"

# 6. X_FILE is absent from the child env and X is present.
out="$(ws A_FILE="$ROOT/A" "$TMP/with-secrets" env)"
grep -q '^A_FILE=' <<<"$out" && fail "A_FILE still in the child env"
grep -qx 'A=one' <<<"$out" || fail "A missing from the child env"

# 7. Args with spaces pass through, the exit code propagates, and exec keeps the PID.
out="$(ws "$TMP/with-secrets" sh -c 'printf "%s|" "$@"' _ 'a b' c)"
[[ "$out" == 'a b|c|' ]] || fail "args mangled: $out"
set +e
ws "$TMP/with-secrets" sh -c 'exit 7'
rc=$?
set -e
[[ $rc == 7 ]] || fail "exit code $rc, want 7"
# With exec the command replaces the wrapper, so its parent is not the wrapper.
out="$(ws "$TMP/with-secrets" sh -c 'tr "\0" " " </proc/$PPID/cmdline')"
[[ "$out" == *"$TMP/with-secrets"* ]] && fail "wrapper did not exec: parent is $out"

# 8. With no _FILE vars, the child env equals the input. PWD is exported by any sh.
want="$(env -i PATH=/usr/bin:/bin FOO=bar 'SP=a b' env | grep -v '^PWD=' | sort)"
got="$(env -i PATH=/usr/bin:/bin FOO=bar 'SP=a b' "$TMP/with-secrets" env | grep -v '^PWD=' | sort)"
[[ "$want" == "$got" ]] || fail "env changed without _FILE vars"

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "with-secrets tests passed"
