#!/usr/bin/env bash
set -euo pipefail

# rotate-secret.sh: edits exactly one secrets.yml line (or one Doppler key), keeps the
# file 0600, never puts a value on a child's argv, and stages JWT keypair rotations safely.
# Run from the repo root: ./scripts/tests/rotate-secret.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$REPO/scripts/rotate-secret.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0
fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

# Every command the script runs through PATH is recorded with its argv.
SHIMS="$TMP/bin"
mkdir -p "$SHIMS"
for c in python3 openssl mv chmod mktemp rm cat cp dirname basename tr grep sed awk od head; do
  real="$(command -v "$c")" || continue
  cat >"$SHIMS/$c" <<EOF
#!/usr/bin/env bash
printf '%s\n' "$c \$*" >>"$TMP/argv.log"
exec "$real" "\$@"
EOF
  chmod +x "$SHIMS/$c"
done
# Fake Doppler: records argv, stdin and whether DOPPLER_TOKEN leaked through.
cat >"$SHIMS/doppler" <<EOF
#!/usr/bin/env bash
printf '%s\n' "doppler \$*" >>"$TMP/argv.log"
printf '%s\n' "doppler \$* token=\${DOPPLER_TOKEN-unset}" >>"$TMP/doppler.log"
case "\$2" in
  set) printf '%s=%s\n' "\$3" "\$(cat)" >>"$TMP/doppler.stdin" ;;
  get) cat "$TMP/doppler.get.\$3" 2>/dev/null || true ;;
esac
EOF
chmod +x "$SHIMS/doppler"

rot() { PATH="$SHIMS:/usr/bin:/bin" DOPPLER_TOKEN=dp.st.SNTLtoken "$SCRIPT" "$@"; }

seed() {
  cat >"$TMP/secrets.yml" <<'EOF'
# header comment
postgres_password: old_pg
postgres_runtime_password: old_runtime
jwt_signing_key: oldsigning
jwt_public_key: oldpublic
jwt_public_key_previous: ""
notify_webhook: ""
ghcr_user: u
EOF
  chmod 600 "$TMP/secrets.yml"
  : >"$TMP/argv.log"
}

val() { python3 -c 'import sys, yaml; print(yaml.safe_load(open(sys.argv[1]))[sys.argv[2]] or "")' "$TMP/secrets.yml" "$1"; }

# Whether $1 is a base58 64-byte keypair whose seed, wrapped the way auth wraps it,
# gives the base58 public key $2 under openssl.
pair_matches() {
  python3 - "$1" "$2" <<'EOF'
import subprocess, sys
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def b58decode(text):
    n = 0
    for char in text:
        n = n * 58 + ALPHABET.index(char)
    body = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return b"\0" * (len(text) - len(text.lstrip("1"))) + body
keypair, public = b58decode(sys.argv[1]), b58decode(sys.argv[2])
der = bytes.fromhex("302e020100300506032b657004220420") + keypair[:32]
derived = subprocess.run(
    ["openssl", "pkey", "-inform", "DER", "-pubout", "-outform", "DER"],
    input=der, capture_output=True,
).stdout[-32:]
sys.exit(0 if len(keypair) == 64 and keypair[32:] == public == derived else 1)
EOF
}

# --- File backend ---
seed
cp "$TMP/secrets.yml" "$TMP/before.yml"
rot postgres_runtime_password --secrets-file "$TMP/secrets.yml" >"$TMP/out" 2>&1 || fail "generated rotation failed: $(cat "$TMP/out")"
new="$(val postgres_runtime_password)"
[[ ${#new} == 32 && "$new" =~ ^[0-9a-f]+$ ]] || fail "postgres password not 32 hex chars"
[[ "$(diff "$TMP/before.yml" "$TMP/secrets.yml" | grep -c '^[<>]')" == 2 ]] || fail "more than the target line changed"
[[ "$(stat -c %a "$TMP/secrets.yml")" == 600 ]] || fail "secrets.yml no longer 0600"
grep -qF "$new" "$TMP/argv.log" && fail "the generated value reached a child argv"
grep -qF "$new" "$TMP/out" && fail "the generated value was printed"
grep -q "ansible-playbook" "$TMP/out" || fail "no redeploy command printed"
[[ -z "$(find "$TMP" -maxdepth 1 -name '*.tmp*' -o -maxdepth 1 -name 'secrets.yml.*')" ]] || fail "temp files left behind"

rot jwt_signing_key --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 || fail "jwt stage failed"
[[ "$(val jwt_public_key_previous)" == oldpublic ]] || fail "jwt stage did not move the current public key to previous"
signing="$(val jwt_signing_key)"
public="$(val jwt_public_key)"
pair_matches "$signing" "$public" || fail "the staged signing key does not match its public key"
grep -qF "$signing" "$TMP/argv.log" && fail "the signing key reached a child argv"
rot jwt_signing_key --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 && fail "a second jwt stage was accepted"
rot jwt_signing_key --finish --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 || fail "jwt --finish failed"
[[ -z "$(val jwt_public_key_previous)" ]] || fail "--finish did not clear previous"
[[ "$(val jwt_signing_key)" == "$signing" && "$(val jwt_public_key)" == "$public" ]] || fail "--finish changed the current keypair"
rot jwt_signing_key --finish --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 && fail "--finish without previous was accepted"

# A first keypair has no previous key to keep verifying.
seed
sed -i 's/^jwt_public_key: .*/jwt_public_key: ""/' "$TMP/secrets.yml"
rot jwt_signing_key --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 || fail "first jwt keypair failed"
[[ -z "$(val jwt_public_key_previous)" ]] || fail "a first keypair set a previous key"
pair_matches "$(val jwt_signing_key)" "$(val jwt_public_key)" || fail "the first keypair does not match"

# Supplied values round-trip through YAML, whatever they contain.
for v in "a#b" "k: v" "it's" 'say "hi"' "https://hooks.invalid/T0/B1/xyz?a=1&b=2"; do
  seed
  printf '%s' "$v" | rot notify_webhook --value-stdin --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 || fail "supplied value '$v' refused"
  [[ "$(val notify_webhook)" == "$v" ]] || fail "value '$v' did not round-trip (got '$(val notify_webhook)')"
  grep -qF -- "$v" "$TMP/argv.log" && fail "supplied value '$v' reached a child argv"
done
# A key not yet in the file is appended.
seed
printf 'SNTLys' | rot yellowstone_token --value-stdin --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 || fail "append failed"
[[ "$(val yellowstone_token)" == SNTLys ]] || fail "appended key not readable"
printf 'a\nb' | rot notify_webhook --value-stdin --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 && fail "a value with a newline was accepted"
rot ghcr_token --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 && fail "ghcr_token without --value-stdin was accepted"
rot no_such_key --secrets-file "$TMP/secrets.yml" >/dev/null 2>&1 && fail "an unknown key was accepted"

# A hand-edited current value with a trailing comment that holds quotes is read whole.
for line in "jwt_public_key: 'cur''pubcurpub' # it's 'old'" \
  'jwt_public_key: "curpubcurpub" # say "x"' \
  "jwt_public_key: curpubcurpub # it's \"old\""; do
  seed
  sed -i "s|^jwt_public_key: .*|$line|" "$TMP/secrets.yml"
  want="$(val jwt_public_key)"
  rot jwt_signing_key --secrets-file "$TMP/secrets.yml" >"$TMP/out" 2>&1 || fail "jwt stage with a commented value failed: $(cat "$TMP/out")"
  [[ "$(val jwt_public_key_previous)" == "$want" ]] || fail "commented value read wrong: got '$(val jwt_public_key_previous)', want '$want'"
done
# A block scalar is refused with a clear error and the file left as it was.
for block in '|' '>-' '|2'; do
  seed
  python3 - "$TMP/secrets.yml" "$block" <<'EOF'
import sys
p, ind = sys.argv[1], sys.argv[2]
s = open(p).read().replace("postgres_password: old_pg\n", "postgres_password: " + ind + "\n  old\n  pg\n")
open(p, "w").write(s)
EOF
  cp "$TMP/secrets.yml" "$TMP/before.yml"
  rot postgres_password --secrets-file "$TMP/secrets.yml" >"$TMP/out" 2>&1 && fail "block scalar '$block' was rewritten"
  grep -q "multi-line" "$TMP/out" || fail "block scalar '$block': no clear error: $(cat "$TMP/out")"
  cmp -s "$TMP/before.yml" "$TMP/secrets.yml" || fail "block scalar '$block': the file changed"
done

# --- Doppler backend ---
: >"$TMP/argv.log"
rot postgres_password --backend doppler --project P --config C >/dev/null 2>&1 || fail "doppler rotation failed"
grep -qx "doppler secrets set POSTGRES_PASSWORD --silent --project P --config C token=unset" "$TMP/doppler.log" \
  || fail "doppler call wrong or DOPPLER_TOKEN passed: $(cat "$TMP/doppler.log")"
dv="$(grep '^POSTGRES_PASSWORD=' "$TMP/doppler.stdin" | cut -d= -f2-)"
[[ ${#dv} == 32 ]] || fail "doppler did not receive the value on stdin"
grep -qF "$dv" "$TMP/argv.log" && fail "the doppler value reached argv"
printf 'https://hooks.invalid/x' | rot notify_webhook --value-stdin --backend doppler --project P --config C >/dev/null 2>&1
grep -q "secrets set ALERT_WEBHOOK_URL " "$TMP/doppler.log" || fail "notify_webhook must map to ALERT_WEBHOOK_URL"
printf 'https://hooks.invalid/g' | rot alert_webhook_url --value-stdin --backend doppler --project P --config C >/dev/null 2>&1
grep -q "secrets set GRAFANA_ALERT_WEBHOOK_URL " "$TMP/doppler.log" || fail "alert_webhook_url must map to GRAFANA_ALERT_WEBHOOK_URL"
rot postgres_password --backend doppler >/dev/null 2>&1 && fail "doppler without --project/--config was accepted"
# JWT staging on Doppler: the current public key moves to previous, refused when previous is set.
printf 'curpublic' >"$TMP/doppler.get.JWT_PUBLIC_KEY"
rot jwt_signing_key --backend doppler --project P --config C >/dev/null 2>&1 || fail "doppler jwt stage failed"
grep -qx 'JWT_PUBLIC_KEY_PREVIOUS=curpublic' "$TMP/doppler.stdin" || fail "doppler jwt stage did not set previous"
pair_matches "$(grep '^JWT_SIGNING_KEY=' "$TMP/doppler.stdin" | cut -d= -f2-)" \
  "$(grep '^JWT_PUBLIC_KEY=' "$TMP/doppler.stdin" | cut -d= -f2-)" || fail "doppler jwt keypair does not match"
printf 'x' >"$TMP/doppler.get.JWT_PUBLIC_KEY_PREVIOUS"
rot jwt_signing_key --backend doppler --project P --config C >/dev/null 2>&1 && fail "doppler jwt second stage accepted"

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "rotate-secret tests passed"
