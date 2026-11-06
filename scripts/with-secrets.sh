#!/bin/sh
# Entrypoint for the app image: for each NAME_FILE pointing under the secrets root,
# export NAME from that file, unset NAME_FILE, then exec the command. Keeps secrets
# out of the container config (docker inspect) while the binaries still read env.
set -eu

# Fixed on purpose: tests run a copy with this line rewritten, never an env knob.
SECRETS_ROOT=/run/secrets/

nl='
'
# ENVIRON keys are names only, so a multi-line value cannot inject a fake one.
set -f
names=$(awk 'BEGIN { for (k in ENVIRON) print k }')

for name in $names; do
  # Upper-case NAME_FILE with a single underscore before FILE (Grafana's __FILE is not ours).
  case "$name" in
    *__FILE | ADMIN_KEYPAIR_FILE | INSTANCE_SEED_FILE) continue ;;
    *_FILE) ;;
    *) continue ;;
  esac
  case "$name" in
    *[!A-Z0-9_]* | [0-9]*) continue ;;
  esac
  eval "path=\${$name}"
  case "$path" in
    *..*) continue ;;
    "$SECRETS_ROOT"*) ;;
    *) continue ;;
  esac
  var=${name%_FILE}
  if [ ! -f "$path" ] || [ ! -r "$path" ]; then
    echo "with-secrets: cannot read the file for $var" >&2
    exit 1
  fi
  # The x sentinel keeps every trailing newline through $(...); then drop exactly one.
  value=$(cat "$path" && printf x) || { echo "with-secrets: cannot read the file for $var" >&2; exit 1; }
  value=${value%x}
  value=${value%"$nl"}
  if [ -z "$value" ]; then
    echo "with-secrets: the file for $var is empty" >&2
    exit 1
  fi
  export "$var=$value"
  unset "$name"
done

exec "$@"
