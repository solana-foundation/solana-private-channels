# shellcheck shell=bash
# env-resolve.sh — resolve a key from env files the way docker compose does.
# Sourced by check-required-env.sh and migrate-stack.sh so the two never
# disagree about which value a service is actually going to be handed.
#
# Reads KEY=VALUE literally rather than sourcing, so a stray line in an env file
# cannot execute code.

# resolve_var <key> <env-file> [<env-file> ...]
resolve_var() {
  local key="$1"
  shift
  # An exported process-env value takes precedence over the files, even when
  # empty — that is what compose does, and a caller that disagreed would use the
  # file's value while compose injected the empty one.
  if [[ -n "${!key+set}" ]]; then
    printf '%s' "${!key}"
    return 0
  fi
  local val="" line f
  for f in "$@"; do
    # Last literal `KEY=` line wins; allow leading space and an `export` prefix.
    line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$f" | tail -n1 || true)"
    [[ -n "$line" ]] || continue
    val="${line#*=}"
    # Drop a trailing CR so CRLF files don't read as non-empty.
    val="${val%$'\r'}"
    # Strip one layer of surrounding quotes so KEY="" reads as empty.
    case "$val" in
      \"*\") val="${val#\"}" && val="${val%\"}" ;;
      \'*\') val="${val#\'}" && val="${val%\'}" ;;
    esac
  done
  printf '%s' "$val"
}
