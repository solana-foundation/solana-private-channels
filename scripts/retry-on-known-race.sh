#!/usr/bin/env bash
set -uo pipefail

# Runs a test command and retries it only if the test validator's broadcast stage panicked.
# Any other failure fails immediately, so a real regression is never retried away.

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <max-attempts> <command...>" >&2
  exit 2
fi

max_attempts="$1"
shift

# The panic the validator's broadcast thread prints when it falls behind the root.
signature="must have a block id"
log="$(mktemp)"
trap 'rm -f "$log"' EXIT

status=1
for attempt in $(seq 1 "$max_attempts"); do
  "$@" 2>&1 | tee "$log"
  status="${PIPESTATUS[0]}"
  if [ "$status" -eq 0 ]; then
    exit 0
  fi
  if ! grep -q "$signature" "$log"; then
    exit "$status"
  fi
  if [ "$attempt" -lt "$max_attempts" ]; then
    echo "Attempt $attempt of $max_attempts hit the validator broadcast panic; retrying." >&2
  fi
done

echo "ERROR: still hitting the validator broadcast panic after $max_attempts attempts." >&2
exit "$status"
