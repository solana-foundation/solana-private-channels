#!/usr/bin/env bash
# Starts a background kubectl port-forward and waits until it answers, or stops one.
#   start <namespace> <svc/name|pod/name> <local:remote> <pidfile> | stop <pidfile>
# KUBECONFIG must point at the deploy's temporary kubeconfig.
set -euo pipefail

case "${1:-}" in
  start)
    ns="$2" target="$3" ports="$4" pidfile="$5"
    local_port="${ports%%:*}"
    setsid kubectl -n "$ns" port-forward --address 127.0.0.1 "$target" "$ports" \
      >"$pidfile.log" 2>&1 </dev/null &
    echo $! >"$pidfile"
    for _ in $(seq 1 60); do
      if (exec 3<>"/dev/tcp/127.0.0.1/$local_port") 2>/dev/null; then exit 0; fi
      if ! kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "port-forward to $ns/$target exited: $(tail -2 "$pidfile.log")" >&2
        exit 1
      fi
      sleep 0.5
    done
    echo "port-forward to $ns/$target did not answer on 127.0.0.1:$local_port" >&2
    exit 1
    ;;
  stop)
    pidfile="$2"
    [[ -f "$pidfile" ]] && kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile" "$pidfile.log"
    ;;
  *)
    echo "usage: $0 start <ns> <target> <local:remote> <pidfile> | stop <pidfile>" >&2
    exit 2
    ;;
esac
