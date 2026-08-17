#!/bin/bash
set -e

# Start the SQL Gateway when running as jobmanager. This runs in a background
# subshell because the JobManager itself must become the foreground (exec) process.
# We (1) wait for the JobManager REST endpoint so the gateway can attach to a live
# cluster, and (2) clear any stale PID file left from a previous container start —
# both were causes of the gateway silently failing to come up on restart, which
# surfaced to the app as "All connection attempts failed" / stale-session 500s.
if [ "$1" = "jobmanager" ]; then
  (
    rm -f /tmp/flink-*-sql-gateway*.pid 2>/dev/null || true
    echo "Waiting for JobManager REST endpoint before starting SQL Gateway..."
    for _ in $(seq 1 60); do
      if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
        break
      fi
      sleep 2
    done
    echo "Starting Flink SQL Gateway on 0.0.0.0:8083..."
    /opt/flink/bin/sql-gateway.sh start \
      -Dsql-gateway.endpoint.rest.address=0.0.0.0 \
      -Dsql-gateway.endpoint.rest.port=8083
    echo "SQL Gateway start invoked."
  ) &
fi

# Hand off to the official Flink entrypoint
exec /docker-entrypoint.sh "$@"
