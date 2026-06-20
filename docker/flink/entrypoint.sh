#!/bin/bash
set -e

# Start SQL Gateway in background when running as jobmanager
if [ "$1" = "jobmanager" ]; then
  echo "Starting Flink SQL Gateway on port 8083..."
  /opt/flink/bin/sql-gateway.sh start
  echo "SQL Gateway started."
fi

# Hand off to the official Flink entrypoint
exec /docker-entrypoint.sh "$@"
