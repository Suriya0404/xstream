#!/usr/bin/env bash
# x-stream sandbox startup script
# Starts all infrastructure via Docker Compose and the dev UI via Vite.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="$REPO_ROOT/docker/docker-compose.yml"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  x-stream sandbox"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Preflight ──────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
  echo "✗ Docker not found. Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
  exit 1
fi

if ! docker info &>/dev/null; then
  echo "✗ Docker daemon is not running. Start Docker Desktop first."
  exit 1
fi

# ── Install backend deps (local dev) ─────────────────────
if ! pip3 show fastapi &>/dev/null 2>&1; then
  echo "→ Installing backend Python dependencies..."
  pip3 install -q -r "$REPO_ROOT/backend/requirements.txt"
fi

# ── Pull & start infrastructure ───────────────────────────
echo ""
echo "→ Starting infrastructure (Kafka, Flink, ScyllaDB, MySQL, ClickHouse)..."
docker compose -f "$COMPOSE" up -d --build 2>&1 | grep -E "(Started|Running|Error|Warning|Pulling|Building)" || true

# ── Wait for MySQL ────────────────────────────────────────
echo ""
echo "→ Waiting for MySQL to be ready..."
for i in {1..30}; do
  if docker compose -f "$COMPOSE" exec -T mysql mysqladmin ping -u root -pxstream123 --silent 2>/dev/null; then
    echo "  MySQL ready ✓"
    break
  fi
  sleep 2
done

# ── Start backend locally (for dev hot-reload) ────────────
echo ""
echo "→ Starting backend API on http://localhost:8000 ..."
cd "$REPO_ROOT/backend"
uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!
echo "  Backend PID: $BACKEND_PID"

# ── Start frontend ────────────────────────────────────────
echo ""
echo "→ Starting frontend on http://localhost:5173 ..."
cd "$REPO_ROOT"
npm run dev &
FRONTEND_PID=$!
echo "  Frontend PID: $FRONTEND_PID"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Services:"
echo "  • UI:             http://localhost:5173"
echo "  • Backend API:    http://localhost:8000"
echo "  • Flink UI:       http://localhost:8082"
echo "  • Flink SQL GW:   http://localhost:8083"
echo "  • Kafka:          localhost:9092"
echo "  • ScyllaDB CQL:   localhost:9042"
echo "  • ClickHouse:     http://localhost:8123"
echo "  • MySQL:          localhost:3306  (root/xstream123)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Press Ctrl+C to stop frontend and backend."
echo "(Infrastructure containers keep running — use 'docker compose -f docker/docker-compose.yml down' to stop.)"

# Wait and cleanup
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; echo 'Stopped.'" INT TERM
wait
