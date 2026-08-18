#!/usr/bin/env bash
# Launches the three services as SEPARATE processes with combined, labeled output.
# They remain independent processes (real microservices) — just displayed together.
# Usage: ./run.sh [order_id]   (defaults to order-123; use order-fail to test failure)

set -euo pipefail

ORDER_ID="${1:-order-123}"
COMPOSE="docker compose -f infra/compose/docker-compose.yml"

echo "==> Resetting databases (down -v / up -d)..."
$COMPOSE down -v >/dev/null 2>&1
$COMPOSE up -d >/dev/null 2>&1

echo "==> Waiting for services to be healthy..."
# crude but effective: wait until all report healthy
for i in {1..30}; do
    if $COMPOSE ps | grep -q "unhealthy\|starting"; then
        sleep 2
    else
        break
    fi
done
sleep 3  # extra buffer for rabbit

# Kill all background service processes when the script exits (Ctrl+C included)
cleanup() {
    echo ""
    echo "==> Shutting down services..."
    kill 0 2>/dev/null
}
trap cleanup EXIT

echo "==> Starting Inventory, Payment, Order (order_id=$ORDER_ID)..."
echo ""

# Each service runs as its own process; sed prefixes its output so you can tell them apart.
uv run python -u -m inventory.consumer 2>&1 | sed 's/^/[INVENTORY] /' &
uv run python -u -m payment.consumer   2>&1 | sed 's/^/[PAYMENT]   /' &
sleep 2
uv run python -u -m order.main "$ORDER_ID" 2>&1 | sed 's/^/[ORDER]     /' &

# wait for all background jobs (keeps the script alive; Ctrl+C triggers cleanup)
wait
