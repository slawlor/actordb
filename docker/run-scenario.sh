#!/bin/bash
# Runs a compose scenario detached, waits for every container, and
# requires exit code 0 from each. Usage:
#
#   docker/run-scenario.sh docker/envs/replication.env
set -u

ENV_FILE="${1:?usage: run-scenario.sh <env-file>}"
SERVICES="node-a node-b node-c"

# Always tear the cluster down, even if a step above fails.
cleanup() {
    docker compose --env-file "$ENV_FILE" down --volumes >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== scenario: $ENV_FILE ($(date -u +%H:%M:%S) UTC) ==="
docker compose --env-file "$ENV_FILE" up -d
FAIL=0
for svc in $SERVICES; do
    code=$(docker wait "$svc")
    echo "--- $svc exited with code $code"
    [ "$code" = "0" ] || FAIL=1
done

if [ "$FAIL" != "0" ]; then
    echo "=== FAILED: container logs ==="
    docker compose --env-file "$ENV_FILE" logs --no-color --tail 300
else
    echo "=== scenario OK ==="
fi
exit $FAIL
