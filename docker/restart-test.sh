#!/bin/bash
# Chaos scenario: SIGKILL node-a mid-cluster (a real kill -9, not a
# graceful shutdown), restart it, and require every node to converge.
# node-a's role detects the reboot via its sentinel file and asserts
# it picks up v2; node-b asserts the peer died and returned.
#
#   docker/restart-test.sh
set -euo pipefail

ENV_FILE="docker/envs/restart.env"

# Always tear the cluster down, even if a step below fails.
cleanup() {
    if [ "$?" -ne 0 ]; then
        docker compose --env-file "$ENV_FILE" logs --no-color --tail 300 || true
    fi
    docker compose --env-file "$ENV_FILE" down --volumes >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_marker() {
    local service="$1" marker="$2" deadline=$((SECONDS + 90))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if docker logs "$service" 2>&1 | grep -Fx "$marker" >/dev/null; then
            return 0
        fi
        if [ "$(docker inspect --format '{{.State.Running}}' "$service")" != true ]; then
            echo "$service exited before $marker"
            return 1
        fi
        sleep 1
    done
    echo "$service did not reach $marker before the deadline"
    return 1
}

wait_stopped() {
    local service="$1" deadline=$((SECONDS + 180))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if [ "$(docker inspect --format '{{.State.Running}}' "$service")" != true ]; then
            return 0
        fi
        sleep 1
    done
    echo "$service exceeded its final exit budget"
    return 1
}

echo "=== scenario: SIGKILL + restart ($(date -u +%H:%M:%S) UTC) ==="
docker compose --env-file "$ENV_FILE" up -d

echo "waiting for node-a to become ready..."
wait_marker node-a READY
# The survivor must hold the test value before the crash, and must observe
# the outage before restart. An immediate restart can hide the zero-peer gap
# from its polling task and make it mistake A's normal final exit for SIGKILL.
wait_marker node-b SURVIVOR_READY

echo "SIGKILLing node-a"
docker kill -s SIGKILL node-a >/dev/null
wait_marker node-b PEER_DIED
echo "restarting node-a"
docker start node-a >/dev/null

FAIL=0
for svc in node-a node-b node-c; do
    wait_stopped "$svc"
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
