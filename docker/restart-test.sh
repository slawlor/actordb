#!/bin/bash
# Chaos scenario: SIGKILL node-a mid-cluster (a real kill -9, not a
# graceful shutdown), restart it, and require every node to converge.
# node-a's role detects the reboot via its sentinel file and asserts
# it picks up v2; node-b asserts the peer died and returned.
#
#   docker/restart-test.sh
set -u

ENV_FILE="docker/envs/restart.env"

# Always tear the cluster down, even if a step below fails.
cleanup() {
    docker compose --env-file "$ENV_FILE" down --volumes >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose --env-file "$ENV_FILE" up -d

echo "waiting for node-a to become ready..."
READY=0
for _ in $(seq 1 90); do
    if docker logs node-a 2>&1 | grep -q "^READY$"; then
        READY=1
        break
    fi
    sleep 1
done
if [ "$READY" != "1" ]; then
    echo "node-a never became ready"
    exit 1
fi

echo "SIGKILLing node-a"
docker kill -s SIGKILL node-a >/dev/null
echo "restarting node-a"
docker start node-a >/dev/null

FAIL=0
for svc in node-a node-b node-c; do
    code=$(docker wait "$svc")
    echo "$svc exited with code $code"
    [ "$code" = "0" ] || FAIL=1
done
exit $FAIL
