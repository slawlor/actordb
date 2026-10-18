#!/usr/bin/env bash
# Exercise the application protocol from a separate client container, including
# a real ingress-node SIGKILL and restart. Run from the repository root.
set -euo pipefail

compose_cmd=(docker compose --project-name actordb-client-test --file docker-compose.client.yml)
finished=0

cleanup() {
    if [ "$finished" -ne 1 ]; then
        "${compose_cmd[@]}" logs --no-color --tail 300 || true
    fi
    "${compose_cmd[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

container_id() {
    "${compose_cmd[@]}" ps --all --quiet "$1"
}

wait_marker() {
    local service="$1" marker="$2" timeout="$3" container deadline
    container=$(container_id "$service")
    [ -n "$container" ] || { echo "missing container for $service"; return 1; }
    deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if docker logs "$container" 2>&1 | grep -Fx "$marker" >/dev/null; then
            return 0
        fi
        if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
            echo "$service exited before $marker"
            return 1
        fi
        sleep 1
    done
    echo "timed out waiting for $service: $marker"
    return 1
}

wait_stopped() {
    local service="$1" timeout="$2" container deadline
    container=$(container_id "$service")
    [ -n "$container" ] || { echo "missing container for $service"; return 1; }
    deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
            return 0
        fi
        sleep 1
    done
    echo "timed out waiting for $service to stop"
    return 1
}

echo "Starting three database nodes and the external SDK client"
"${compose_cmd[@]}" up --detach
wait_marker external-client CLIENT_WAITING_FOR_NODE_A_FAILURE 120
echo "SIGKILLing node-a after external client verification"
docker kill --signal SIGKILL "$(container_id node-a)" >/dev/null
wait_marker external-client CLIENT_FAILOVER_OK 90
echo "Restarting node-a after external client failover"
docker start "$(container_id node-a)" >/dev/null
wait_marker external-client SCENARIO_OK 90

# The client must finish successfully, rather than merely print a marker.
wait_stopped external-client 20
client_code=$(docker wait "$(container_id external-client)")
[ "$client_code" = 0 ] || { echo "external client exited $client_code"; exit 1; }
for service in node-a node-b node-c; do
    docker kill --signal SIGTERM "$(container_id "$service")" >/dev/null
done
for service in node-a node-b node-c; do
    wait_stopped "$service" 30
    code=$(docker wait "$(container_id "$service")")
    [ "$code" = 0 ] || { echo "$service exited $code"; exit 1; }
done
"${compose_cmd[@]}" logs --no-color --tail 60
finished=1
echo "External client scenario OK"
