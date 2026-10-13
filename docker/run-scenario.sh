#!/bin/bash
# Runs a compose scenario detached, waits for every container, and
# requires exit code 0 from each. Usage:
#
#   docker/run-scenario.sh docker/envs/replication.env
set -u

ENV_FILE="${1:?usage: run-scenario.sh <env-file>}"
SERVICES="node-a node-b node-c"

docker compose --env-file "$ENV_FILE" up -d
FAIL=0
for svc in $SERVICES; do
    code=$(docker wait "$svc")
    echo "$svc exited with code $code"
    [ "$code" = "0" ] || FAIL=1
done
docker compose --env-file "$ENV_FILE" down --volumes
exit $FAIL
