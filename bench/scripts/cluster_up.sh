#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../.."

if [[ "$(uname -m)" == "arm64" || "$(uname -m)" == "aarch64" ]]; then
  BASE=docker-compose-arm.yml
else
  BASE=docker-compose.yml
fi

COMPOSE=(docker compose -f "$BASE" -f bench/docker/compose.latency.yml)
"${COMPOSE[@]}" up -d
DEADLINE=$((SECONDS + 180))

inspect() {
  local id
  id=$("${COMPOSE[@]}" ps -a -q "$1")
  [[ -n "$id" ]] && docker inspect -f "$2" "$id"
}

not_ready() {
  local svc state
  for svc in kafka-1 kafka-2 kafka-3; do
    [[ "$(inspect "$svc" '{{.State.Health.Status}}' 2>/dev/null)" == "healthy" ]] || { echo "$svc is not healthy"; return; }
  done
  for svc in scram-setup kafka-setup; do
    state=$(inspect "$svc" '{{.State.Status}} {{.State.ExitCode}}' 2>/dev/null) || true
    if [[ "$state" =~ ^exited\ ([1-9][0-9]*)$ ]]; then
      echo "FATAL $svc exited with code ${BASH_REMATCH[1]}"
      return
    fi
    [[ "$state" == "exited 0" ]] || { echo "$svc has not exited 0"; return; }
  done
  curl -sf http://localhost:8474/version >/dev/null || { echo "toxiproxy is not answering"; return; }
}

while reason=$(not_ready); [[ -n "$reason" ]]; do
  if [[ "$reason" == FATAL\ * ]]; then
    echo "${reason#FATAL }" >&2
    exit 1
  fi
  if ((SECONDS >= DEADLINE)); then
    echo "cluster not ready after 180s: $reason" >&2
    exit 1
  fi
  sleep 2
done
echo "cluster ready"
