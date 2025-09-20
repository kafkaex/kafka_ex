#!/bin/bash
set -euo pipefail

if [[ "$(uname -m)" == "arm64" || "$(uname -m)" == "aarch64" ]]; then
  BASE="-f docker-compose-arm.yml"
else
  BASE="-f docker-compose.yml"
fi

echo "Starting Kafka with mode: $BASE"
docker compose $BASE  up -d