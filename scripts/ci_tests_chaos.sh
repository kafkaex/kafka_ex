#!/bin/bash

# Runs chaos tests for CI
#
# Tests network failure handling and resilience
# Uses Testcontainers - no external docker-compose required
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test
export ENABLE_TESTCONTAINERS=true

TEST_COMMAND=${TEST_COMMAND:-test}

# Run chaos tests
# Testcontainers will automatically start Kafka and Toxiproxy containers
mix $TEST_COMMAND --only chaos "$@" \
  || mix $TEST_COMMAND --only chaos --failed \
  || mix $TEST_COMMAND --only chaos --failed
