#!/bin/bash

# Runs consumer group tests for CI (Phase 1 from INTEGRATION_TEST_ROADMAP.md)
#
# Tests consumer group lifecycle, async/sync consumers, rebalancing
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

TEST_COMMAND=${TEST_COMMAND:-test}

# Run consumer group tests with retry logic
mix $TEST_COMMAND \
  --only consumer_group "$@" \
  || mix $TEST_COMMAND --failed \
  || mix $TEST_COMMAND --failed
