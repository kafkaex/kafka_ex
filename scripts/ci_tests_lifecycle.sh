#!/bin/bash

# Runs lifecycle tests for CI (Phase 4 from INTEGRATION_TEST_ROADMAP.md)
#
# Tests topic admin, client lifecycle, stream consumption, and application flows
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

TEST_COMMAND=${TEST_COMMAND:-test}

# Run lifecycle tests with retry logic
mix $TEST_COMMAND \
  --only lifecycle "$@" \
  || mix $TEST_COMMAND --failed \
  || mix $TEST_COMMAND --failed
