#!/bin/bash

# Runs consume tests for CI (Phase 3 from INTEGRATION_TEST_ROADMAP.md)
#
# Tests batch fetch, offset handling, and multi-partition operations
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

TEST_COMMAND=${TEST_COMMAND:-test}

# Run consume tests with retry logic
mix $TEST_COMMAND \
  --only consume "$@" \
  || mix $TEST_COMMAND --failed \
  || mix $TEST_COMMAND --failed
