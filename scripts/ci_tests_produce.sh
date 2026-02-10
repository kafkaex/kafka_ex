#!/bin/bash

# Runs produce tests for CI (Phase 2 from INTEGRATION_TEST_ROADMAP.md)
#
# Tests batch produce, reliability (acks), and partitioner behavior
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

TEST_COMMAND=${TEST_COMMAND:-test}

# Run produce tests with retry logic
mix $TEST_COMMAND \
  --only produce "$@" \
  || mix $TEST_COMMAND --failed \
  || mix $TEST_COMMAND --failed
