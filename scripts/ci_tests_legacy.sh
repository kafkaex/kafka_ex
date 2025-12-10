#!/bin/bash

# Runs legacy server tests for CI
#
# Tests Kafka server versions 0.8, 0.9, 0.10.x compatibility
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

TEST_COMMAND=${TEST_COMMAND:-test}

# Run legacy server tests with retry logic
mix $TEST_COMMAND \
  --only server_0_p_8_p_0 \
  --only server_0_p_9_p_0 \
  --only server_0_p_10_p_1 \
  --only server_0_p_10_and_later "$@" \
  || mix $TEST_COMMAND --failed \
  || mix $TEST_COMMAND --failed
