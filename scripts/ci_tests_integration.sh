#!/bin/bash

# Runs integration tests for CI
#
# Tests new client (Kayrock), consumer groups, and general integration
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

TEST_COMMAND=${TEST_COMMAND:-test}

# Run integration tests with retry logic
mix $TEST_COMMAND \
  --only integration \
  --only consumer_group \
  --only new_client "$@" \
  || mix $TEST_COMMAND --failed \
  || mix $TEST_COMMAND --failed
