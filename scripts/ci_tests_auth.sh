#!/bin/bash

# Runs authentication tests for CI
#
# Tests SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

TEST_COMMAND=${TEST_COMMAND:-test}

# Run SASL authentication tests with retry logic
mix $TEST_COMMAND \
  --only sasl "$@" \
  || mix $TEST_COMMAND --failed \
  || mix $TEST_COMMAND --failed
