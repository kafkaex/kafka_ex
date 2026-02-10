#!/bin/bash

# Runs auth tests for CI (Phase 5 from INTEGRATION_TEST_ROADMAP.md)
#
# Tests SASL/PLAIN, SASL/SCRAM, OAUTHBEARER, and SSL authentication
#
# This script could be used for local testing.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

TEST_COMMAND=${TEST_COMMAND:-test}

# Run auth tests with retry logic
mix $TEST_COMMAND \
  --only auth "$@" \
  || mix $TEST_COMMAND --failed \
  || mix $TEST_COMMAND --failed
