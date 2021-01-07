#!/bin/bash

# Runs the test suite for the travis build
#
# If COVERALLS is true, then we report test coverage to coveralls.
#
# This script could be used for local testing as long as COVERALLS is not set.

set -ex

cd $(dirname $0)/..
PROJECT_DIR=$(pwd)

export MIX_ENV=test

if [ "${CREDO}" = true ]
then
  cd ${PROJECT_DIR}
  MIX_ENV=dev mix credo
fi

TEST_COMMAND=test

export TEST_COMMAND

ALL_TESTS=${PROJECT_DIR}/scripts/all_tests.sh

# Retry if it doesn't work the first time
${ALL_TESTS} || ${ALL_TESTS}
