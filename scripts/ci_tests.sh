#!/bin/bash

# Runs the test suite for the travis build
#
# If COVERALLS is true, then we report test coverage to coveralls.
#
# This script could be used for local testing as long as COVERALLS is not set.

set -ex

export MIX_ENV=test

if [ "$CREDO" = true ]
then
  MIX_ENV=dev mix credo
fi

if [ "$COVERALLS" = true ]
then
  echo "Coveralls will be reported"
  TEST_COMMAND=coveralls.travis
else
  TEST_COMMAND=test
fi

INCLUDED_TESTS="--include integration --include consumer_group --include server_0_p_10_and_later --include server_0_p_9_p_0 --include server_0_p_8_p_0 --include new_client"

# Retry if it doesn't work the first time
mix "$TEST_COMMAND" $INCLUDED_TESTS || mix "$TEST_COMMAND" $INCLUDED_TESTS
