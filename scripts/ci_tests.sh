#!/bin/bash

# Runs the test suite for the travis build
#
# If COVERALLS is true, then we report test coverage to coveralls.
#
# This script could be used for local testing as long as COVERALLS is not set.

export MIX_ENV=test

if [ "$COVERALLS" = true ]
then
  echo "Coveralls will be reported"
  TEST_COMMAND=coveralls.travis
else
  TEST_COMMAND=test
fi

mix "$TEST_COMMAND" --include integration --include consumer_group --include server_0_p_9_p_0 

# sometimes the first test run fails due to broker issues and we need to run it again
#    (we should strive to remove this but it is necessary for now)
if [ $? -eq 0 ]
then
  echo "First tests passed, skipping repeat"
else
  echo "Repeating tests"
  mix "$TEST_COMMAND" --include integration --include consumer_group --include server_0_p_9_p_0 --include server_0_p_8_p_0
fi
