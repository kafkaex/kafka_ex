#!/bin/bash

# Runs the test suite for the travis build

set -ev

# first test run - tends to work the kinks out of the kafka brokers
#    (we should strive to remove this but it is necessary for now)
mix test --include integration --include consumer_group --include server_0_p_9_p_0 || true

if [ "$COVERALLS" = true ]
then
  MIX_ENV=test mix coveralls.travis --include integration --include consumer_group --include server_0_p_9_p_0
else
  mix test --cover --include integration --include consumer_group --include server_0_p_9_p_0
fi
