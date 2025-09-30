#!/bin/bash

cd $(dirname $0)/..

TEST_COMMAND=${TEST_COMMAND:-test}

mix $TEST_COMMAND \
  --include integration \
  --include consumer_group \
  --include server_0_p_10_and_later \
  --include server_0_p_10_p_1 \
  --include server_0_p_9_p_0 \
  --include server_0_p_8_p_0 \
  --include sasl \
  --include new_client "$@"
