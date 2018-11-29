#! /bin/sh

# WARN: when changing something here, there should probably also be a change in scripts/ci_tests.sh

mix test --include integration --include consumer_group --include server_0_p_10_and_later  --include server_0_p_9_p_0 --include server_0_p_8_p_0
