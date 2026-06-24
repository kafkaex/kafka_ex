#!/usr/bin/env bash
# Fails if a test calls GenServer.stop/Supervisor.stop inside an on_exit callback,
# or uses the racy `if Process.alive?(x), do: (GenServer|Supervisor).stop` teardown.
# Teardown must use KafkaEx.TestSupport.ProcessHelpers.stop_safely/1.
set -euo pipefail

racy=$(grep -rnE --exclude-dir=support 'if Process\.alive\?\(.*\), do: (GenServer|Supervisor)\.stop' test/ || true)
inline=$(grep -rnE --exclude-dir=support 'on_exit.*(GenServer|Supervisor)\.stop' test/ || true)

if [ -n "$racy$inline" ]; then
  echo "Racy teardown found — use KafkaEx.TestSupport.ProcessHelpers.stop_safely/1:"
  echo "$racy"
  echo "$inline"
  exit 1
fi
echo "teardown check: clean"
