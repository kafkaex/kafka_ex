#!/usr/bin/env bash
# CI wrapper for `mix test`: test output must not be parsed as GitHub workflow commands.
set -uo pipefail

token="$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
echo "::stop-commands::${token}"
mix test "$@"
status=$?
echo "::${token}::"
exit "$status"
