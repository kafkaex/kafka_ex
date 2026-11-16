#!/usr/bin/env bash
# CI: warn on the run summary when a test job passed only on retry.
# Usage: ci_retry_annotation.sh <job label> <first outcome> <retry-1 outcome> <retry-2 outcome>
set -euo pipefail

label="$1"
first="$2"
retry1="$3"
retry2="$4"

[[ "$label" =~ ^[A-Za-z0-9\ _-]+$ ]] || { echo "invalid job label" >&2; exit 1; }

[[ "$first" == "failure" ]] || exit 0

if [[ "$retry1" == "success" ]]; then
  retry=1
elif [[ "$retry2" == "success" ]]; then
  retry=2
else
  exit 0
fi

message="${label}: first run failed, passed on retry ${retry}"
echo "::warning title=Passed on retry::${message}"
echo "- :warning: ${message}" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
