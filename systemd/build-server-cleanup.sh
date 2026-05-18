#!/usr/bin/env bash
# Removes orphaned CI service containers (ci-pg-*, ci-redis-*, ci-*) that have
# been running longer than MAX_AGE_HOURS. These are spun up by runner jobs and
# should exit when the job finishes; stragglers indicate a crashed/killed runner.
set -euo pipefail

MAX_AGE_HOURS="${MAX_AGE_HOURS:-6}"
THRESHOLD=$(( MAX_AGE_HOURS * 3600 ))
NOW=$(date +%s)
REMOVED=0

while IFS= read -r line; do
  id=$(echo "$line" | awk '{print $1}')
  created_at=$(echo "$line" | awk '{print $2 " " $3}')
  name=$(echo "$line" | awk '{print $4}')

  created_epoch=$(date -d "$created_at" +%s 2>/dev/null || date -j -f "%Y-%m-%d %H:%M:%S" "$created_at" +%s 2>/dev/null || echo 0)
  age=$(( NOW - created_epoch ))

  if [[ $age -gt $THRESHOLD ]]; then
    echo "Removing orphaned container: name=$name id=${id:0:12} age=${age}s"
    docker rm -f "$id" || true
    REMOVED=$(( REMOVED + 1 ))
  fi
done < <(docker ps --filter 'name=ci-' --format '{{.ID}} {{.CreatedAt}} {{.Names}}' 2>/dev/null)

echo "Cleanup done: removed=$REMOVED threshold=${MAX_AGE_HOURS}h"
