#!/usr/bin/env bash

# Usage: ./strip_year_prefix.sh inputfile
# Returns unique prefixes of the first filepath field,
# stopping before the first 4-digit sequence.

if [ $# -lt 1 ]; then
  echo "Usage: $0 inputfile" >&2
  exit 1
fi

inputfile="$1"

awk -F',' '
{
  path = $1

  # Remove first occurrence of 4 consecutive digits and everything after
  sub(/[0-9]{4}.*/, "", path)

  # Remove trailing slash if any
  sub(/\/$/, "", path)

  if (!(path in seen)) {
    print path
    seen[path] = 1
  }
}' "$inputfile"
