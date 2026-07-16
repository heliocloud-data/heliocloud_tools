#!/usr/bin/env bash

# Usage: ./strip_year_prefix.sh inputfile
# Returns unique first two parts of the first filepath field.

if [ $# -lt 1 ]; then
  echo "Usage: $0 inputfile" >&2
  exit 1
fi

inputfile="$1"

awk -F',' '
{
  # Take 1st field (the filepath)
  split($1, a, "/")
  # Join first two path components
  key = a[1] "/" a[2]

  if (!(key in seen)) {
    print key
    seen[key] = 1
  }
}' "$inputfile"
