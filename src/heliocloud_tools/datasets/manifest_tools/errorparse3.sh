#!/usr/bin/env bash

# Usage: ./errorparse3.sh inputfile
# For each line, looks at the first comma-separated field (a path),
# removes everything from the first 4-digit year onward,
# then removes the last remaining path component.
#
# Example:
#   asda/sadas/XXX/1985  -> asda/sadas
#   data/FMI/kilp/2000   -> data/FMI

if [ $# -lt 1 ]; then
  echo "Usage: $0 inputfile" >&2
  exit 1
fi

inputfile="$1"

awk -F',' '
{
  path = $1

  # 1) Remove first occurrence of 4 consecutive digits and everything after
  sub(/[0-9][0-9][0-9][0-9].*/, "", path)

  # 2) Remove trailing slash if any
  sub(/\/$/, "", path)

  # 3) Strip the last remaining path component:
  #    find last "/" and keep everything before it
  last = 0
  i = index(path, "/")
  while (i > 0) {
    last = i
    i = index(substr(path, last + 1), "/")
    if (i > 0) i += last
  }

  if (last > 0) {
    path = substr(path, 1, last - 1)
  } else {
    path = ""
  }

  if (path != "" && !(path in seen)) {
    print path
    seen[path] = 1
  }
}' "$inputfile"
