#!/usr/bin/env bash
# usage: ./dedup_by_key.sh input.csv > output.csv

echo "#regex, sample failing file, number of failed files"

awk -F',' '
{
  rawkey = $3
  val = $2

  # strip leading re.compile and parentheses
  key = rawkey
  sub(/^re\.compile\('\s*/', "", key)
  sub(/'\)\s*$/', "", key)

  # track unique field2 values per key
  if (!seen_pair[key, val]++) {
    count[key]++
  }

  # remember a sample field2 for this key (the first one seen)
  if (!(key in sample)) {
    fname = val
    gsub(/^.*\//, "", fname)
    sample[key] = fname
  }
}
END {
  for (k in count) {
    # print: sample_field2, key_field3, unique_field2_count_for_key
    print k "," sample[k] "," count[k]
  }
}
' "$1" | sort -t',' -k3,3nr

