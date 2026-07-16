#!/usr/bin/env bash
#
# Usage: ./manifest_prefilter.sh ODRMANIFEST SPDFCURRENT
#
# Output:
#   MANIFEST.filtered  - ODRMANIFEST lines whose normalized 1st field exists
#                        in SPFCURRENT (normalized 4th field)
#   MANIFEST.deleteme  - ODRMANIFEST lines whose normalized 1st field does NOT
#                        exist in SPDFCURRENT
#
# Notes:
#   - ODRMANIFEST: 2 fields, comma-separated; match on field1 after stripping "spdf/cdaweb/"
#   - SPDFCURRENT:  4 fields, whitespace-separated; match on last field after stripping "pub/"
#
#   This script:
#     1) Builds sorted, normalized key+line files on disk.
#     2) Runs a streaming merge in awk.
#
#   Large-file friendly (handles multi-GB via sort + streaming).

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 ODRMANIFEST SPDFCURRENT" >&2
    exit 1
fi

ODRMANIFEST="$1"
SPDFCURRENT="$2"

OUT_FILTERED="${ODRMANIFEST}.filtered"
OUT_DELETEME="${ODRMANIFEST}.deleteme"

TMP_SPDFCURRENT_SORTED="$(mktemp)"
TMP_ODRMANIFEST_SORTED="$(mktemp)"

cleanup() {
    rm -f "$TMP_SPDFCURRENT_SORTED" "$TMP_ODRMANIFEST_SORTED"
}
trap cleanup EXIT

########################################
# 1) Normalize + sort SPDFCURRENT
#    - Extract last field
#    - Strip leading "pub/"
#    - Output: key<TAB>original-current-line
########################################
awk '
{
    f = $NF
    sub(/^pub\//, "", f)
    # key \t original line
    print f "\t" $0
}
' "$SPDFCURRENT" | sort -t$'\t' -k1,1 > "$TMP_SPDFCURRENT_SORTED"

########################################
# 2) Normalize + sort ODRMANIFEST
#    - Field1 is key, strip leading "spdf/cdaweb/"
#    - Output: key<TAB>original-manifest-line
########################################
awk -F',' '
{
    key = $1
    sub(/^spdf\/cdaweb\//, "", key)
    # key \t original line
    print key "\t" $0
}
' "$ODRMANIFEST" | sort -t$'\t' -k1,1 > "$TMP_ODRMANIFEST_SORTED"

########################################
# 3) Streaming merge in awk
#    - Walk ODRMANIFEST.sorted sequentially
#    - Walk SPDFCURRENT.sorted sequentially via getline
#    - Compare keys and output to filtered / deleteme
########################################
awk -F'\t' -v keep="$OUT_FILTERED" -v drop="$OUT_DELETEME" -v curfile="$TMP_SPDFCURRENT_SORTED" '
BEGIN {
    keep_fp = keep
    drop_fp = drop

    # Initialize SPDFCURRENT stream
    cur_eof = (getline cur_line < curfile) <= 0
    if (!cur_eof) {
        split(cur_line, a, "\t")
        cur_key = a[1]
    }
}

# Process ODRMANIFEST.sorted: key<TAB>orig-manifest-line
{
    man_key = $1

    # Reconstruct the original ODRMANIFEST line from fields 2..NF
    man_line = $2
    for (i = 3; i <= NF; i++) {
        man_line = man_line OFS $i
    }

    # Advance SPDFCURRENT until cur_key >= man_key or EOF
    while (!cur_eof && cur_key < man_key) {
        cur_eof = (getline cur_line < curfile) <= 0
        if (!cur_eof) {
            split(cur_line, a, "\t")
            cur_key = a[1]
        }
    }

    if (!cur_eof && cur_key == man_key) {
        # match => keep
        print man_line >> keep_fp
    } else {
        # no match => delete
        print man_line >> drop_fp
    }
}

END {
    close(curfile)
    close(keep_fp)
    close(drop_fp)
}
' "$TMP_ODRMANIFEST_SORTED"
