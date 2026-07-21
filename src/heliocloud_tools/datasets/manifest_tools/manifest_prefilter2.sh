#!/usr/bin/env bash
#
# Usage: ./manifest_prefilter.sh ODRMANIFEST SPDFCURRENT
#
# Output:
#   MANIFEST.filtered   - ODRMANIFEST lines whose normalized 1st field exists
#                         in SPDFCURRENT (normalized 4th/last field)
#   MANIFEST.deleteme   - ODRMANIFEST lines whose normalized 1st field does NOT
#                         exist in SPDFCURRENT
#   SPDFCURRENT.updates - SPDFCURRENT lines whose normalized last field does NOT
#                         exist in ODRMANIFEST
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
OUT_UPDATES="${SPDFCURRENT}.updates"

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
#    - Compare keys and output to filtered / deleteme / updates
########################################
awk -F'\t' \
    -v keep="$OUT_FILTERED" \
    -v drop="$OUT_DELETEME" \
    -v updates="$OUT_UPDATES" \
    -v curfile="$TMP_SPDFCURRENT_SORTED" '
BEGIN {
    keep_fp = keep
    drop_fp = drop
    upd_fp  = updates

    # Initialize SPDFCURRENT stream
    cur_eof = (getline cur_line < curfile) <= 0
    if (!cur_eof) {
        split(cur_line, a, "\t")
        cur_key  = a[1]
        cur_orig = cur_line
        sub(/^[^\t]*\t/, "", cur_orig)  # remove key and tab -> original current line
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

    # 1) Emit all CURRENT-only entries with key < man_key as updates
    while (!cur_eof && cur_key < man_key) {
        print cur_orig >> upd_fp
        cur_eof = (getline cur_line < curfile) <= 0
        if (!cur_eof) {
            split(cur_line, a, "\t")
            cur_key  = a[1]
            cur_orig = cur_line
            sub(/^[^\t]*\t/, "", cur_orig)
        }
    }

    # 2) Compare keys for this manifest entry
    if (!cur_eof && cur_key == man_key) {
        # match => keep
        print man_line >> keep_fp

        # advance CURRENT once so we don’t re-match the same key
        cur_eof = (getline cur_line < curfile) <= 0
        if (!cur_eof) {
            split(cur_line, a, "\t")
            cur_key  = a[1]
            cur_orig = cur_line
            sub(/^[^\t]*\t/, "", cur_orig)
        }
    } else {
        # no match => delete (in manifest only)
        print man_line >> drop_fp
    }
}

END {
    # Any remaining CURRENT lines after MANIFEST EOF are also updates
    while (!cur_eof) {
        print cur_orig >> upd_fp
        cur_eof = (getline cur_line < curfile) <= 0
        if (!cur_eof) {
            split(cur_line, a, "\t")
            cur_key  = a[1]
            cur_orig = cur_line
            sub(/^[^\t]*\t/, "", cur_orig)
        }
    }

    close(curfile)
    close(keep_fp)
    close(drop_fp)
    close(upd_fp)
}
' "$TMP_ODRMANIFEST_SORTED"
