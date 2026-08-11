#!/usr/bin/env bash
# takes an ODR manifest of filepath,filesize and splits it by source
# note individual sorts faster than using awk to do in one pass
#
# Assumes potentially unsorted input, filters and sorts.
#
# sample times: filtering 11min 26G-23G, sorting 30 min eek 23G, spdf in 3 min 3.7G, sdac in 10 min 19G, contrib in 1 min 0.2G, non-matches 4min 0G

if [ $# -eq 0 ]; then
    echo "Usage: 00_filter_manifest.sh [manifest.csv]"
    exit 1
fi
infile="$1"
stem="${infile%.csv}"

out_spdf="${stem}_spdf.csv"        # ^spdf/ and data ext
out_sdac="${stem}_sdac.csv"        # ^sdac/ and data ext
out_contrib="${stem}_contrib.csv"  # ^contrib/ and data ext
out_other="${stem}_other.csv"      # not spdf/sdac/contrib but data ext
out_nondata="${stem}_nondata.csv"  # everything without those data extensions

tmp_filtered="filtered.csv"

SECONDS=0

if [ ! -f "$tmp_filtered" ]; then
    echo 'Filtering file types'
    egrep "\.(cdf|nc|fits|fts)" "$infile" >"${tmp_filtered}"
    echo "Elapsed: ${SECONDS}s, now generating exceptions list"
    egrep -v "\.(cdf|nc|fits|fts)" "$infile" >"${out_nondata}"
    echo "Elapsed: ${SECONDS}s"
fi

echo "Splitting in sub files"
egrep "^spdf/" "${tmp_filtered}" >"${out_spdf}"
egrep "^sdac/" "${tmp_filtered}" >"${out_sdac}"
egrep "^contrib/" "${tmp_filtered}" >"${out_contrib}"
egrep -v "^(spdf|sdac|contrib)/" "${tmp_filtered}" >"${out_other}"

echo "Elapsed: ${SECONDS}s, now sorting each subset"
sort "${out_spdf}" -o "${out_spdf}"
sort "${out_sdac}" -o "${out_sdac}"
sort "${out_contrib}" -o "${out_contrib}"
sort "${out_other}" -o "${out_other}"

echo "Elapsed: ${SECONDS}s, done."
