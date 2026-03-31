#!/bin/sh

echo "usage: sh stage2_spdf_to_odr.sh [manifest.csv]"

manifest="manifest.csv"
if [ $# -ge 1 ] && [ -n "$1" ]; then
  manifest="$1"
fi

if [ ! -f "$manifest" ]; then
    echo "Warning: manifest file '$manifest' does not exist." >&2
    exit 1
fi

echo "Using manifest: $manifest"

# Extract paths from newest list (last field)
awk '{print $NF}' spdf_curr | sort > temp_spdf_curr

grep "spdf/cdaweb/data" "$manifest" | sort > manifest_odr_cdaweb.csv

# Extract & normalize paths from manifest
awk -F',' '{
    path = $1
    sub(/^spdf\/cdaweb\/data\//, "pub/data/", path)
    print path
}' manifest_odr_cdaweb.csv | sort > temp_odr_curr

# Output: files in spdf_curr but not in manifest.csv
comm -23 temp_spdf_curr temp_odr_curr > fetch_cdaweb_for_odr.list

echo "Created fetch_cdaweb_for_odr.list.  Line counts for inputs/output:"
wc -l temp_spdf_curr temp_odr_curr fetch_cdaweb_for_odr.list | sed '$d'

mms_count=$(grep -c mms fetch_cdaweb_for_odr.list)
nonmms_count=$(grep -c -v mms fetch_cdaweb_for_odr.list)
echo "($mms_count files are MMS fetches, $nonmms_count are non-MMS fetches)"

echo "Count of unique dataids in fetch file (full list in 'dataids.txt':"
awk -F/ '{print $4}' fetch_cdaweb_for_odr.list | sort -u | tee dataids.txt | wc -l

