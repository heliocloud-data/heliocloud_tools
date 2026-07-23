#!/bin/sh

: <<'COMMENT'
Given the output of stage1_getCDAWeb_Inventory.sh is 'spdf_curr'
This file takes [manifest.csv] plus [spdf_curr] to generate three files.
All 3 are in 'filename,filesize' format

1) fetch_cdaweb_for_odr.list = files to fetch from CDAWeb
2) delete_from_odr.list = files that are in ODR but no longer in CDAWeb
     (note that files that are in both but have different sizes are
      not scheduled for deletion, as the above fetch will overwrite them)
3) odr_index_me.list = files currently in ODR that are also in the
     current CDAWeb, and hence safe to index.  Does not include the
     above 'fetch' files since the are not yet in ODR.

 It can deal with spdf 4 fields whitespace-separated starting with pub/ and
 our manifest 2 fields comma-separated starting with spdf/cdaweb

 Note if file sizes differ, we do not mark for deletion, just fetch (overwrite)

 For indexing, there are 2 approaches:
    a) run stage1 to get latest CDAWeb, index only current valids that we have
    b) after moving fetches to staging, update manifest.csv and re-run this
       with the older spdf_curr (not fetching a new one via stage1_)
       If you do this approach, in theory fetch_cdaweb_for_odr.list should
       be zero size as the two should match (ignoring deletes)
       And, if you did do the deletes, delete_from_odr.list will also be zero.

COMMENT

echo "usage: sh stage2_spdf_to_odr.sh [manifest.csv] [spdf_curr]"

manifest="manifest.csv"
if [ $# -ge 1 ] && [ -n "$1" ]; then
  manifest="$1"
fi

if [ ! -f "$manifest" ]; then
    echo "Warning: manifest file '$manifest' does not exist." >&2
    exit 1
fi

spdf_curr="spdf_curr"
if [ $# -ge 2 ] && [ -n "$2" ]; then
  spdf_curr="$2"
fi

if [ ! -f "$spdf_curr" ]; then
    echo "Warning: manifest file '$spdf_curr' does not exist." >&2
    exit 1
fi

echo "Using manifest $manifest and current SPDF holdings $spdf_curr"

# Extract paths from newest list (last field), changing pub/ to spdf/cdaweb/
awk '{
    path = $NF
    sub(/^pub\//, "spdf/cdaweb/", path)
    print path "," $(NF-1)
}' "$spdf_curr" | sort > temp_spdf_curr

grep "spdf/cdaweb/data" "$manifest" | egrep "\.cdf|\.nc" | sort > temp_odr_cdaweb

# Output 1: files in spdf_curr but not in manifest.csv
comm -23 temp_spdf_curr temp_odr_cdaweb > fetch_cdaweb_for_odr.list

# Output 2: deletion list, spdf files in manifest.csv but not in spdf_curr
comm -23 temp_odr_cdaweb temp_spdf_curr > tmp_in_odr_not_in_cdaweb

# needs second pass to handle same filename but different file sizes,
# so we remove from delete list if also fetching same

awk -F',' '
NR==FNR {
    # First file: Bkeep → remember filenames
    keep[$1] = 1
    next
}
{
    # Second file: Amaybe → output if filename not in Bkeep
    if (!($1 in keep)) {
        print $0
    }
}
' fetch_cdaweb_for_odr.list tmp_in_odr_not_in_cdaweb > delete_from_odr.list

# Output 3: index-me list, files that already exist in both
comm -12 temp_odr_cdaweb temp_spdf_curr > odr_index_me.list

echo "Created fetch_cdaweb_for_odr.list, delete_from_odr.list, odr_index_me.list."
echo "Line counts for inputs:"
wc -l temp_spdf_curr temp_odr_cdaweb
echo "Line counts for outputs:"
wc -l fetch_cdaweb_for_odr.list delete_from_odr.list odr_index_me.list

mms_count=$(grep -c mms fetch_cdaweb_for_odr.list)
nonmms_count=$(grep -c -v mms fetch_cdaweb_for_odr.list)
echo "($mms_count files are MMS fetches, $nonmms_count are non-MMS fetches)"

echo "Count of unique dataids in delete file (full list in 'del_dataids.txt':"
awk -F/ '{print $4}' delete_from_odr.list | sort -u | tee del_dataids.txt | wc -l

echo "Count of unique dataids in fetch file (full list in 'fetch_dataids.txt':"
awk -F/ '{print $4}' fetch_cdaweb_for_odr.list | sort -u | tee fetch_dataids.txt | wc -l
