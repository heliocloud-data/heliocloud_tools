#!/bin/sh

echo "Given a list of CDAWeb files to copy (fetch_cdaweb_for_odr.list),"
echo "  creates script 'fetch_cdaweb_for_odr.sh' that copies via wget into S3,"

# assumes input is in format 'filename,filesize'

source="fetch_cdaweb_for_odr.list"
dest="fetch_cdaweb_to_scratch.sh"
httpsget="https://spdf.gsfc.nasa.gov"
s3dest="s3://scratch-data-ops/spdf" #_$(LC_ALL=C date +%d%b%Y)"

echo "... setting S3 destination in script to '$s3dest'"

awk -F',' '{
  # $1 = filename, $2 = filesize (ignored)
  file1 = $1
  file2 = file1
  sub(/^pub\//, "cdaweb/", file2)
  print "curl -sSL " httpsget "/" file1 " | aws s3 cp - " s3dest "/" file2
}' httpsget="$httpsget" s3dest="$s3dest" "$source" > "$dest"

echo "Done, made script '$dest'."
