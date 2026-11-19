#!/bin/sh
echo "Given a list of CDAWeb files to copy (fetch_cdaweb_for_odr.list),"
echo "  creates script 'pull_data_to_sync.sh' that copies via curl into S3"

# assumes input is in format 'filename,filesize'

source="fetch_cdaweb_for_odr.list"
dest="pull_data_to_sync.sh"
httpsget="https://cdaweb.gsfc.nasa.gov/pub"
s3dest="s3://cdaweb-sync" #_$(LC_ALL=C date +%d%b%Y)"

echo "... setting S3 destination in script to '$s3dest'"

awk -F',' '{
  # $1 = filename, $2 = filesize (ignored)
  file1 = $1
  file2 = file1

  # Convert spdf/cdaweb/data/... to data/...
  sub(/^spdf\/cdaweb\//, "", file2)

  print "curl -sSL " httpsget "/" file2 " | aws s3 cp - " s3dest "/" file1
}' httpsget="$httpsget" s3dest="$s3dest" "$source" > "$dest"

echo "Done, made script '$dest'."
#
