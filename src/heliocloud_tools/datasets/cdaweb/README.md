# ABOUT

Simple shell scripts for fetching delta updates of CDAWeb for S3 holdings
(in ODR, with hard-coded destinations)

Currently codes for s3://gov-nasa-hdrl-data1/spdf/

Also includes helper scripts for CDAWeb specifics.

Fetching the CDAWeb inventory from their website takes 4-8 minutes.
Our 'stage2' processing takes est. 6-8 minutes to chug through the 4-8GB files.
'stage3' to make the final curl fetch script is <10 seconds.

## CORE SEQUENCE for creating a fetch roster

==> stage1_getCDAWeb_inventory.sh <==
Fetches the current SPDF filelist (CDF and netCDF files only)
  (Derived from cdaweb's spdf_filelist.sh script)
usage: sh stage1_getCDAWeb_inventory.sh
Output is 'spdf_curr'

==> stage2_spdf_to_odr.sh <==
Compares the above spdf_curr against an ODR manifest.csv to generate the
  delta fetch list AND the delete list
usage: sh stage2_spdf_to_odr.sh [manifest.csv] [spdf_curr]
Output is 'fetch_cdaweb_for_odr.list',
          'delete_cdaweb_from_odr.list', and
	  'odr_index_me.list'

==> stage3_generate_S3_cp.sh <==
Converts the above fetch_cdaweb_for_odr.list filelist into a 'copy to S3'
  script that, when run, uses curl to copy files into
  s3://scratch-data-ops/spdf/
usage: sh stage3_generate_S3_cp.sh
Output is 'fetch_cdaweb_for_odr.sh'

## LOGIC

stage1 is just a copy of the CDAWeb 'fetch me' script, that also filters
for only .cdf/.nc files.

stage2 uses the output of stage1_getCDAWeb_Inventory.sh ('spdf_curr') and
generates the fetch, delete, and index lists, in format 'filename,filesize'

stage3 just takes the fetch file and makes a curl script out of it.

Stage 2 notes:
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


## HELPER SCRIPTS

cdaweb_xml2json.py: convert CDAWeb's 'all.xml' into our 'catalog.json' format
                    optionally can fetch the all.xml from CDAWeb
  usage: python cdaweb_xml2json.py -x all.xml -o catalog-cdaweb.json --pretty
    or   python cdaweb_xml2json.py --fetchxml -o catalog-cdaweb.json --pretty

cdawebxml2txt.py: writes each 'all.xml' XML element dataid into 'dataids.txt'

check_cdaweb_times.py: checks 'filelist.gz' for parseable ISO-like times,
                       tallies error count plus logs into 'errorfiles.pkl'
		       
find_big_cdaweb_files.py: Given a CDAWeb 'filelist.gz', tells us how many
                          files >5GB exist, stores file names in 'bigfiles.pkl'

odr_testing.py: Spot checks ODR catalog holdings via CloudCatalog API
