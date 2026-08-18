This includes a tool to convert a sorted S3 MANIFEST.csv into a set of per-dataid catalog indexes and main catalog.json index.

Currently works for CDAWeb, presumes the following:

1) obtain current SPDF holdings as 'spdf_curr'

2) obtain current SPDF metadata 'all.xml' and convert to catalog form

   xml2json2.py TBD

2) obtain current ODR <MANIFEST> CSV in form 's3key,filesize'
   (via Omar)

3) Compare the two (20-40 min task):
    sh manifest_prefilter.sh <MANIFEST> spdf_curr
   This generates <MANIFEST>.filtered (files ODR owns that are in CDAWeb)
              and <MANIFEST>.deleteme (files in ODR that are not in CDAWeb)
   (there is a 3rd category, files in CDAWeb that are not in ODR)

4) Generate new CloudCatalog indices via filtered
   manifest2indices --archive cdaweb --catalog FROMABOVE --manifest FROMABOVE

5) Merge new catalog to existing catalog
   TBD

6) Copy indices, new catalog to S3 staging

7) Run test script on temp holdings


An XML or CSV file containing dataid descriptions and regexes to extract dates from filenames.  Currently a CDAWeb example 'sample_data/smallall.xml' is provided.

An S3 CSV manifest, arbitrary fields so long as the last two files in a line are the full filename, then the filesize in bytes.  This file must be sorted in (a) dataid order and (b) time order for a given dataid.  In most cases this is just a straight alphabetic sort of the filename, as generally files are in some form of '*<dataid>*<year-leading date>*'.  Currently a CDAWeb example 'sample_data/smallsorted.csv' is provided.
# How to convert an AWS Manifest to CloudCatalog indices and metadata

How to convert a raw S3 MANIFEST.csv into a set of per-dataid catalog
indexes and main catalog.json index.  Examples provided using
HelioCloud ODR as a test example.

Note this requires the manifest be in format 'filename,filesize'.

The core 'manifest2indices.py' expects a sorted manifest, so we provide
pre-processing scripts as well.

## Steps

### fetching and preparing the manifest(s)

1) obtain current raw ODR <MANIFEST> CSV in form 's3key,filesize'
   (via ops)

2) 00_filter_manifest.sh [MANIFEST.csv]

   Splits the manifest into sorted outputs by source aka the first part of
   the file path, also filters on .cdf/.nc/.fits/.fts, produces:
     * [MANIFEST]_spdf.csv     # ^spdf/ and data ext
     * [MANIFEST]_sdac.csv     # ^sdac/ and data ext 
     * [MANIFEST]_contrib.csv  # ^contrib/ and data ext
     * [MANIFEST]_other.csv    # not spdf/sdac/contrib but data ext
     * [MANIFEST]_nondata.csv  # everything without those data extensions

(est. time on a 26GB manifest was 41 minutes. Sorting was the slowest step,
taking 2/3rds of the total time.)

### fetching and updating the metadata (catalog.json)

3) Copy the current 'catalog.json' for the above disk, e.g.

   aws s3 cp s3://gov-nasa-hdrl-data1/catalog.json .

4) Optionally, current SPDF metadata 'all.xml' and convert to catalog form

   * python ../cdaweb/cdaweb_xml2json.py --fetchxml -o catalog-cdaweb.json
   * cloudcatalog-update-json catalog.json catalog-cdaweb.json --verbose

### CDAWeb-specific step to validate holdings

5) Inherit output 'odr_index_me.list' from (or re-run)

   ../cdaweb/stage2_spdf_to_odr.sh [MANIFEST]_spdf.csv

   This creates a valid CDAWeb 'odr_index_me.list' file which validates
   our holdings against the canonical CDAWeb holdings

### Actually generate the new indices and update the metadata

6) Process each:

   * python manifest2indices.py --archive cdaweb --manifest odr_index_me.list --catalog catalog.json
   * python manifest2indices.py --archive sdac --manifest [MANIFEST]_sdac.csv --catalog catalog.json
   * python manifest2indices.py --archive contrib --manifest [MANIFEST]_contrib.csv --catalog catalog.json

### Test and stage

7) Manually inspect or otherwise deal with [MANIFEST]_other.csv, which probably
   lacks metadata in catalog.json.  Or process, if metadata exists:
   
   python manifest2indices.py --manifest [MANIFEST]_other.csv --catalog catalog.json
   
8) Likewise, manually inspect/spot check for validity of [MANIFEST]_nondata.csv

9) Copy indices, new catalog to S3 staging

10) Run test script on staging items

### Finalize

11) Move from staging to prod, re-test, done!
