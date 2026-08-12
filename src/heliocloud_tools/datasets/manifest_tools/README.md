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
