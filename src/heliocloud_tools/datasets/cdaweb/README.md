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
