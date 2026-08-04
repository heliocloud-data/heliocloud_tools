# new plan, reuse our fetch scripts 1-2-3 because they work!
'''
1) get new MANIFEST
2) run stage2_ (which filters on cdaweb, cdf/nc) to get index_me_
3) fetch current catalog.json and all.xml
4) run manifest2indices
5) merge catalog-stub with catalog
6) test in place
7) copy to staging



1) fetch current catalog.json (important!)
   'curl -O https://gov-nasa-hdrl-data1.s3.amazonaws.com/catalog.json'
2) fetch current cdaweb spdf_new, all.xml
   'heliocloud_tools/datasets/cdaweb/cdaweb_xml2json --fetchxml -o catalog-cdaweb.json'
   'heliocloud_tools/datasets/cdaweb/stage1_getCDAWeb_inventory.sh'
3) First do CDAWeb:
3a)filter 'manifest.csv' and split into csvs by spdf/sdac/contrib
   'heliocloud_tools/datasets/cdaweb/stage2_spdf_to_odr.sh'
   (generates 'manifest_odr_cdaweb.csv'
(make a simple manifest-to-spdf_curr and use their own diff script?)
   'filter_manifest -i manifest.CSV -o manifest_cdaweb.csv -filter 'spdf/''
   'manifest2csv -i manifest_cdaweb.csv -o spdf_curr'
   'heliocloud_tools/datasets/cdaweb/stage1_getCDAWeb_inventory.sh

5) generate indices/catalog from either raw csv or for cdaweb, the new diff-csv
6) test and validate
     'heliocloud_tools/datasets/cdaweb/odr_testing.py'
7) merge updated catalog-stubs into catalog.json
8) also generate the HTML indexes used by heliodata etc
9) somehow push to staging

scripts in play: see also cdaweb/README.md asap as above is a dup!?!?!
manifest_tools/
               manifest2indices.py
               errorparse[1-3].sh
datasets/cdaweb/cdaweb_xml2json.py
       check_cdaweb_xml.py (maybe?)
jul-21/sdac_to_csv.py
       processing.sh
       MISSING.sh
       xml2json[_v2].py
       ABOUT_ERRORS.txt (maybe)
metaindexing/cc_fetchtest.py
             cc_test_local.py
cloudcatalog/src/cloudcatalog/updater/catalog_updater.py
                              validators/*
   validate_cloudcatalog_api.pyuses 'dataids.txt' or 'all.xml' to verify they return valid HTML pages


# middle

odr2catalog.json??
  splits manifest into pieces
  calls cloudcatalog-manifest2indices on sdac, spdf, contrib




# OLD

egrep ".nc,|.cdf,|.fits,|.fts," manifest_Oct28.csv | sort | cat >manifest_sorted.csv
cp ~/gits/heliocloud/cloudcatalog/src/cloudcatalog/generators/sample_data/all.xml .
cloudcatalog-manifest2indices --manifest manifest_sorted.csv --xml-path ./all.xml

OR skip the grep and use --filter_filetypes with m2i
cp ~/gits/heliocloud/cloudcatalog/src/cloudcatalog/generators/sample_data/all.xml .
cloudcatalog-manifest2indices --manifest manifest_sorted.csv --xml-path ./all.xml --filter_filetypes

THEN
cloudcatalog-update-csv catalog.json updates.csv

FINALLY
copy new catalog.json and all the staged indices over
cloudcatalog-manifest2indices manifest_sorted_spdf.csv s3://gov-nasa-hdrl-data1/ --ensure_prefix spdf/cdaweb/data --xml_file all.xml --strip_me pub/data/
