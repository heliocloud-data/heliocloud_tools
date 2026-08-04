# Indexing and Re-Indexing.

## Status

It's all mostly there, I just need to glue tools and proofing and also
a lot of testing tools for safety.  And, a way to document or script the
workflow for Omar esp. involving file renaming, inheriting the right file, etc.

(Also some of this isn't proper MD so ACE/ it to fix)

## ACTIONS:

add filter to manifest2indices.py to ignore 0-sized files
   code compare_catalogs.py incl. GUI
   update/write filter_manifest.sh optional pre-processor
   improve tests for both local and destination/alt-catalog plus final
   Ugh, create metadata for all SDAC, for contrib/fdl-sdoml, contrib/*


## Setup

Requires (a) MANIFEST.csv in form 'filename,filesize'
         (b) current 'catalog.json' (inc metadata with regex/path for files)
	 (c) coded 'manifest2indices.py'
	 (d) optionally 'filter_manifest.sh'
	 (e) optionally 'compare_catalogs.py' to tweak metadata
	 (f) index-making script (LOOK THIS UP, or was it an API call?)
	 (g) xml2json.py (for CDAWeb, makes catalog json from all.xml)

## Operations

Broadly: update metadata as needed. Filter MANIFEST into SPDF/SDAC/Other.
  Run 'manifest2indices.py' on each sub-manifest. Test, migrate, test,
  finalize, test.

1) Optionally, run 'compare_catalogs.py' on CDAWeb Collection to handle
     new or expired datasets:
       convert all.xml -> cdaweb-all.xml
       compare catalog.json to cdaweb-all.xml and flag:
         items in Collections==CDAWeb in catalog but not cdaweb: DELIST?
	 items in Collections==CDAWeb in cdaweb but not catalog: ADD Metadata?

2) Optionally but recommended, filter MANIFEST.csv into separates:
       MANIFEST-cdaweb.csv, MANIFEST-sdac.csv, and MANIFEST-other.csv
   (also can filter out on filetypes here if desired, discuss with Omar)

3) manifest2indices --a cdaweb, --a spdf, --a contrib
   First step uses 'catalog.json', but after each step,
   use the new catalog-updated.json as input for the next step.
   This way we don't lose changes!

4) Also run 'index-making-script' (name TBD?)

5) Run tests in place (catalog.json and indices/)

6) Migrate to staging then destination

7) Test in destination using catalog-updated.json

8) Tests pass? Finalize with 'mv catalog-updated.json catalog.json' in dest
   (with versioning)

9) Rerun tests yet again


## Under the Hood

manifest2indices ingests metadata from catalog.json, which must be
  pre-populated with the regex/paths needed to index
  Make sure this error reprots 'item in manifest but no metadata in json'!

Issue: what if e.g. CDAWeb ADDS or DELETES an entire mission, how do we update
  our metadata?  Sol'n:
    + generate cdaweb-meta.json from 'all.xml'
    + compare it to existing catalog.json (by Collection==CDAWeb),
    + pop up discrepancies via GUI
  Note some 'add' aren't valid .cdf/.nc but e.g. orbit .csv etc, that's a
    thing to track.
  Also does not handle edge case of 'items are not in Manifest but CDAWeb
    thinks it's a thing', especially with non-.cdf/.nc, so some ghost
    entries may end up existing.
  Later make a pruner of 'if catalog.json metadata exists but no actual
    files in MANIFEST, flag to de-list' [kind of an edge case tool]
    