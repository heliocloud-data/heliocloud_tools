# S3 Bucket Comparison

## About

These scripts support comparison of a 'source' and a 'destination' S3 bucket. This code will report if there are any missing files in the destination bucket.

## Setup

You will need to create an s3 inventory report for both the destination and source buckets which contain 
the following additional reported fields: 

'size', 'last modified'

Let the inventory generate a manifest report (~1 day) for each bucket then download and rename them to distinguish source from destination manifest files.

At this point, you may use the script as indicated below.

 
## Use

The main script is 'find_missing_in_destination.sh' which will compare a source to a destination bucket, for example

```bash
> sh start_comparison.sh data_staging_manifest.json tops_manifest.json
```

This script will output a report about which files are present in the source bucket but missing in the destination.

