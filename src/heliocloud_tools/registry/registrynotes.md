Tools for use with your HelioCloud instance.

# Tools for HelioCloud

## About

These are scripts for maintaining and working with a HelioCloud instance deployed into AWS. They are
of varying utility.

## Requirements



## Setup
Your environment must be setup to run BASH shell scripts and the AWS cli package. It will
also be necessary to have administrative privileges on the AWS account, and for you to setup
an AWS key & passphrase in ~/.aws/credentials. It maybe also be useful to put the name the AWS
region in which your HelioCloud is deployed into this file as well.

The `get_credentials.sh` script can help you with setting up your local environment, returning
back the various environment variables you need to set.


---
#### User Management
Scripts for helping to administer HelioCloud user accounts.

`list_users.sh` - script for listing the HelioCloud users configured in the AWS Cognito pool(s)

---
#### Registry
These scripts support working with the HelioCloud registry and datasets stored therein.

`s3_bucket_bulk_delete.py` - Helps with selectively bulk deleting the contents of an S3 bucket

`s3_bucket_list_objects.py` - Used by the bulk delete script to list some/all verions of objects in the bucket

`s3_bucket_object_delete.py` - For deleting individual objects

`ingest.py` - Use to ingest a dataset stored in an S3 bucket into a HelioCloud's Registry module

`catalog.py` - Use to invoke the Cataloger for this HelioCloud. This will force an update of the 
Registry.

---
##### S3 Bucket Comparison
These scripts support comparison of a 'source' and a 'destination' S3 bucket as may be necessary
when confirming data has been ingested into the HelioCloud registry correctly. This code will report 
if there are any missing files in the destination bucket.

_Setup_

First, you need to create an AWS S3 inventory report for both the destination and source 
buckets. The report must contain the following _additional_ reported fields:
`size` and `last modified`. 

Let the inventory generate a manifest report (~1 day) for each bucket then download and rename them 
to distinguish source from destination manifest files. At this point, you may use the script as
below:

```bash
> sh find_missing_in_destination.sh data_staging_manifest.json tops_manifest.json
```

This script will output a report about which files are present in the source bucket but missing in the destination.