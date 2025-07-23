# S3 Delete scripts

## About

This code will got through a list of objects and delete them. 

## Usage
Find name of the S3 bucket to delete from, and use an output list of files to feed it.

You may then invoke the delete\_files script.

For example:

```bash
> sh delete_files.sh helio-data-staging s3_datafile_list.csv

```

## Bucket wide operations

For bucket wide delete and list operations, please reference `./bucket-operations`
