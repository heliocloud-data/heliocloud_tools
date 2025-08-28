"""
Script to delete ALL versions of an object in an S3 bucket.
Takes a list of objects to be deleted.
"""

import argparse
import logging
import sys
import boto3
from botocore.exceptions import ClientError

# Setup logging environment
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def permanently_delete_object(bucket, object_key) -> bool:
    """
    Permanently deletes a versioned object by deleting all of its versions.

    Usage is shown in the usage_demo_single_object function at the end of this module.

    :param bucket: The bucket that contains the object.
    :param object_key: The object to delete.
    """
    try:
        bucket.object_versions.filter(Prefix=object_key).delete()
    except ClientError:
        # Delete failed
        logger.exception("Couldn't delete all versions of %s.", object_key)
        return False

    # Delete succeeded
    return True


def delete_files(bucket_name: str, filelist_name: str, start: int = 0, end: int = -1) -> None:
    """
    Deletes files listed in filelist_name from AWS S3 bucket bucket_name.
    Start and end serve as indices in filelist_name to bound the delete by
    (delete only those files listed between start & end)
    """

    # open up bucket
    s3_res = boto3.resource("s3")
    s3_bucket = s3_res.Bucket(bucket_name)

    with open(filelist_name, encoding="UTF-8") as filelist_file:
        s3_objs = filelist_file.read().splitlines()

    # delete each obj one by one (*sigh*)
    # implement a little counter so we can check progress
    count = 0
    for s3_obj in s3_objs[start:end]:
        if permanently_delete_object(s3_bucket, s3_obj):
            if count % 10000 == 0:
                logger.info("Deleted %s objs", str(count))
            count += 1

    logger.info("Finished. Deleted %s objs", str(count))


if __name__ == "__main__":
    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(
        description="Extract relevant text from Astro2010 whitepaper pdfs."
    )
    ap.add_argument(
        "-b", "--bucket", type=str, required=True, help="Name of bucket to delete objects from "
    )
    ap.add_argument(
        "-o",
        "--objlist_name",
        type=str,
        required=True,
        help="Name of file with list of objects to delete",
    )
    ap.add_argument(
        "-s",
        "--start",
        type=int,
        default=0,
        help="Start object index in list, defaults to first object",
    )
    ap.add_argument(
        "-e",
        "--end",
        type=int,
        default=-1,
        help="Stop/end object index in list, defaults to last object",
    )
    ap.add_argument(
        "-n",
        "--number",
        type=int,
        default=-1,
        help="Number of objects to delete, dont use with --end",
    )

    # parse argv
    args = ap.parse_args()

    # check number
    if args.number > 0:
        args.end = args.start + args.number

    delete_files(args.bucket, args.objlist_name, args.start, args.end)
