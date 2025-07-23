# Program to delete ALL versions of an object in an S3 bucket. Takes a list of objs to be deleted.
#
import boto3
from botocore.exceptions import *

import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def permanently_delete_object(s3_bucket, object_key: str, skip_directories: bool = True) -> None:
    """
    Permanently deletes a versioned object by deleting all of its versions.

    Usage is shown in the usage_demo_single_object function at the end of this module.

    :param s3_bucket: The bucket that contains the object.
    :param object_key: The object to delete.
    :param skip_directories: skip deleting directories
    """

    # skip 'directories', e.g. anything ending in a slash
    logger.debug(f" key: {object_key}")
    if skip_directories and object_key.endswith("/"):
        logger.debug(f"  skipping: {object_key}")
        return object_key

    try:
        s3_bucket.object_versions.filter(Prefix=object_key).delete()
        # logger.info("Permanently deleted all versions of object '%s'", object_key)
    except ClientError:
        logger.exception("Couldn't delete all versions of '%s'", object_key)
        raise

    return None


def delete_files(bucket_name: str, filelist_name: str, start: int = 0, end: int = -1) -> None:
    # open up bucket
    s3_res = boto3.resource("s3")
    s3_bucket = s3_res.Bucket(bucket_name)

    logger.info(s3_bucket)

    # stream only the parts (a potentially large file) into memory
    s3_objs = []
    line_nr = 0
    with open(filelist_name) as f:
        for line in f:
            if line_nr >= start and (end == -1 or line_nr < end):
                # strip off newline if exists
                if line[-1] == "\n":
                    line = line[:-1]
                # add to our list for deletion
                s3_objs.append(line)
            line_nr += 1

            if line_nr > end:
                break

    clean_up_objs = do_delete(s3_bucket, s3_objs)

    # get the dirs now
    # do_delete(s3_bucket, clean_up_objs, False)

    logger.info(f"Finished.")


def do_delete(s3_bucket, s3_objs: list, skip_dirs: bool = True) -> list:
    dir_objs = []

    logger.info(f"Got %s objects to delete" % len(s3_objs))
    if len(s3_objs) > 0:
        logger.info(f"first object to delete: %s" % s3_objs[0])

    # delete each obj one by one (*sigh*)
    # implement a little counter so we can check progress
    i = 0
    for s3_obj in s3_objs:
        try:
            dir_result = permanently_delete_object(s3_bucket, s3_obj, skip_dirs)
            if dir_result:
                dir_objs.append(dir_result)
            if i % 10000 == 0 and i != 0:
                logger.info(f"Deleted {i} objs")
            i = i + 1

        except Exception as ex:
            logger.error(ex)
            pass

    logger.info(f"Deleted {i} objs")

    return dir_objs


if __name__ == "__main__":
    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(
        description="Extract relevant text from Astro2010 whitepaper pdfs."
    )
    ap.add_argument(
        "-b", "--bucket", type=str, required=True, help=f"Name of bucket to delete objects from "
    )
    ap.add_argument(
        "-o",
        "--objlist_name",
        type=str,
        required=True,
        help=f"Name of file with list of objects to delete",
    )
    ap.add_argument(
        "-s",
        "--start",
        type=int,
        default=0,
        help=f"Start object index in list, defaults to first object",
    )
    ap.add_argument(
        "-e",
        "--end",
        type=int,
        default=-1,
        help=f"Stop/end object index in list, defaults to last object",
    )
    ap.add_argument(
        "-n",
        "--number",
        type=int,
        default=-1,
        help=f"Number of objects to delete, dont use with --end",
    )

    # parse argv
    args = ap.parse_args()

    # check number
    if args.number > 0:
        args.end = args.start + args.number

    delete_files(args.bucket, args.objlist_name, args.start, args.end)
