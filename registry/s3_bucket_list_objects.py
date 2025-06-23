"""
Script for producing a file containing the names of objects and their locations in a
particular AWS S3 bucket.
"""
import argparse
import boto3


def list_objects(bucket_name: str, list_all_objects: bool = False) -> list:
    """
    Lists the objects found in the AWS S3 bucket indicated by bucket_name.
    If list_all_objects is True, return the list populated with every VERSION of an object
    (note: One object could have multiple versions)
    Otherwise, just the names of the current version of each object are returned.
    """

    # declare and open up bucket
    s3_res = boto3.resource("s3")
    s3_bucket = s3_res.Bucket(bucket_name)

    objects = []
    if list_all_objects:
        for obj in s3_bucket.object_versions.all():
            objects.append(obj.key)
    else:
        for obj in s3_bucket.objects.all():
            objects.append(obj.key)

    return objects


if __name__ == "__main__":  # use if csv of text
    ap = argparse.ArgumentParser(description="Client to pull list of objects in an s3 bucket.")
    ap.add_argument(
        "-a",
        "--all_versions",
        default=False,
        action="store_true",
        help="Pull data for all objects.",
    )
    ap.add_argument(
        "-b", "--bucket_name", type=str, help="Name of S3 bucket to get objects for.", required=True
    )
    ap.add_argument(
        "-o",
        "--output_filename",
        type=str,
        default="s3_objects.csv",
        help="Name of output filename to write reported objects to.",
        required=False,
    )

    args = ap.parse_args()

    s3_objs = list_objects(args.bucket_name, args.all_versions)

    # write / cache files to local listing (speed purposes)
    with open(args.output_filename, "w", encoding="UTF-8") as object_file:
        for s3_obj in s3_objs:
            object_file.write(f"{s3_obj}\n")

    print(f"Finished, wrote report to {args.output_filename}")
