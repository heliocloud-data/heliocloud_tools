"""
Creates a sequence of Python scripts to help with the bulk delete of AWS S3 bucket contents,
accomplished by breaking up the deletes into chunks - a count of individual files to delete
in a single invocation of the delete script.
"""


def make_delete_script(files_to_delete_filename: str, bucket: str, chunksize: int = 50000) -> None:
    """
    Outputs to stdout the sequence of script calls a user can do to delete the contents of the
    S3 bucket in chunks.
    """
    with open(files_to_delete_filename, "r", encoding="UTF-8") as file:
        lines = file.readlines()

    files_to_delete = len(lines)
    chunks = int(files_to_delete / chunksize)

    num = 0
    start = 0
    for i in range(0, chunks):
        print(
            f"python delete_s3_objs.py -b {bucket} -o {files_to_delete_filename} -s {start} -n "
            f"{chunksize} > logs/delete_obj_all_{i}.log &"
        )
        start += chunksize
        num += 1

        # "Thread by sleeping", e.g. group every 3 or so calls by inserting a sleep statement
        if num % 3 == 0:
            print("sleep 1800")


if __name__ == "__main__":
    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(
        description="Script to create a shell script for bulk deletion of objs from S3 bucket"
    )
    ap.add_argument(
        "-b", "--bucket", type=str, required=True, help="Name of bucket to delete objects from "
    )
    ap.add_argument(
        "-o",
        "--objlist_filename",
        type=str,
        required=True,
        help="Name of csv file with list of objects to delete",
    )
    ap.add_argument(
        "-c", "--chunksize", type=int, default=50000, help="List chunksize to step through"
    )

    # parse argv
    args = ap.parse_args()

    make_delete_script(args.objlist_filename, args.bucket, args.chunksize)
