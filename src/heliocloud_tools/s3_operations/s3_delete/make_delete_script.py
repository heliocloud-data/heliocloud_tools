# a little script to create another script


def make_delete_script(
    files_to_delete_filename: str,
    bucket: str,
    chunksize: int = 20000,
    sleep_period=1200,
    number_of_concurrent_jobs: int = 8,
):
    with open(files_to_delete_filename, "r") as f:
        lines = f.readlines()

    files_to_delete = len(lines)
    chunks = int(files_to_delete / chunksize)

    print(f"# files to delete {files_to_delete}")
    print(f"# in {chunks} chunks")

    num = 0
    start = 0
    for i in range(0, chunks):
        print(
            f"python delete_s3_objs.py -b {bucket} -o {files_to_delete_filename} -s {start} -n {chunksize} > logs/delete_obj_all_{i}.log &"
        )
        start += chunksize
        num += 1

        # "Thread by sleeping", e.g. group every 3 or so calls by inserting a sleep statement
        if num % number_of_concurrent_jobs == 0:
            print(f"sleep {sleep_period}")


if __name__ == "__main__":
    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(
        description="Script to create a shell script for bulk deletion of objs from S3 bucket"
    )
    ap.add_argument(
        "-b", "--bucket", type=str, required=True, help=f"Name of bucket to delete objects from "
    )
    ap.add_argument(
        "-o",
        "--objlist_filename",
        type=str,
        required=True,
        help=f"Name of csv file with list of objects to delete",
    )
    ap.add_argument(
        "-c", "--chunksize", type=int, default=20000, help=f"List chunksize to step through"
    )
    ap.add_argument(
        "-s",
        "--sleep_period",
        type=int,
        default=1200,
        help=f"Length to sleep between calls to delete script, in sec",
    )
    ap.add_argument(
        "-j", "--concurrent_jobs", type=int, default=10, help=f"Number of concurrent jobs to run."
    )

    # parse argv
    args = ap.parse_args()

    make_delete_script(
        args.objlist_filename, args.bucket, args.chunksize, args.sleep_period, args.concurrent_jobs
    )
