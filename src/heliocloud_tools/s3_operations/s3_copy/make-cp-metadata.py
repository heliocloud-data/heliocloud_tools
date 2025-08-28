# Py Script to create a shell script for copying between S3 buckets


def make_copy_script(
    file_name: str,
    src: str,
    dst: str,
    sleep=30,
    concurrent_jobs: int = 20,
    dryrun: bool = False,
    metadata: bool = True,
):
    with open(file_name, "r") as f:
        lines = f.readlines()

    print(f"# Number of files to copy: {len(lines)}")
    # Combine the parameters and create the cp command
    copy_list = []
    if dryrun and metadata:
        for line in lines:
            copy_list.append(
                f"aws s3 cp s3://{src}/{line[:-1]} s3://{dst}/{line[:-1]} --dryrun --metadata-directive COPY >> ./copy_log.txt &"
            )
    elif metadata and not dryrun:
        for line in lines:
            copy_list.append(
                f"aws s3 cp s3://{src}/{line[:-1]} s3://{dst}/{line[:-1]} --metadata-directive COPY >> ./copy_log.txt &"
            )
    elif dryrun and not metadata:
        for line in lines:
            copy_list.append(
                f"aws s3 cp s3://{src}/{line[:-1]} s3://{dst}/{line[:-1]} --dryrun >> ./copy_log.txt &"
            )
    elif not dryrun and not metadata:
        for line in lines:
            copy_list.append(
                f"aws s3 cp s3://{src}/{line[:-1]} s3://{dst}/{line[:-1]} >> ./copy_log.txt &"
            )

    # Divide the commands into chunks
    for idx, command in enumerate(copy_list):
        # "Thread by sleeping", e.g. group every J calls by inserting a sleep statement
        print(command)
        if int(idx + 1) % concurrent_jobs == 0:
            print(f"sleep {sleep}")


if __name__ == "__main__":
    import argparse

    # Use nargs to specify how many arguments an option should take.
    ap = argparse.ArgumentParser(
        description="Py Script to create a shell script for copying between S3 buckets"
    )
    ap.add_argument(
        "-i",
        "--input_file",
        type=str,
        required=True,
        help=f"Name of csv file with list of objects to copy",
    )
    ap.add_argument(
        "-src", "--source", type=str, required=True, help=f"Name of bucket to copy objects from"
    )
    ap.add_argument(
        "-dst", "--destination", type=str, required=True, help=f"Name of bucket to copy objects to"
    )

    ap.add_argument(
        "-s",
        "--sleep",
        type=int,
        default=30,
        help=f"Length to sleep between calls to copy script, in sec",
    )
    ap.add_argument(
        "-j", "--concurrent_jobs", type=int, default=20, help=f"Number of concurrent jobs to run."
    )
    ap.add_argument(
        "-dry", "--dryrun", type=bool, default=False, help=f"Whether or not to use the dryrun flag"
    )
    ap.add_argument(
        "-m",
        "--metadata",
        type=bool,
        default=True,
        help=f"Whether or not to copy the metadata over",
    )

    # parse argv
    args = ap.parse_args()
    make_copy_script(
        args.input_file,
        args.source,
        args.destination,
        args.sleep,
        args.concurrent_jobs,
        args.dryrun,
        args.metadata,
    )
