# Py Script to create a shell script for copying between S3 buckets
def make_copy_script(
    file_name: str,
    src: str,
    dst: str,
    sleep=30,
    concurrent_jobs: int = 20,
    checksum: bool = True,
):

    line_count = 0
    # Stream the file instead of reading all at once
    with open(file_name, "r") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            line_count += 1
            if checksum:
                cmd = (
                    f"aws s3 cp s3://{src}/{line} s3://{dst}/{line} "
                    f"--checksum-algorithm CRC64NVME >> ./copy_log.txt &"
                )
            else:
                cmd = (
                    f"aws s3 cp s3://{src}/{line} s3://{dst}/{line} "
                    f">> ./copy_log.txt &"
                )

            print(cmd)
            if int(idx + 1) % concurrent_jobs == 0:
                print(f"sleep {sleep}")

    print(f"# Number of files to copy: {line_count}")


if __name__ == "__main__":
    import argparse

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
        "-src",
        "--source",
        type=str,
        required=True,
        help=f"Name of bucket to copy objects from",
    )
    ap.add_argument(
        "-dst",
        "--destination",
        type=str,
        required=True,
        help=f"Name of bucket to copy objects to",
    )
    ap.add_argument(
        "-s",
        "--sleep",
        type=int,
        default=30,
        help=f"Length to sleep between calls to copy script, in sec",
    )
    ap.add_argument(
        "-n",
        "--concurrent_jobs",
        type=int,
        default=20,
        help=f"Number of concurrent jobs to run.",
    )
    ap.add_argument(
        "-c",
        "--checksum",
        type=bool,
        default=True,
        help=f"Whether or not to generate a checksum",
    )

    args = ap.parse_args()
    make_copy_script(
        args.input_file,
        args.source,
        args.destination,
        args.sleep,
        args.concurrent_jobs,
        args.checksum,
    )
