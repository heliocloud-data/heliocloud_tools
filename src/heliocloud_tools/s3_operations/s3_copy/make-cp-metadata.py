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
    import subprocess
    
    # Get total line count using wc -l
    result = subprocess.run(['wc', '-l', file_name], capture_output=True, text=True)
    total_lines = int(result.stdout.split()[0])
    
    line_count = 0
    print("START_TIME=$(date +%s)")
    print("")
    
    # Stream the file line by line instead of reading all into memory
    with open(file_name, "r") as f:
        batch_first_object = None
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            line_count += 1
            
            if (idx % concurrent_jobs) == 0:
                batch_first_object = line
            
            if dryrun and metadata:
                cmd = (
                    f"aws s3 cp s3://{src}/{line} s3://{dst}/{line} "
                    f"--dryrun --metadata-directive COPY >> ./copy_log.txt &"
                )
            elif metadata and not dryrun:
                cmd = (
                    f"aws s3 cp s3://{src}/{line} s3://{dst}/{line} "
                    f"--metadata-directive COPY >> ./copy_log.txt &"
                )
            elif dryrun and not metadata:
                cmd = (
                    f"aws s3 cp s3://{src}/{line} s3://{dst}/{line} "
                    f"--dryrun >> ./copy_log.txt &"
                )
            elif not dryrun and not metadata:
                cmd = (
                    f"aws s3 cp s3://{src}/{line} s3://{dst}/{line} "
                    f">> ./copy_log.txt &"
                )
            print(cmd)
            if int(idx + 1) % concurrent_jobs == 0:
                print(f"sleep {sleep}")
                print(f'CURRENT_TIME=$(date +%s)')
                print(f'ELAPSED=$((CURRENT_TIME - START_TIME))')
                print(f'DAYS=$((ELAPSED / 86400))')
                print(f'HOURS=$(((ELAPSED % 86400) / 3600))')
                print(f'MINUTES=$(((ELAPSED % 3600) / 60))')
                print(f'SECONDS=$((ELAPSED % 60))')
                print(f'echo "Time elapsed: ${{DAYS}}d ${{HOURS}}h ${{MINUTES}}m ${{SECONDS}}s"')
                print(f'echo "Progress: ({idx + 1}/{total_lines})"')
                print(f'echo "First object of batch: {batch_first_object}"')
                print(f'echo "Last object of batch: {line}"')
                print(f'echo "=================="')
                print("")
    print(f"# Number of files to copy: {line_count}")
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
