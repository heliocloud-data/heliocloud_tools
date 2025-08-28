import argparse
import os


def make_script(
    search_string: str,
    chunksize: int = 10,
    sleep_period: int = 10,
    number_of_concurrent_jobs: int = 4,
    keep: bool = False,
):
    # Scan the working directory for CSV files
    csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]

    files_to_process = len(csv_files)
    chunks = (files_to_process + chunksize - 1) // chunksize  # Ensure all files are processed

    keep_flag = "-keep" if keep else ""

    print(f"# Processing {files_to_process} files in {chunks} chunks")
    num = 0
    start = 0

    for i in range(chunks):
        chunk_files = csv_files[start : start + chunksize]
        if not chunk_files:
            break

        chunk_files_str = " ".join(chunk_files)
        print(
            f'./filter_csv.sh {keep_flag} "{search_string}" {chunk_files_str} > logs/filter_csv_{i}.log &'
        )

        start += chunksize
        num += 1

        # Sleep after a batch of concurrent jobs
        if num % number_of_concurrent_jobs == 0:
            print(f"sleep {sleep_period}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Script to create a parallelized shell script for CSV processing"
    )
    ap.add_argument(
        "-s", "--search_string", type=str, required=True, help="String to search for in CSV files"
    )
    ap.add_argument(
        "-c", "--chunksize", type=int, default=10, help="Number of CSV files to process per batch"
    )
    ap.add_argument(
        "-p",
        "--sleep_period",
        type=int,
        default=10,
        help="Sleep time in seconds between job batches",
    )
    ap.add_argument(
        "-j", "--concurrent_jobs", type=int, default=4, help="Number of concurrent jobs to run"
    )
    ap.add_argument(
        "-k",
        "--keep",
        type=bool,
        default=False,
        help="Move files instead of deleting if no match is found",
    )

    args = ap.parse_args()

    make_script(
        args.search_string, args.chunksize, args.sleep_period, args.concurrent_jobs, args.keep
    )
