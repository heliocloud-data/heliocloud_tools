import pandas as pd
import argparse
import json
import os

DEF_BATCH_SIZE = 10
DEF_WRITE_HEADER = False

# Default column names
col_names = ["Bucket", "Key", "Size"]

# Check for manifest.json and update col_names if the file exists
manifest_path = "./manifest.json"
if os.path.exists(manifest_path):
    with open(manifest_path, "r") as manifest_file:
        try:
            manifest_data = json.load(manifest_file)
            if "fileSchema" in manifest_data:
                col_names = [col.strip() for col in manifest_data["fileSchema"].split(",")]
        except json.JSONDecodeError:
            print("Error: manifest.json is not a valid JSON file.")

ap = argparse.ArgumentParser(description="Program to combine inventory csv files.")
ap.add_argument(
    "--output_file",
    "-o",
    type=str,
    help="output file to write merged csv files to",
    default="merged.csv",
)
ap.add_argument(
    "--batch_size",
    "-b",
    type=int,
    help="size of batch to use for merging files",
    default=DEF_BATCH_SIZE,
)
ap.add_argument("--write_header", "-w", default=False, action="store_true", help="Write a header")
ap.add_argument("-chksum", action="store_true", help="Enable checksum mode")
ap.add_argument("files", nargs="+", type=str, help="csv files to use")

args = ap.parse_args()

# Ensure col_names length matches CSV column count
with open(args.files[0]) as scanner:
    firstL = scanner.readline()
cols = firstL.strip().split(",")

if len(cols) > len(col_names):
    print("Column names mismatch, appending placeholders to align.")
    diff = len(cols) - len(col_names)
    for i in range(diff):
        col_names.append(chr(ord("a") + i))

batch_size = min(args.batch_size, len(args.files))

batch_cntr = 0
files = args.files
fcntr = 0

while fcntr < len(files):
    cntr = 0
    frames = []
    while cntr < batch_size and fcntr < len(files):
        f = files[fcntr]
        print(f"Reading [{fcntr}] {f}")
        df = pd.read_csv(f, header="infer", names=col_names)
        frames.append(df)

        fcntr += 1
        cntr += 1

    merged = pd.concat(frames) if len(frames) > 1 else frames[0]

    ofilename = f"{batch_cntr}_{args.output_file}"

    # Determine columns for final output
    output_columns = ["Key", "Size"]
    if args.chksum and "ChecksumAlgorithm" in col_names:
        output_columns.append("ChecksumAlgorithm")

    # Write sorted key values to CSV
    merged.to_csv(ofilename, columns=output_columns, index=False, header=args.write_header)

    batch_cntr += 1

    print(f"Wrote merged list to {ofilename}")

    del merged
