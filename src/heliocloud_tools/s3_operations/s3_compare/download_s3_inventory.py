# script to download all files listed in the manifest, dont forget to use aws-mfa first!
import json
import os
import argparse

ap = argparse.ArgumentParser(description="Client to pull s3 inventory using manifest.json file.")
ap.add_argument("-m", "--manifest", type=str, help="Manifest file to use", default="manifest.json")
ap.add_argument(
    "-d", "--dryrun", default=False, action="store_true", help="Dont download, do a dry run."
)
args = ap.parse_args()

with open(args.manifest, "r") as m:
    manifest_data = json.load(m)
    
    # Check if this is a job completion manifest or inventory manifest
    if "Results" in manifest_data:
        # Job completion manifest format
        src_bucket = manifest_data["Results"][0]["Bucket"]
        files_to_download = [
            {"key": result["Key"]} 
            for result in manifest_data["Results"]
        ]
    elif "destinationBucket" in manifest_data:
        # Inventory manifest format
        src_bucket = manifest_data["destinationBucket"]
        src_bucket = src_bucket.split(":")[-1]
        files_to_download = manifest_data["files"]
    else:
        raise ValueError("Unknown manifest format - missing both 'destinationBucket' and 'Results' keys")
    
    for df in files_to_download:
        filepath = df["key"]
        cmd = f"aws s3 cp --quiet s3://{src_bucket}/{filepath} ."
        if args.dryrun:
            pass
        else:
            os.system(cmd)
        print(os.path.basename(filepath))
