# script to download all files listed in the manifest, dont forget to use aws-mfa first!

import json
import os
import argparse

ap = argparse.ArgumentParser(description="Client to pull s3 inventory using manifest.json file.")
# ap.add_argument('-sb', '--src_bucket', type=str, help='Manifest file to use', default=DEF_SRC_BUCKET)
ap.add_argument("-m", "--manifest", type=str, help="Manifest file to use", default="manifest.json")
ap.add_argument(
    "-d", "--dryrun", default=False, action="store_true", help="Dont download, do a dry run."
)

args = ap.parse_args()

with open(args.manifest, "r") as m:
    manifest_data = json.load(m)

    src_bucket = manifest_data["destinationBucket"]
    src_bucket = src_bucket.split(":")[-1]

    for df in manifest_data["files"]:
        filepath = df["key"]
        cmd = f"aws s3 cp --quiet s3://{src_bucket}/%s ." % df["key"]
        if args.dryrun:
            pass
        else:
            os.system(cmd)
        print(os.path.basename(filepath))
