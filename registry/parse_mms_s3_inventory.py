r"""
MMS parser
Rips through an S3 Inventory CSV file to put it into the 'IsoTime, key' format required by the
CloudMe spec. Currently assumes the existing files are in time order.
(CloudMe spec does not enforce time order, but suggests it. Later APIs might require it.)

Note we use CloudMe 0.1.3 which allows for a global 'bucket/' that is assumed prefixed to the S3key
by API tools. Therefore this file manifest is portable even when the top-level bucket changes,
e.g. from s3://helio-public/ to s3://gov-nasa-smce-data1/

Initial CSV is #time, s3key, filesize

sample s3key:
    MMS/mms1/feeps/srvy/l2/ion/2021/11/mms1_feeps_srvy_l2_ion_20211128000000_v7.1.1.cdf

tree is: MMS/mms[1-4]/
   feeps/
       brst/l2/
           electron
           ion/
       srvy/l2/
           electron
           ion/
   fgm/
        brst/l2/
        srvy/l2/
   fpi/
        brst/l2/
            des-dist/
            des-moms/
            dis-dist/
            dis-moms/
        fast/l2/
            des-dist/
            des-moms/
            dis-dist/
            dis-moms/
   mec
        brst/l2/epht89d/
        srvy/l2/epht89d/

So s3keys are:  'MMS'/INSTR_MODE/'l2'/0 or 1 subcategories/YYYY/MM/filename

And the fileRegistry dataset is INSTR_MODE + optional subcategories with '/' replaced by '_'
plus _YYYY.csv

parts = re.split(r'/l2/',s3key)
part1 = parts[0][4:]
part1 = re.sub(r'/','_',part1)
extras = re.split(r'/\d{4}',parts[1])
part2 = extras[0]
if part2 != '': part2 = '_'+part2
get_year = re.search(r'/\d{4}/',s3key)
year = get_year[0][1:5]
index_name = part1 + part2 + '_' + year + '.csv'

Under this, there are 16 MMS datasets per spacecraft, resulting in 64 FileRegistries.

"""
import argparse
import re
import os.path
import pandas


def registry_name(s3key):
    """
    Name of the registry file to write to.
    """
    print(f"Key is {s3key}")
    parts = re.split(r"/l2/", s3key)
    part1 = parts[0][4:]
    part1 = re.sub(r"/", "_", part1)
    extras = re.split(r"/\d{4}", parts[1])
    part2 = extras[0]
    if part2 != "" and len(extras) > 1:
        part2 = "_" + part2
    else:
        part2 = ""
    year_str = re.search(r"/\d{4}/", s3key)
    year = year_str[0][1:5]
    index_name = part1 + part2 + "_" + year + ".csv"
    print(f"Writing to {index_name}")
    return index_name


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="MMS data parser script. Rips through an S3 Inventory CSV file to put it into "
        "the 'IsoTime, key' format required by the CloudMe spec. Currently assumes the "
        "existing files are in time order. (CloudMe spec does not enforce time order, "
        "but suggests it. Later APIs might require it."
    )
    ap.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Name of the CSV file produced by the" "S3 Inventory service",
    )

    # Catch the file into a Pandas dataframe
    args = ap.parse_args()
    FILE_NAME = args.file
    BASE_URI = "s3://helio-public/"  # must end in /
    csv_df = pandas.read_csv(
        FILE_NAME, header=None, names=["bucket", "s3key", "fsize", "moddate", "hash", "null"]
    )

    # Cache for the names & handles to the registry files that will be created
    reg_files = {}

    # Loop through the CSV dataframe and write out entries to appropriate registry files
    for row in csv_df.itertuples():
        my_key = row[2]
        my_size = row[3]

        # Only process CDF files
        if my_key.endswith(".cdf"):
            # looking for pattern *YYYYMMDDhhmmss*.cdf
            # or *_YYYYMMDD_
            my_time_1 = re.search(r"\d{14}", my_key)
            my_time_2 = re.search(r"_\d{8}_", my_key)
            if my_time_1:
                got = my_time_1[0]
            elif my_time_2:
                got = my_time_2[0][1:9] + "0000000"  # not all files have hhmmsec
            else:
                print("Skipping, trouble parsing ", my_key)
                continue
            reparse = (
                got[0:4]
                + "-"
                + got[4:6]
                + "-"
                + got[6:8]
                + "T"
                + got[8:10]
                + ":"
                + got[10:12]
                + ":"
                + got[12:14]
                + "Z"
            )
            reg_file_name = registry_name(my_key)

            # If we don't have a reg file opened yet, open one up
            # pylint: disable=consider-using-with
            if reg_file_name not in reg_files:
                # If it doesn't exist yet add the header
                if not os.path.exists(reg_file_name):
                    handle = open(reg_file_name, mode="w", encoding="UTF-8")
                    handle.write("#time, s3key, filesize\n")
                # Otherwise we are appending to an existing file (perhaps from a prior run)
                else:
                    handle = open(reg_file_name, mode="a", encoding="UTF-8")
                reg_files[reg_file_name] = handle

            # Write to the file
            full_str = reparse + "," + BASE_URI + my_key + "," + str(my_size)
            reg_files[reg_file_name].write(full_str + "\n")
            # pylint: enable=consider-using-with

    # Close the file handles
    for f_handle in reg_files.values():
        f_handle.close()
