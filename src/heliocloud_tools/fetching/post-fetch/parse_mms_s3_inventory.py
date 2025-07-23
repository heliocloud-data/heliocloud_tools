"""
MMS parser
Rips through an S3 Inventory CSV file to put it into the 'IsoTime, key' format required by the CloudMe spec
Currently assumes the existing files are in time order.
(CloudMe spec does not enforce time order, but suggests it. Later APIs might require it.)

Note we use CloudMe 0.1.3 which allows for a global 'bucket/' that is assumed prefixed to the S3key by API tools
Therefore this file manifest is portable even when the top-level bucket changes, e.g. from
s3://helio-public/ to s3://gov-nasa-smce-data1/

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

And the fileRegistry dataset is INSTR_MODE + optional subcategories with '/' replaced by '_' plus _YYYY.csv

parts = re.split(r'/l2/',s3key)
part1 = parts[0][4:]
part1 = re.sub(r'/','_',part1)
extras = re.split(r'/\d{4}',parts[1])
part2 = extras[0]
if part2 != '': part2 = '_'+part2
getyear = re.search(r'/\d{4}/',s3key)
year = getyear[0][1:5]
indexname = part1 + part2 + '_' + year + '.csv'

Under this, there are 16 MMS datasets per spacecraft, resulting in 64 FileRegistries.

"""

import pandas
import re
import sys
import os.path

# fname = '2cf0f8e0-47ff-4bb7-b897-818c1378f532.csv'
try:
    fname = sys.argv[1]
    print("Processing ", fname)
except:
    print("Usage is: python parse_mms_s3_inventory.py FILENAME.CSV")
    exit()

baseuri = "s3://helio-public/"  # must end in /

csvData = pandas.read_csv(
    fname, header=None, names=["bucket", "s3key", "fsize", "moddate", "hash", "null"]
)

fout_name = "skipme"  # just initializing to a will-not-match value


def registryName(s3key):
    parts = re.split(r"/l2/", s3key)
    part1 = parts[0][4:]
    part1 = re.sub(r"/", "_", part1)
    extras = re.split(r"/\d{4}", parts[1])
    part2 = extras[0]
    if part2 != "" and len(extras) > 1:
        part2 = "_" + part2
    else:
        part2 = ""
    getyear = re.search(r"/\d{4}/", s3key)
    year = getyear[0][1:5]
    indexname = part1 + part2 + "_" + year + ".csv"
    # print("index is ",indexname," from ",s3key,"(",part1,")",part2)
    return indexname


for row in csvData.itertuples():
    mykey = row[2]
    mysize = row[3]

    if mykey.endswith(".cdf"):
        # looking for pattern *YYYYMMDDhhmmss*.cdf
        # or *_YYYYMMDD_
        mytime1 = re.search(r"\d{14}", mykey)
        mytime2 = re.search(r"_\d{8}_", mykey)
        if mytime1:
            got = mytime1[0]
        elif mytime2:
            got = mytime2[0][1:9] + "0000000"  # not all files have hhmmsec
        else:
            print("Skipping, trouble parsing ", mykey)
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
        fmaybe = registryName(mykey)
        if fmaybe != fout_name:
            # need to open a different file handle
            try:
                fout.close()
            except:
                pass
            fout_name = fmaybe
            if os.path.exists(fout_name):
                fout = open(fout_name, mode="a")
            else:
                fout = open(fout_name, mode="w")
                fout.write("#time, s3key, filesize\n")
        fullstr = reparse + "," + baseuri + mykey + "," + str(mysize)
        fout.write(fullstr + "\n")

fout.close()
