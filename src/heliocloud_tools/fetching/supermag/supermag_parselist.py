""" Grabbed manifests via:
tree -sf ulfncdf.01s/ > ~/ulfncdf.01s_MANIFEST.txt
tree -sf global/ > ~/mag.01s-global_MANIFEST.txt
tree -sf station/ > ~/mag.01s-station_MANIFEST.txt

Then post-processed with:
grep .cdf.gz ulfncdf.01s_MANIFEST.txt >ulfncdf.01s_cdf.gz_MANIFEST.txt
grep .dmap mag.01s-global_MANIFEST.txt >mag.01s-global_dmap_MANIFEST.txt
grep .dmap mag.01s-station_MANIFEST.txt >mag.01s-station__dmap_MANIFEST.txt

Then run this script with no arguments needed:
python supermag_parselist.py

Then copy indices over with:
aws s3 cp --recursive indices s3://helio-data-staging/jhuapl/supermag/indices

"""

import os
import re

created = "2024-09-18T:12:00:00Z" # date when was copied to helio-data-staging
#bucket = "s3://helio-data-staging/jhuapl/supermag/";
bucket = "s3://gov-nasa-hdrl-data1/contrib/jhuapl/supermag/"
destdir = "indices/" # location of this program's output
os.makedirs(destdir, exist_ok=True)

fnames = ["ulfncdf.01s_cdf.gz_MANIFEST.txt",
          "mag.01s-global_dmap_MANIFEST.txt",
          "mag.01s-station_dmap_MANIFEST.txt"]
ftypes = ["netcdf4", "datamap", "datamap"]
oname_stems = ["supermag_ulfncdf",
               "supermag_mag-01s-global",
               "supermag_mag-01s-station"]
#bucket_stems = ["ulfncdf.01s/","mag.01s/global/","mag.01s/station/"]
bucket_stems = ["","mag.01s/","mag.01s/"]

choice = input("Which set, 1 (ulf), 2 (mag-global) or 3 (mag-station)? [1-3] ")
ichoice = int(choice)-1
fname = fnames[ichoice]
ftype = ftypes[ichoice]
oname_stem = oname_stems[ichoice]
bucket_stem = bucket_stems[ichoice]
print(f"Operating on {oname_stem} series, from {fname}")

icount = 0

testing = False

year_sofar = 1900

with open(fname,"r") as fin:
    for line in fin:
        icount += 1
        m1 = ".*\[(.*)\]"
        fg = re.search(m1,line)
        fsize = fg.group(1).strip()
        
        items = line.split(' ')
        s3key = items[-1].strip()
        pieces = s3key.split('/')
        #m2 = ".*\/(.*(global|ulf|station))"
        m2 = "(\d*)"
        dg = re.search(m2,pieces[-1])
        date = dg.group(1)
        year = date[0:4]
        month = date[4:6]
        day = date[6:8]
        try:
            hour = date[8:]
            if len(hour) < 1: hour = "00"
        except:
            hour = "00"
        date = year + '-' + month + '-' + day + 'T' + hour + ':00:00Z'
        if icount == 1: stub_start = date
        stub_end = date
        if int(year) > int(year_sofar):
            try:
                fout.close()
            except:
                pass
            year_sofar = year
            index_name = destdir + oname_stem +"_"+year+".csv"
            fout = open(index_name,"w")
            print(f"Starting index file {index_name}")

        fout.write(f"{date},{date},{bucket}{bucket_stem}{s3key},{fsize}\n")
                
        if testing:
            print(f"icount: {icount}, year: {year}, date: {date}, key:{s3key}, fsize: {fsize}")
            if icount > 5: break
        
try:
    fout.close()
except:
    pass

print(f"Processed {icount} lines")
stub = {"id": oname_stem, "title": oname_stem, "index": bucket+"indices/",
        "start": stub_start, "stop": stub_end, "modification": created,
        "indextype": "csv", "filetype": ftype}
for key in stub:
    print(f"\t\"{key}\": \"{stub[key]}\",")
