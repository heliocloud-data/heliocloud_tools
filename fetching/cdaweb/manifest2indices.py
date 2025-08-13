#***** REQUIRES MANIFEST.csv is in sorted order by first id, then timestamp

# Works but needs better exception handling, run it to see what I mean

""" Streams a sorted MANIFEST.csv into its indices, also creates a versioned
    'updates.csv' to update the catalog.json with.
    (usually the case when just alphabetically sorting it)
   Also, that only .nc/.cdf files exist

   ***** REQUIRES MANIFEST.csv is in sorted order by first id, then timestamp

(Catalog updater does the 'update if exists, otherwise use XML to create)

    tbd: updating existing indices, updating catalog.json with partials
"""

import os
import shutil
import cdaweb_xml_checker as cxc

DEBUG = True

def dumpline(fout,cache):
        fout.write(f"{cache['start']},{cache['stop']},{cache['s3key']},{cache['fsize']}\n")
        #print("\t\t",cache)

def parseline(line):
        line=line.rstrip()
        items=line.split(',')
        fsize = items[-1]
        fname = items[-2]
        return fname, fsize

def version_file(filepath):
    if filepath.startswith("s3://"):
        print("Warning, cannot version files in S3 yet.")
        return

    if not os.path.exists(filepath):
        # print("File does not exist. No need to version.")
        return

    dirname, filename = os.path.split(filepath)
    name, ext = os.path.splitext(filename)

    version = 1
    while True:
        new_filename = f"{name}_v{version}{ext}"
        new_filepath = os.path.join(dirname, new_filename)
        if not os.path.exists(new_filepath):
            break
        version += 1

    shutil.move(filepath, new_filepath)
    # print(f"File versioned as: {new_filepath}")

manifest = 'sortedmanifest.csv'
coutfile = 'updates.csv'
version_file(coutfile)

xml_path = '.'
strip_me = 'pub/data/'
ensure_prefix = 'spdf/cdaweb/data/'
regex_base, regex_pattern = cxc.load_fromxml(xml_path, strip_me=strip_me,
                                             ensure_prefix=ensure_prefix)

fin = open(manifest,"r")
cout = open(coutfile,"w")

# init setup to trigger first rounds
currid = 'junk'
tracker = ['junk','0','.']
ztime = '0000'
cache = {'start':'','s3key':'','fsize':''}
fout = open('junk.ignore','w')

for line in fin:
    fname, fsize = parseline(line)
    dataid, filename = cxc.extract_just_dataid(fname,shortprefix=ensure_prefix)
    if dataid != currid:
        cache['stop']=ztime
        dumpline(fout,cache)
        fout.close()
        tracker.append(ztime)
        cout.write(','.join(tracker)+'\n')
        
        dataid, indexbase, x_regex = cxc.extract_regex(regex_base, regex_pattern, fname)
        indexbase = cxc.best_indexdir(fname, short_prefix=ensure_prefix)
        ztime = cxc.extract_datetime(fname, x_regex, form="str")
        year = ztime[0:4]
        os.makedirs(indexbase,exist_ok=True)
        tracker = [dataid,indexbase,ztime]
        foutname = indexbase + '/' + dataid + '_' + year + '.csv'
        if DEBUG: print(f"Initiating {dataid} {year} index {foutname}")
        fout = open(foutname,"w")
        fout.write('#start,stop,s3key,filesize\n')
        cache = {'start':ztime,'s3key':fname,'fsize':fsize}
        curryear = year
        currid = dataid
    else:
        ztime = cxc.extract_datetime(fname, x_regex, form="str")
        cache['stop']=ztime
        dumpline(fout,cache)
        cache = {'start':ztime,'s3key':fname,'fsize':fsize}
        year = ztime[0:4]
        if year != curryear:
                fout.close()
                foutname = indexbase + '/' + dataid + '_' + year + '.csv'
                fout = open(foutname,"w")
                fout.write('#start,stop,s3key,filesize\n')
                if DEBUG: print(f"\tYearskip {dataid} {year} index {foutname}")
                curryear = year
        
cache['stop']=ztime
dumpline(fout,cache)
tracker.append(ztime)
cout.write(','.join(tracker)+'\n')
fout.close()
cout.close()
