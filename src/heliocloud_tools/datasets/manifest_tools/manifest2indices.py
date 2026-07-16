"""
***** REQUIRES MANIFEST.csv is in sorted order by first id, then timestamp
***** REQUIRE catalog[someone].json with metadata for all items to index

Streams a sorted MANIFEST.csv into its indices, also creates a versioned
    'updates.csv' to update the catalog.json with.
    (usually the case when just alphabetically sorting it)

Also makes list of valid ids indexed and list of all non-index errors.

+ Currently filters for cdf/nc/fits/fts files. Change initial regex to alter
+ Has config for cdaweb or psp. Will improve UI and genericize later.
+ requires a catalog-*.json that includes Regex/Patterns for date-matching
    and dataid extraction.

Format of manifest is typically e.g.
'
sdac/hinode/SP3D/20190619_032035/SP3D20190619_034043.5C.fits,938880
sdac/psp_wispr/fits/L1/psp_L1_wispr_20181101T013048_V1_1221.fits,3954240
'
with sample catalog-psp.json having fields:
            "id": "PARKERSOLARPROBE_WISPR_FITS_LEVEL1_PT30M",
            "index": "s3://gov-nasa-hdrl-data1/sdac/psp_wispr/indices/",
            "title": "Parker Solar Probe, WISPR Level 1 FITS images",
            "start": "2018-11-01T00:00:00.000Z",
            "stop": "2023-01-21T23:00:00.000Z",
            "regex": "psp_L1_wispr_%Y%m%dT%H%M%S_%Q.fits",
            "subpath": "sdac/psp_wispr/fits/L1",


    tbd: updating existing indices, updating catalog.json with partials

Uses lookahead-- if filenames do not include an explicit end time,
  it sends the end time interval to be the start of the next data file
  (as requested by MMS users)

Added extra handling for when multiple dataid exist in a shared directory
  (e.g. if endurance_l2_slp & endurance_l3_pes both live in endurance/
   rather than their own subdirs). Affected 35 dataids.

"""

import argparse
import bisect
import csv
from datetime import datetime
import json
import os
import re
import sys

FILENAME_EXT_RE = re.compile(r"\.(cdf|nc|fits|fts)$", re.IGNORECASE)

## Option-setting, add/mod as new datasets require it
def setoptions(archive=None,catalog=None,manifest=None,s3prefix=None,chomp=0):
    globs = {}
    globs['indexhome'] = "indices"
    globs['chomp'] = chomp # default is to remove nothing from MANIFEST lines
    globs['catalog'] = catalog
    globs['manifest'] = manifest
    globs['s3prefix'] = s3prefix
    # some know preset possibilities
    if archive == 'cdaweb':
        print(f"Processing {archive}")
        if globs['catalog'] is None:
            globs['catalog'] = "catalog-cdaweb.json"
        if globs['manifest'] is None:
            globs['manifest'] = "jun-11_manifest_spdf.csv.filtered"
        if globs['s3prefix'] is None:
            globs['s3prefix'] = "s3://gov-nasa-hdrl-data1/spdf/cdaweb/"
    elif archive == 'psp':
        print(f"Processing {archive}")
        if globs['catalog'] is None:
            globs['catalog'] = "catalog-psp.json"
        if globs['manifest'] is None:
            globs['manifest'] = "jun-11_manifest_psp_wispr_encounterless.csv"
        if globs['s3prefix'] is None:
            globs['s3prefix'] = "s3://gov-nasa-hdrl-data1/sdac/"
    if any(item is None for item in globs.values()):
        print("Need either a archive name, or a catalog/manifest/s3prefix, exiting")
        exit()
    globs['errorfile'] = re.sub('.json','',globs['catalog'])+'_errors.txt'
    globs['validfile'] = re.sub('.json','',globs['catalog'])+'_valids.txt'

    return globs
    
## Line parsing routines

def find_prefix(line: str, prefixes: list[str]) -> str | None:
    """
    Return a prefix from `prefixes` that `line` starts with, or None if none match.
    `prefixes` must be sorted.
    """
    # Find insertion position for `line`
    i = bisect.bisect_right(prefixes, line)

    # Candidate 1: prefix just before insertion point
    if i > 0:
        cand = prefixes[i - 1]
        if line.startswith(cand):
            return cand

    # Optional candidate 2: the one at the insertion point (for very short prefixes)
    if i < len(prefixes):
        cand = prefixes[i]
        if line.startswith(cand):
            return cand

    return None

def year_from_filename(filename, regex):
    # Find where the %Y starts in the pattern
    try:
        y_index = int(regex.index("%Y"))
    except:
        return 'static'
    # Use the same position in the filename to slice out the year (4 chars)
    year_str = os.path.basename(filename)[y_index : y_index + 4]
    return year_str

def template_to_regex_multi(template: str) -> re.Pattern:
    """
    Convert a filename template into a regex that captures one or more
    date/time segments.

    Supported tokens:
      %Y, %m, %d, %j, %H, %M, %S : time fields (numbered Y1, m1, d1, Y2, ...)
      %Q                         : version/sequence (matched but not used for time)

    %Q is turned into a non-greedy match up to the next literal text in the template.
    """

    base_name = {
        "%Y": "Y",
        "%m": "m",
        "%d": "d",
        "%j": "j",
        "%H": "H",
        "%M": "M",
        "%S": "S",
        "%Q": None,  # special
    }

    regex_parts = []
    i = 0
    seg_idx = 1
    in_segment = False

    while i < len(template):
        if template[i] == "%" and i + 1 < len(template):
            token = template[i:i+2]
            if token in base_name:
                if token == "%Q":
                    # Build a lookahead based on the literal suffix after %Q
                    j = i + 2
                    literal_suffix = []
                    while j < len(template):
                        if template[j] == "%" and j + 1 < len(template) and template[j:j+2] in base_name:
                            break
                        literal_suffix.append(template[j])
                        j += 1
                    lit = "".join(literal_suffix)
                    if lit:
                        # match anything (non-greedy) up to the literal suffix
                        regex_parts.append(r".+?(?=" + re.escape(lit) + ")")
                    else:
                        # %Q at end: match the rest
                        regex_parts.append(r".+")
                    i += 2
                    continue

                # Date/time tokens: create named groups with segment index
                in_segment = True
                name = base_name[token] + str(seg_idx)
                if token == "%Y":
                    regex_parts.append(rf"(?P<{name}>\d{{4}})")
                elif token == "%j":
                    regex_parts.append(rf"(?P<{name}>\d{{3}})")
                else:
                    regex_parts.append(rf"(?P<{name}>\d{{2}})")
                i += 2
                continue

        # Literal character
        regex_parts.append(re.escape(template[i]))
        if in_segment:
            seg_idx += 1
            in_segment = False
        i += 1

    pattern = "^" + "".join(regex_parts) + "$"
    return re.compile(pattern)


def isos_from_template_and_filename(pattern, filename):
    """
    Using a template and filename with possibly multiple date/time segments,
    return a list of ISO times 'YYYY-MM-DDTHH:MM:SS.000Z', one per segment.

    Tokens per segment:
      %Y, %m, %d, %j, %H, %M, %S
    %Q is matched but NOT used for time.
    Missing time components default to 00.
    """

    m = pattern.match(filename)
    if not m:
        raise ValueError(f"Filename {filename!r} does not match template {pattern!r}\n"
                         f"Regex: {pattern.pattern}")

    g = m.groupdict()
    # print("GROUPS:", g)

    # Collect segment indices (Y1, m1, d1, j1, H1...)
    segment_indices = set()
    for name in g:
        if name and name[-1].isdigit():
            segment_indices.add(int(name[-1]))

    if not segment_indices:
        raise ValueError(f"No date/time segments found in {filename!r} with template {pattern!r}")

    iso_times = []

    for idx in sorted(segment_indices):
        keyY = f"Y{idx}"
        keym = f"m{idx}"
        keyd = f"d{idx}"
        keyj = f"j{idx}"
        keyH = f"H{idx}"
        keyM = f"M{idx}"
        keyS = f"S{idx}"

        if g.get(keyY) is None:
            continue

        year = int(g[keyY])

        # Date: year + DOY (%j) or year + month + day (%m,%d)
        if g.get(keyj) is not None:
            doy = int(g[keyj])
            base = datetime.strptime(f"{year:04d}{doy:03d}", "%Y%j")
            month = base.month
            day = base.day
        else:
            if g.get(keym) is None or g.get(keyd) is None:
                raise ValueError(
                    f"Segment {idx} has %Y but not %m/%d or %j in template {template!r}"
                )
            month = int(g[keym])
            day = int(g[keyd])

        hour = int(g[keyH]) if g.get(keyH) is not None else 0
        minute = int(g[keyM]) if g.get(keyM) is not None else 0
        second = int(g[keyS]) if g.get(keyS) is not None else 0

        dt = datetime(year, month, day, hour, minute, second)
        #iso_times.append(dt.strftime("%Y-%m-%dT%H:%M:%S") + ".00Z")
        iso_times.append(dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z")

    return iso_times

def dumpline(buffer,fout,prefix=''):
    fout.write(f"{buffer['start']},{buffer['end']},{prefix}{buffer['s3key']},{buffer['fsize']}\n")

## Main routines
    
def dumpstats(validlist,validfile,errorlist, errorfile):
    print(f"{len(validlist)} good IDs indexed, see {validfile} for details")
    with open(validfile,"w") as fout:
        fout.writelines(validlist)
            
    print(f"{len(errorlist.keys())} bad path/datasets, see {errorfile} for details")
    with open(errorfile,"w") as eout:
        for mykey in sorted(errorlist.keys()):
            eout.write(f"{mykey},{errorlist[mykey]}\n")

def loadcatalog(catalog):
    with open(catalog, "r") as f:
        data = json.load(f)
        info = {}
        for item in data.get("catalog", []):
            ele = {"id": item["id"],
                   "regex": item["regex"],
                   "pattern": template_to_regex_multi(item["regex"])
                   }
            info.setdefault(item["subpath"], []).append(ele)
    return info

##### MAIN #####

def m2i_main(globs):
    info = loadcatalog(globs['catalog'])
    allstarts = sorted(info.keys())
    currentid, currentyear, currentindex = None, None, None
    errorlist = {}
    validlist = []
    buffer = None

    with open(globs['manifest'], mode="r") as f:
        for line in f:
            line=line[globs['chomp']:-1]
            try:
                line,fsize=line.split(',')
            except:
                fsize=0 # for directory stubs and their ilk
            if FILENAME_EXT_RE.search(line):
                prefix = find_prefix(line, allstarts)
                if prefix is None:
                    errorlist[os.path.dirname(line)] = line
                    continue
                if len(info[prefix]) > 1:
                    # edge case where multiple 'id' live in one 'prefix' dir
                    myinfo = None
                    for thisinfo in info[prefix]:
                        if thisinfo["pattern"].match(os.path.basename(line)):
                            myinfo = thisinfo
                    if myinfo is None:
                        errorlist[os.path.dirname(line)] = line
                        continue
                else:
                    myinfo = info[prefix][0]
                
                if myinfo["id"] != currentid:
                    # new index time
                    try:
                        dumpline(buffer,fout,prefix=globs['s3prefix'])
                        buffer=None
                        fout.close()
                    except:
                        pass
                    currentid = myinfo["id"]
                    regex = myinfo["regex"]
                    pattern = myinfo["pattern"]
                    currentyear = None
                    validlist.append(f"{currentid}\n")
                year = year_from_filename(line, regex)
                try:
                    isos = isos_from_template_and_filename(pattern,
                            os.path.basename(line))
                except:
                    errorlist[os.path.dirname(line)] = line
                    continue
                start=isos[0]
                try:
                    end=isos[1]
                    lookahead=False
                except:
                    end=start
                    lookahead=True
            
                if year != currentyear:
                    if buffer is not None:
                        if buffer['lookahead']:
                            buffer['end']=start
                        dumpline(buffer,fout,prefix=globs['s3prefix'])
                        buffer=None
                    try:
                        fout.close()
                    except:
                        pass
                    currentyear = year
                    foutname = f"{globs['indexhome']}/{currentid}_{currentyear}.csv"
                    fout = open(foutname,"w")
                    fout.write("#start,stop,s3key,filesize\n")

                if buffer is not None:
                    if buffer['lookahead']:
                        buffer['end']=start
                    dumpline(buffer,fout,prefix=globs['s3prefix'])
                buffer={'start':start,'end':end,'s3key':line,
                        'fsize':fsize,'lookahead':lookahead}
    try:
        fout.close()
    except:
        pass

    dumpstats(validlist, globs['validfile'], errorlist, globs['errorfile'])

if __name__ == "__main__":
    """ archive 'psp' or 'cdaweb' sets defaults for the below, or do manually:
        catalog in input metadata JSON in cloudcatalog format
        manifest is the MANIFEST in form 's3key,filesize'
        s3prefix is where the prefix to add to the fname as its cloud loc
        chomp is optional, default 0, # of initial chars to strip from
           manifest keys (e.g. if MANIFEST has
                spdf/cdaweb/data/ace/orbit
           but regex subpaths are looking for start with
                data/ace/orbit
           instead)
    """
    parser = argparse.ArgumentParser(
        prog="manifest2indices",
        description="Convert AWS Manifest into CloudCatalog indices",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument("--archive",
                        dest="archive",
                        default=None,
                        help="name of dataset")
    parser.add_argument("--catalog",
                        dest="catalog",
                        default=None,
                        help="catalog JSON with metadata")
    parser.add_argument("--manifest",
                        dest="manifest",
                        default=None,
                        help="AWS manifest in form s3key,filesize")
    parser.add_argument("--s3prefix",
                        dest="s3prefix",
                        default="s3://gov-nasa-hdrl-data1/",
                        help="destination path for S3 files")
    parser.add_argument("--chomp",
                        dest="chomp",
                        default=0,
                        help="extra chars to trim off manifest lines")
    args = parser.parse_args(sys.argv[1:])
    globs = setoptions(archive=args.archive,
                       catalog=args.catalog,manifest=args.manifest,
                       s3prefix=args.s3prefix,chomp=args.chomp)
    os.makedirs(globs['indexhome'],exist_ok=True)
    m2i_main(globs)
