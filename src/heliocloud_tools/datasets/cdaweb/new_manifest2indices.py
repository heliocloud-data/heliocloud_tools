import bisect
import csv
from datetime import datetime
import json
import os
import re

s3prefix = "s3://gov-nasa-hdrl-data1/spdf/cdaweb/"
#catalog = "catstub.json" # catalog-cdaweb.json
catalog = "catalog-cdaweb.json"
#fname = "minimanifest.csv" # "jun-10_manifest_spdf.csv"
#fname = "medmanifest.csv"
fname = "jun-10_manifest_spdf.csv"
FILENAME_EXT_RE = re.compile(r"\.(cdf|nc|fits|fts)$", re.IGNORECASE)
errorfile = "cdaweb_errors.txt"
validfile = "cdaweb_valids.txt"

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
        iso_times.append(dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z")

    return iso_times



##### MAIN #####

with open("catalog-cdaweb.json", "r") as f:
    data = json.load(f)
info = {
    item["subpath"] : {"id": item["id"],
                       "regex": item["regex"],
                       "pattern": template_to_regex_multi(item["regex"])
                       }
    for item in data.get("catalog", [])
}
allstarts = sorted(info.keys())

currentid = None
currentyear = None
currentindex = None
errorlist = {}
validlist = []
fsize = 0
with open(fname, mode="r") as f:
    for line in f:
        line=line[12:-1]
        if FILENAME_EXT_RE.search(line):
            prefix = find_prefix(line, allstarts)
            if prefix is None:
                errorlist[os.path.dirname(line)] = line
                continue
            if info[prefix]["id"] != currentid:
                # new index time
                validlist.append(f"{id}\n")
                try:
                    fout.close()
                except:
                    pass
                currentid = info[prefix]["id"]
                regex = info[prefix]["regex"]
                pattern = info[prefix]["pattern"]
                currentyear = None
            year = year_from_filename(line, regex)
            if year != currentyear:
                currentyear = year
                foutname = f"indices/{currentid}_{currentyear}.csv"
                fout = open(foutname,"w")
                print("Opening new index",currentid, currentyear)
            try:
                isos = isos_from_template_and_filename(pattern,
                                                       os.path.basename(line))
            except:
                errorlist[os.path.dirname(line)] = line
                continue
            
            try:
                end=isos[1]
            except:
                end=isos[0]
            fout.write(f"{isos[0]},{end},{s3prefix}{line},{fsize}\n")


print(f"{len(validlist)} good IDs indexed, see {validfile} for details")
with open(validfile,"w") as fout:
    fout.writelines(validlist)
            
print(f"{len(errorlist.keys())} bad path/datasets, see {errorfile} for details")
with open(errorfile,"w") as eout:
    for mykey in sorted(errorlist.keys()):
        eout.write(f"{mykey},{errorlist[mykey]}\n")
