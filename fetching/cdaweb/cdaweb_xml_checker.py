import xml.etree.ElementTree as ET
import os
import requests
import re
from datetime import datetime, timedelta
from collections import defaultdict

"""
Parsing via https://spdf.gsfc.nasa.gov/pub/catalogs/00readme.txt
Note they say the 'all.xml' is incomplete, so for unlisted items
we do a best guess on YYYYMMDD.

"""

def load_fromxml(homepath = ".", strip_me = None):
    # Define file name and URL
    FILE_NAME = f"{homepath}/all.xml"
    FILE_URL = "https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml"

    # Check if file exists locally; if not, download it
    if not os.path.exists(FILE_NAME):
        print(f"Downloading {FILE_NAME}...")
        response = requests.get(FILE_URL)
        with open(FILE_NAME, "wb") as file:
            file.write(response.content)

    # Parse XML from the local file
    with open(FILE_NAME, "r", encoding="utf-8") as file:
        tree = ET.parse(file)
        root = tree.getroot()

    # Extract namespace dynamically
    namespace = root.tag.split("}")[0].strip("{")  # Get namespace from root tag
    ns_map = {"cdas": namespace}  # Namespace dictionary

    # Load XML data into dictionaries
    regex_base = {}
    regex_pattern = {}

    # Iterate over dataset elements
    for dataset in root.findall(".//cdas:dataset", ns_map):
        # Extract serviceprovider_ID from dataset attributes
        dataid = dataset.attrib.get("serviceprovider_ID", "").strip()

        # Extract URL and filenaming from inside <access>
        access_element = dataset.find(".//cdas:access", ns_map)
        if access_element is not None:
            url_element = access_element.find("cdas:URL", ns_map)
            filenaming = access_element.attrib.get("filenaming", "").strip()
        
            if url_element is not None:
                url_text = url_element.text.strip()
                if url_text.startswith("https://cdaweb"):
                    url_cleaned = re.sub(r"https://.*?.nasa.gov/", "", url_text)  # Remove web address
                    if strip_me != None:
                        url_cleaned = re.sub(f"^{strip_me}","",url_cleaned)
                    regex_base[dataid] = url_cleaned
                    regex_pattern[dataid] = strftime_to_regex(filenaming)
                    
    #print(len(regex_base.keys()),len(regex_pattern.keys()))
    return regex_base, regex_pattern

def guess_regex(basename):
    patterns = ["_%Y%m%d%H%M%S_",
                "_%Y%m%d%H%M_",
                "_%Y%m%d_",
                "_%Y%m_",
                "_%Y%j_",
                "_%Y_%j_",
                "_%Y%m%dt%H%M%S"
                ]
    patterns = [strftime_to_regex(p) for p in patterns]
    for x_p in patterns:
        if re.search(x_p,basename):
            return x_p
    return "0000"



### THIS NEEDS WORK!!!  But it'll be cleaner

def xml_to_dicts():
    # copy from earlier, outcome is:
    metadata = {}
    for xmldataid in xmlfilenamingplusxmldataids:
        filenaming = '???'# regex for parsing time
        url = '???'  # path for indices etc
        dataid = '???' # canonical dataid
        regex = re.sub('https://cdaweb.gsfc.nasa.gov/',"",filenaming)
        keyid = re.sub(r'(\/\d+)+$','',filenaming)
        metadata.setdefault(keyid, []).append((regex, url, dataid))
        dataids[rshort] = xmldataid
    return regexes, dataids

def efficient_regex(metadata, fullname, filename):
    keyid = re.sub(r'(\/\d+)+$','',os.path.dirname(fullname))
    try:
        reg_path_id_sets = metadata[keyid]
    except:
        # generate new entries on the fly
        x_reg = guess_regex(filename)
        regs = [x_reg]
        dataid_m = re.match(".*_\d{4}",basename)
        if dataid_m:
            dataid = dataid_m[0][:-5].upper()
        else:
            dataid = "None" # note use of string not NoneType
        reg_path_id_sets = [(x_reg, keyid, dataid)]
        metadata[keyid] = [reg_path_id]

    for trio in reg_path_id_sets:
        starttime = extract_datetime(filename, trio[0], form="str")
        if starttime != None:
            return trio[2], starttime, trio[0], trio[1]
    return None, None, None, None

def efficient_parse_line(line, metadata):
    valid = True
    if csvflag:
        lineset = line.rstrip().split(",")
    else:
        # CDAWeb space-delinated format
        lineset = line.rstrip().split()
    filesize = lineset[-2]
    fullname = lineset[-1]
    if strip_me != None and fullname.startswith(strip_me):
        fullname = fullname[len(strip_me):]
    filename = os.path.basename(fullname)
    dataid, starttime, x_reg, path = efficient_regex(metadata, fullname, filename)
    if dataid == "None":
        valid = False
    if starttime == None:
        global_badregexes[x_reg] = filename
        valid = False
    queueset = dataid + ":" + starttime[:4]
    if len(lineset) > 3 and lastdate != None and str2datetime(lineset[0]) > lastdate:
        status = 3
    else:
        status = 2
    return fullname, filename, filesize, starttime, dataid, queueset, status, valid





def extract_regex(regex_base, regex_pattern, fullname):
    # Function to retrieve base and regex by dataid aka serviceprovider_ID
    basename = os.path.basename(fullname)
    dataid_m = re.match(".*_\d{4}",basename)
    if dataid_m != None:
        dataid = dataid_m[0][:-5].upper()
        try:
            x_regex = regex_pattern[dataid]
            base = regex_base[dataid]
            return dataid, base, x_regex
        except:
            try:
                x_regex = regex_pattern[dataid+"_alt"]
                base = regex_base[dataid+"_alt"]
                return dataid, base, x_regex
            except:
                pass
    dataid, base, x_regex = slow_extract_regex(regex_base, regex_pattern, fullname)
    if dataid != None:
        return dataid, base, x_regex
    # not in 'all.xml', so let us add it
    x_regex = guess_regex(basename)
    dataid_m = re.match(".*_\d{4}",basename)
    if dataid_m == None:
        return None, None, None
    dataid = dataid_m[0][:-5].upper()
    base = os.path.dirname(fullname)
    regex_base[dataid] = base
    regex_pattern[dataid] = x_regex
    return dataid, base, x_regex

def slow_extract_regex(regex_base, regex_pattern, fullname):
    # Function to retrieve base and regex by dataid aka serviceprovider_ID
    basename = os.path.basename(fullname)
    for dataid, base in regex_base.items():
        if fullname.startswith(base):
            x_regex = regex_pattern[dataid]
            #x_regex_alt = regex_pattern.setdefault(f"{dataid}_alt", re.sub(r"%[a-zA-Z]", ".*", x_regex))
            #if f"{dataid}_alt" not in regex_pattern.keys():
            #    print("loading alt",dataid+"_alt",x_regex,fullname)
            # also ensuring the date regex will work later
            x_regex_alt = regex_pattern.setdefault(f"{dataid}_alt", re.sub(r"\(.*\)", ".*", x_regex))
            if re.search(x_regex_alt,basename):
                return dataid, base, x_regex

    return None, None, None  # No match found

def strftime_to_regex(pattern):
    # Replace %Q fields with ".*" as they are not date-related                  
    pattern = re.sub(r"%Q[0-9]*", ".*", pattern)

    format_mapping = {
        "%Y": ("year", r"\d{4}"),
        "%m": ("month", r"\d{2}"),
        "%d": ("day", r"\d{2}"),
        "%j": ("doy", r"\d{3}"),
        "%H": ("hour", r"\d{2}"),
        "%M": ("minute", r"\d{2}"),
        "%S": ("second", r"\d{2}")
    }

    group_counts = defaultdict(int)

    def replace_fmt(m):
        fmt = m.group(0)
        group_name, pattern_str = format_mapping[fmt]
        if group_counts[group_name] == 0:
            group_counts[group_name] += 1
            return f"(?P<{group_name}>{pattern_str})"
        else:
            return ""  # 🧹 Ignore duplicate field entirely

    # Replace only known formats; skip all others
    fmt_regex = re.compile("|".join(re.escape(k) for k in format_mapping.keys()))
    regex_pattern = fmt_regex.sub(replace_fmt, pattern)

    # ✂️ Trim everything after the last ')'
    last_paren = regex_pattern.rfind(')')
    if last_paren != -1:
        regex_pattern = regex_pattern[:last_paren + 1]
        regex_pattern += '_'

    return regex_pattern

# Function to extract datetime from filename using filenaming pattern
def extract_datetime(filename, regex_pattern, form='dt'):
    if not regex_pattern:
        return None  # No valid pattern provided
    match = re.search(regex_pattern, filename)

    if match == None:
        # somewhat iffy, there are some with 't' instead of 'T' etc
        match = re.search(regex_pattern, filename, re.IGNORECASE)

    #except:
    #    print(regex_pattern, filename)
    #    exit()
    if match:
        try:
            year = int(match.group("year"))
            month = int(match.group("month")) if "month" in match.groupdict() else 1
            day = int(match.group("day")) if "day" in match.groupdict() else 1
            
            # Handle DOY conversion to month/day
            if "doy" in match.groupdict():
                doy = int(match.group("doy"))
                date_from_doy = datetime(year, 1, 1) + timedelta(days=doy - 1)
                month, day = date_from_doy.month, date_from_doy.day

            mydate = datetime(
                year,
                month,
                day,
                int(match.group("hour")) if "hour" in match.groupdict() else 0,
                int(match.group("minute")) if "minute" in match.groupdict() else 0,
                int(match.group("second")) if "second" in match.groupdict() else 0
            )
            if form == "str":
                return mydate.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                return mydate

        except: # ValueError:
            if '?P' not in regex_pattern:
                # regex has no data info, so not our problem
                return "0000"
            else:
                return None  # Invalid date components

    return None  # No match found

# Test case
def test_case():
    log_entry = "2003-12-16T19:59:22.0000000000 GMT     189952 pub/data/isis/topside_sounder/ionogram_cdf/isis1/ODG_14N_359E/1969/345/i1_av_odg_1969345140242_v01.cdf"
    fullname = log_entry.split()[-1]
    
    regex_base, regex_pattern = load_fromxml()
    x_id, x_base, x_regex = extract_regex(regex_base, regex_pattern, fullname)
    if x_regex:
        x_datetime, failcheck = extract_datetime(os.path.basename(fullname), x_regex)
        print("Extracted Datetime:", x_datetime)
    else:
        print("No valid filenaming pattern found.")

    print("Extracted Service Provider ID:", x_id, "from", fullname)
    print("Extracted base path:", x_base)

if __name__ == "__main__":
    test_case()
