"""
Guesses at dataids and regex patterns from an AWS manifest of sdac holdings.
"""
import re

# In priority order
#PATTERNS = [
#    (r"\d{8}T\d{6}",      "%Y%m%dT%h%m%s"),   # 1) YYYYMMDDTHHMMSS
#    (r"\d{8}_\d{6}",      "%Y%m%d_%h%m%s"),   # 2) YYYYMMDD_HHMMSS
#    (r"\d{8}_\d{4}",      "%Y%m%d_%h%m"),     # 3) YYYYMMDD_HHMM
#    (r"\d{8}",            "%Y%m%d"),          # 4) YYYYMMDD
#    (r"\d{4}/\d{2}/\d{2}", "%Y/%m/%d"),       # 5) YYYY/MM/DD
#]

PATTERNS = [
    # 1) YYYYMMDDTHHMMSS
    (r"\d{8}T\d{6}", "T"),
    # 2) YYYYMMDD_HHMM[SS]  (seconds optional)
    (r"\d{8}_\d{4}(\d{2})?", "_"),
    # 3) YYYYMMDD
    (r"\d{8}", "DATE8"),
    # 4) YYYY/MM/DD
    (r"\d{4}/\d{2}/\d{2}", "DATE_SLASH"),
]

def find_datetime_with_format(s: str):
    """
    Return (match_obj, format_string) based on priority.
    format_string is your token form: %Y%m%dT%h%m%s, etc.
    """
    for pat, kind in PATTERNS:
        m = re.search(pat, s)
        if not m:
            continue

        dt = m.group(0)

        if kind == "T":  # YYYYMMDDTHHMMSS
            fmt = "%Y%m%dT%h%m%s"

        elif kind == "_":  # YYYYMMDD_HHMM[SS]
            # Decide based on total length after the underscore
            # dt looks like: 'YYYYMMDD_HHMM' or 'YYYYMMDD_HHMMSS'
            after_underscore = dt.split("_", 1)[1]
            if len(after_underscore) == 4:
                fmt = "%Y%m%d_%h%m"       # no seconds
            else:
                fmt = "%Y%m%d_%h%m%s"     # with seconds

        elif kind == "DATE8":  # YYYYMMDD
            fmt = "%Y%m%d"

        elif kind == "DATE_SLASH":  # YYYY/MM/DD
            fmt = "%Y/%m/%d"

        else:
            raise ValueError(f"Unhandled kind {kind}")

        return m, fmt

    return None, None


def orig_find_datetime_with_format(s: str):
    """
    Returns (match_obj, format_string) or (None, None)
    according to the priority list.
    """
    for pat, fmt in PATTERNS:
        m = re.search(pat, s)
        if m:
            return m, fmt
    return None, None

def derive_pattern(line: str) -> str:
    m, fmt = find_datetime_with_format(line)
    if not m:
        # No datetime at all
        return "%Q"

    start, end = m.span()
    prefix = line[:start]
    suffix = line[end:]  # currently not inspected, just collapsed to %Q

    # Preserve one trailing non-alphanumeric separator in the prefix
    sep = ""
    if prefix and not prefix[-1].isalnum():
        sep = prefix[-1]

    # Build final pattern: "%Q<sep><datetime_fmt>%Q"
    return f"%Q{sep}{fmt}%Q"

def pattern_to_regex(pattern: str) -> str:
    # Escape regex special chars except what we will substitute
    p = pattern

    # Replace escaped tokens with regex equivalents
    replacements = {
        r"%Q": ".*",
        r"%Y": r"\d{4}",
        r"%m": r"\d{2}",
        r"%d": r"\d{2}",
        r"%h": r"\d{2}",
        r"%s": r"(?:\d{2})?", # seconds are optional
    }
    for k, v in replacements.items():
        p = p.replace(k, v)

    return "^" + p + "$"


def test_pattern(pattern: str, line: str) -> bool:
    regex = pattern_to_regex(pattern)
    return bool(re.match(regex, line))


def extract_most_missions(line,filename):       
    m = re.search(r"\d{4}/\d{2}/\d{2}/(.+?)/[^/]+$", line)
    if m:
        identifier = m.group(1)
        identifier = identifier.split('/')[0]
        identifier = re.sub(r"/?H\d{4}$","",identifier)
        if len(identifier) == 0:
            identifier = re.split(r"(?<!\d)\d{4}", filename, 1)[0]
    else:
        identifier = re.split(r"(?<!\d)\d{4}", filename, 1)[0]
        identifier = re.sub(r"_*$","",identifier) # clean training _s
    return identifier

def extract_mostmissions(line,filename):       
    m = re.search(r"\d{4}/\d{2}/\d{2}/(.+?)/[^/]+$", line)
    if m:
        identifier = m.group(1)
        identifier = identifier.split('/')[0]
        identifier = re.sub(r"/?H\d{4}$","",identifier)
        if len(identifier) == 0:
            identifier = re.split(r"(?<!\d)\d{4}", filename, 1)[0]
    else:
        identifier = re.split(r"(?<!\d)\d{4}", filename, 1)[0]
        identifier = re.sub(r"_*$","",identifier) # clean training _s
    return identifier

def extract_fermi(line,filename):       
    m = re.search(r"\d{4}/\d{2}/\d{2}/(.+?)/[^/]+$", line)
    if m:
        identifier = m.group(1)
        identifier = identifier.split('/')[0]
        identifier = re.sub(r"/?H\d{4}$","",identifier)
        if len(identifier) == 0:
            identifier = re.split(r"(?<!\d)\d{4}", filename, 1)[0]
    else:
        identifier = re.split(r"(?<!\d)\d{4}", filename, 1)[0]
        identifier = re.sub(r"_*$","",identifier) # clean training _s
    return identifier

def s2c_main():

    datepatterns = {}
    indexhomes = {}
    identifiers = {}

    fname = "manifest_sorted_sdac.csv"
    with open(fname) as fin:
        for line in fin:
            line = re.split(',',line)[0]
            #items = re.split(r'(?<!\d)\d{4}', line, 1)
            #indexhome = items[0]
            indexhome = '/'.join(line.split('/')[0:2]) + '/indices/'
            
            pieces = line.split('/')
            filename = pieces[-1]

            # one of two forms:  path/YYYY/MM/DD/identifier/Hxxx/filename or
            #                    path/YYYY/MM/DD/identifier/filename or
            #                    path/identifier_filename

            identifier = extract_most_missions(line,filename)

            if re.search(r"^\d{4}$", pieces[2]):
                # remove _YYYY_ from paths, e.g. sdac/trace/2008
                prefix = pieces[1]
            else:
                prefix = pieces[1] + "_" + pieces[2]
            dataid = prefix + "_" + identifier
        
            if dataid not in datepatterns.keys():
                regex = derive_pattern(line)
                datepatterns[dataid] = regex
                identifiers[dataid] = "/" + identifier
                indexhomes[dataid] = indexhome
                print(f"Found dataid {dataid}, indexhome {indexhome}, datepattern {regex}, identifier {identifier} : {line}")
            else:
                # run verifier
                m = re.match(pattern_to_regex(datepatterns[dataid]),line)
                if not m:
                    # know alt, %Q_%Y%m%dT%h%m%s%Q   OR   %Q_%Y%m%d_%Q
                    # so update to allow for this special case
                    print("\tFailed pattern, adding alt on",line[0:-2])
                    datepatterns[dataid] = "%Q_%Y%m%d(?:T%h%m%s%Q|_%Q)"
                    m = re.match(pattern_to_regex(datepatterns[dataid]),line)
                    if not m:
                        print("\tFailed twice with ",datepatterns[dataid],"on",line[0:-2])

    with open("sdac_metadata.csv","w") as fout:
        fout.write("dataid,matchme,indexhome,datepattern\n")
        for dataid in sorted(datepatterns.keys()):
            fout.write(f"{dataid},{identifiers[dataid]},{indexhomes[dataid]},{datepatterns[dataid]}\n")

if __name__ == "__main__":
    s2c_main()
    
