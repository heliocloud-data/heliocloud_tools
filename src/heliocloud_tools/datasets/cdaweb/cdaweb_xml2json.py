"""
Convert CDAWeb XML into catalog JSON objects using mappings:

usage: python cdaweb_xml2json.py -x all.xml -o catalog-cdaweb.json
  or   python cdaweb_xml2json.py --fetchxml -o catalog-cdaweb.json

Known bug: if CDAWeb XML elements have start/stop times of 'AUTO' or 'Recent'
these get mapped to JSON null, which might break downstream software that
is expecting ISO times.

XML-to-JSON mapping is:
serviceprovider_ID -> id
description -> title
timerange_start -> start (ISO: YYYY-MM-DDTHH:MM:SS.SSSZ)
timerange_stop  -> stop  (ISO: YYYY-MM-DDTHH:MM:SS.SSSZ)
filenaming -> regex
URL -> subpath (minus https://cdaweb.gsfc.nasa.gov/pub/ part)
URL -> resource
data_producer name + affiliation -> contact
index -> s3://gov-nasa-hdrl-data1/spdf/cdaweb/[subpath]
   or    s3://gov-nasa-hdrl-data1/spdf/cdaweb/data/indices/ [default]
about -> https://cdaweb.gsfc.nasa.gov/misc/Notes[firstletter].html#[id]
collections -> ["CDAWeb"]

Flags: default is to write all indices to $INDEX_LOC, --localindex instead
puts the indices next to the data.

"""

from __future__ import annotations

import argparse
import json
import os
import re
import requests
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


PUB_PREFIX = "https://cdaweb.gsfc.nasa.gov/pub/"
S3_PREFIX = "s3://gov-nasa-hdrl-data1/spdf/cdaweb/"
INDEX_LOC = "s3://gov-nasa-hdrl-data1/spdf/cdaweb/data/indices/"
ABOUT_PREFIX = "https://cdaweb.gsfc.nasa.gov/misc/"
XML_URL = "https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml"

# Require filenaming to end with one of these extensions (at end-of-string)
FILENAME_EXT_RE = re.compile(r"\.(cdf|nc|fits|fts)$", re.IGNORECASE)


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _find_direct_child(elem: ET.Element, child_localname: str) -> List[ET.Element]:
    return [c for c in list(elem) if _local(c.tag) == child_localname]


def _find_child_text(elem: ET.Element, child_localname: str) -> Optional[str]:
    for c in list(elem):
        if _local(c.tag) == child_localname and c.text:
            t = c.text.strip()
            return t if t else None
    return None


def _get(elem: ET.Element, attr: str, child_localname: str) -> Optional[str]:
    v = elem.attrib.get(attr)
    if v is not None:
        v = v.strip()
        return v if v else None
    return _find_child_text(elem, child_localname)


def _title_from_top_description_short(dataset: ET.Element) -> Optional[str]:
    for desc in _find_direct_child(dataset, "description"):
        short = desc.attrib.get("short")
        if short:
            short = short.strip()
            if short:
                return short
    return None


def _parse_time_to_iso_z(value: str) -> str:
    s = value.strip()

    if s.endswith("Z"):
        s_noz = s[:-1]
        tz = timezone.utc
    else:
        s_noz = s
        tz = timezone.utc

    s_noz = s_noz.replace(" ", "T")

    fmts = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            if s_noz.startswith('AUTO') or s_noz.startswith('Recent'):
                return None
            dt = datetime.strptime(s_noz, fmt)
            if fmt == "%Y-%m-%d":
                dt = dt.replace(hour=0,minute=0,second=0,microsecond=0)
            dt = dt.replace(tzinfo=tz)
            ms = int(dt.microsecond / 1000)
            return (
                dt.replace(microsecond=ms * 1000)
                .strftime("%Y-%m-%dT%H:%M:%S.")
                + f"{ms:03d}Z"
            )
        except ValueError:
            continue
    raise ValueError(f"Unrecognized datetime format: {value!r}")


def _url_to_subpath(url: str) -> str:
    u = url.strip()
    if not u.startswith(PUB_PREFIX):
        u = re.sub(r"^https?://[^/]+/", "", u)
        return u.strip("/")
    return u[len(PUB_PREFIX):].strip("/")


def _dataset_contact(dataset: ET.Element) -> Optional[str]:
    producer = None
    for c in list(dataset):
        if _local(c.tag) == "data_producer":
            producer = c
            break
    if producer is None:
        return None

    name = (producer.attrib.get("name") or _find_child_text(producer, "name") or "").strip() or None
    aff = (producer.attrib.get("affiliation") or _find_child_text(producer, "affiliation") or "").strip() or None

    if name and aff:
        return f"{name} ({aff})"
    return name or aff


def _about_url(dataset_id: str) -> str:
    first = dataset_id[0].upper()
    return f"{ABOUT_PREFIX}Notes{first}.html#{dataset_id}"


def xml_to_catalog(xml_path: str, localindex: Bool) -> List[Dict[str, Any]]:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    catalog: List[Dict[str, Any]] = []

    errors = []
    for elem in root.iter():
        if _local(elem.tag) != "dataset":
            continue
        dataset_id = _get(elem, "serviceprovider_ID", "serviceprovider_ID")
        if not dataset_id:
            continue

        title = _title_from_top_description_short(elem)
        if not title:
            title = _get(elem, "description", "description") or ""

        start_raw = _get(elem, "timerange_start", "timerange_start")
        stop_raw = _get(elem, "timerange_stop", "timerange_stop")

        access = None
        for c in list(elem):
            if _local(c.tag) == "access":
                access = c
                break

        url = None
        regex_pat = None
        if access is not None:
            regex_pat = _get(access, "filenaming", "filenaming")
            for c in list(access):
                if _local(c.tag) == "URL" and c.text:
                    url = c.text.strip()
                    break

            if start_raw is None:
                start_raw = _get(access, "timerange_start", "timerange_start")
            if stop_raw is None:
                stop_raw = _get(access, "timerange_stop", "timerange_stop")

        # NEW: filter by filenaming suffix
        if not regex_pat or not FILENAME_EXT_RE.search(regex_pat):
            errors.append(f"wrong filetype: {dataset_id},{regex_pat}\n")
            continue

        if not url:
            errors.append(f"no url: {dataset_id},{regex_pat}")
            continue

        subpath = _url_to_subpath(url)
        if localindex:
            index = f"{S3_PREFIX}{subpath}/"
        else:
            index = f"{INDEX_LOC}"
            
        obj: Dict[str, Any] = {
            "id": dataset_id,
            "title": title,
            "index": index,  # 3rd element
            "start": _parse_time_to_iso_z(start_raw) if start_raw else None,
            "stop": _parse_time_to_iso_z(stop_raw) if stop_raw else None,
            "regex": regex_pat,
            "subpath": subpath,
            "resource": url,
            "contact": _dataset_contact(elem) or "",
            "about": _about_url(dataset_id),
            "collections": ["CDAWeb"],
        }

        #obj = {k: v for k, v in obj.items() if v is not None}
        obj = {k: v for k, v in obj.items()}
        catalog.append(obj)

    return catalog, errors

def fetch_xml():
    localfile = "all.xml"
    response = requests.get(XML_URL)
    if os.path.exists(localfile):
        # version existing file
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        versioned_path = f"{localfile}.{timestamp}.bak"
        shutil.move(localfile, versioned_path)
    with open(localfile,"wb") as file:
        file.write(response.content)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-x","--xml", required=False, help="Input XML file")
    ap.add_argument("-o", "--out", required=True, help="Output JSON file")
    ap.add_argument("--fetchxml", action="store_true", help="Fetch latest all.xml")
    ap.add_argument("--localindex", action="store_true", help="store indexes next to data, not in toplevel")
    args = ap.parse_args()

    if args.fetchxml:
        fetch_xml()
        xmlfile = 'all.xml'
    elif args.xml:
        xmlfile = args.xml
    else:
        print("Error, no xml file specified, exiting.")
        exit()
        
    catalog_items, errors = xml_to_catalog(xmlfile,args.localindex)

    output = {
        "Cloudy": "1.1",
        "endpoint": "s3://gov-nasa-hdrl-data1/",
        "name": "GSFC HelioCloud",
        "contact": "Brian Thomas, brian.a.thomas@nasa.gov",
        "description": "NASA TOPS ODR datasets",
        "citation": "tbd",
        "catalog": catalog_items,
        "status": {
            "code": 1200,
            "message": "OK request successful",
        },
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    if len(errors) > 0:
        with open("xml_errors.txt", "w", encoding="utf-8") as f:
            f.writelines(errors)
            

if __name__ == "__main__":
    main()
