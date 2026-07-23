"""
Convert CDAWeb-ish XML into catalog JSON objects using mappings:

serviceprovider_ID -> id
description -> title
timerange_start -> start (ISO: YYYY-MM-DDTHH:MM:SS.SSSZ)
timerange_stop  -> stop  (ISO: YYYY-MM-DDTHH:MM:SS.SSSZ)
filenaming -> regex
URL -> subpath (minus https://cdaweb.gsfc.nasa.gov/pub/ part)
URL -> resource
data_producer name + affiliation -> contact
index -> s3://gov-nasa-hdrl-data1/spdf/cdaweb/[subpath]
about -> https://cdaweb.gsfc.nasa.gov/misc/Notes[firstletter].html#[id]
collections -> ["CDAWeb"]

Notes:
- In the sample XML I’ve seen, serviceprovider_ID/timerange_* live on dataset attributes, and filenaming on access attributes.
- This script checks both attributes and child elements to be robust.
"""

from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


PUB_PREFIX = "https://cdaweb.gsfc.nasa.gov/pub/"
S3_PREFIX = "s3://gov-nasa-hdrl-data1/spdf/cdaweb/"
ABOUT_PREFIX = "https://cdaweb.gsfc.nasa.gov/misc/"


def _local(tag: str) -> str:
    """Strip XML namespace, returning localname."""
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _find_child_text(elem: ET.Element, child_localname: str) -> Optional[str]:
    for c in list(elem):
        if _local(c.tag) == child_localname and c.text:
            t = c.text.strip()
            return t if t else None
    return None


def _get(elem: ET.Element, attr: str, child_localname: str) -> Optional[str]:
    """Prefer attribute; fall back to direct child element text."""
    v = elem.attrib.get(attr)
    if v is not None:
        v = v.strip()
        return v if v else None
    return _find_child_text(elem, child_localname)


def _parse_time_to_iso_z(value: str) -> str:
    """
    Convert common CDAWeb timestamp strings to ISO-8601 with milliseconds and Z.
    Examples seen:
      2001-08-24 16:16:12
      2001-08-24T16:16:12
      2001-08-24 16:16:12.123
      2001-08-24T16:16:12.123Z
    If no timezone is present, treat as UTC.
    """
    s = value.strip()

    # Normalize 'Z' if present
    if s.endswith("Z"):
        s_noz = s[:-1]
        tz = timezone.utc
    else:
        s_noz = s
        tz = timezone.utc  # assumption per requirement; adjust if you have a different convention

    # Replace space with T for parsing convenience
    s_noz = s_noz.replace(" ", "T")

    # Try multiple formats
    fmts = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ]
    dt = None
    for fmt in fmts:
        try:
            dt = datetime.strptime(s_noz, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        raise ValueError(f"Unrecognized datetime format: {value!r}")

    dt = dt.replace(tzinfo=tz)

    # Ensure exactly 3-digit milliseconds
    ms = int(dt.microsecond / 1000)
    return dt.replace(microsecond=ms * 1000).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def _url_to_subpath(url: str) -> str:
    u = url.strip()
    if not u.startswith(PUB_PREFIX):
        # If it doesn't match, still return something stable:
        # strip scheme/host and leading slashes.
        u = re.sub(r"^https?://[^/]+/", "", u)
        return u.strip("/")

    sub = u[len(PUB_PREFIX):]
    return sub.strip("/")


def _dataset_contact(dataset: ET.Element) -> Optional[str]:
    # data_producer may be an element (possibly with attributes)
    producer = None
    for c in list(dataset):
        if _local(c.tag) == "data_producer":
            producer = c
            break
    if producer is None:
        return None

    name = producer.attrib.get("name")
    affiliation = producer.attrib.get("affiliation")

    # Fall back to child elements if present in other variants
    if not name:
        name = _find_child_text(producer, "name")
    if not affiliation:
        affiliation = _find_child_text(producer, "affiliation")

    name = (name or "").strip() or None
    affiliation = (affiliation or "").strip() or None

    if name and affiliation:
        return f"{name} ({affiliation})"
    return name or affiliation


def _about_url(dataset_id: str) -> str:
    if not dataset_id:
        raise ValueError("Cannot build 'about' without id")
    first = dataset_id[0].upper()
    return f"{ABOUT_PREFIX}Notes{first}.html#{dataset_id}"


def xml_to_catalog(xml_path: str) -> List[Dict[str, Any]]:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    catalog: List[Dict[str, Any]] = []

    # Find all dataset elements regardless of namespace
    for elem in root.iter():
        if _local(elem.tag) != "dataset":
            continue

        # dataset-level fields (often attributes)
        dataset_id = _get(elem, "serviceprovider_ID", "serviceprovider_ID")
        title = _get(elem, "description", "description")

        # timerange can be on dataset or access
        start_raw = _get(elem, "timerange_start", "timerange_start")
        stop_raw = _get(elem, "timerange_stop", "timerange_stop")

        # access child
        access = None
        for c in list(elem):
            if _local(c.tag) == "access":
                access = c
                break

        url = None
        regex_pat = None
        if access is not None:
            regex_pat = _get(access, "filenaming", "filenaming")
            # URL is usually access/URL
            for c in list(access):
                if _local(c.tag) == "URL" and c.text:
                    url = c.text.strip()
                    break

            # Access may also carry timerange_* in some variants
            if start_raw is None:
                start_raw = _get(access, "timerange_start", "timerange_start")
            if stop_raw is None:
                stop_raw = _get(access, "timerange_stop", "timerange_stop")

        if not dataset_id:
            # Skip datasets that don’t have an id
            continue
        if not url:
            # If URL is missing, you can decide to skip or keep; here we skip.
            continue

        subpath = _url_to_subpath(url)
        obj: Dict[str, Any] = {
            "id": dataset_id,
            "title": title or "",
            "start": _parse_time_to_iso_z(start_raw) if start_raw else None,
            "stop": _parse_time_to_iso_z(stop_raw) if stop_raw else None,
            "regex": regex_pat or "",
            "subpath": subpath,
            "resource": url,  # per your mapping: URL -> resource
            "contact": _dataset_contact(elem) or "",
            "index": f"{S3_PREFIX}{subpath}/",
            "about": _about_url(dataset_id),
            "collections": ["CDAWeb"],
        }

        # Remove None fields if desired
        obj = {k: v for k, v in obj.items() if v is not None}

        catalog.append(obj)

    return catalog


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("xml", help="Input XML file")
    ap.add_argument("-o", "--out", required=True, help="Output JSON file")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = ap.parse_args()

    catalog = xml_to_catalog(args.xml)

    with open(args.out, "w", encoding="utf-8") as f:
        if args.pretty:
            json.dump(catalog, f, indent=2, ensure_ascii=False)
        else:
            json.dump(catalog, f, separators=(",", ":"), ensure_ascii=False)


if __name__ == "__main__":
    main()
