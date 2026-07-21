"""
Terms:
   TOPS bucket: NASA-provided free S3, s3://gov-nasa-gsfc-data1/
   staging bucket: where to temporarily transfer files into, likely s3://helio-data-staging
   s3destination = eventual final home for the files, likely TOPS bucket
   staging fileRegistry = fileRegistries in the staging bucket
   canonical fileRegistry = fileRegistries in the s3destination

Update process: fetch latest data files, then move to destination and update registries

ADDED: optionally creates a {movelogdir}mastercache_movelog.json file at the close of a run,
making it easier to assimilate items and also check which runs have been completed.

Fetch latest data files code:
1) get catalog of all CDAWeb IDs
    either local file, fetch of full list, or similar
2) diff against DB:catalog.json to get added or dropped datasets
3) for each ID, get deltas (all items if a new set)
    a) iterate by year
    b) for each item
         if item already exists in canonical fileRegistry, do not fetch or do anything
         otherwise, fetch item, and register to staging fileRegistry
    c) add this id to the 'move-over' list for the next step (unless no changes were done)
    d) optionally, S3 inventory and/or checksum and/or file existence checks on fetched items?
4) for each ID, get GAP file for id
       for each 'old/new' pair, verify new is fetched, and write name of 'old' to 'deleteme' list
5) run 'move-over' code for that ID once ID is entirely done

Move-over code: go from helio-data-staging to TOPS
1) read 'move-over' list indicating which IDs have new/changed contents
2) for each ID:
     a) move files to appropriate s3destination
     b) take new fileRegistry and add to canonical fileRegistry
     c) update db:catalog.json with any change in stop, modification, start
     d) optionally, S3 Inventory or other check that files were copied over successfully
     e) write destination catalog.json from DB:catalog.json
     f) clean out staging area once safely done
     g) delete and deregister obsolete files from 'deleteme' list

Note that fileRegistries are named [id]_YYYY.csv and consist of a CSV header + lines:
   start,s3key,size,any additional items,,,
They reside in the sub-bucket for that [id]
Also, the 'catalog.json' list of holdings is in the root directory of s3destination

Tried using 's3fs' for more elegant writes, but it kept hanging on .close()
so switched to straightforward but less direct boto3 .upload_file()


"""

import requests
import re
import os
import json
import shutil
import urllib
import boto3
import datetime
from dateutil import parser
import magic
from typing import Dict, List, Tuple, Any, IO, Optional, Union
import logging
import multiprocessing_logging
import time
import shutil
import urllib.parse

""" General driver routines here, should work for most cases """


def bundleme(
    s3staging: str,
    s3destination: str,
    movelogdir: str,
    stripuri: str,
    extrameta: Optional[Dict[str, str]] = None,
    fetchlocal=None,
):
    """
    Bundles the core parameters required for any run.

    :param s3staging: The S3 staging bucket to use.
    :param s3destination: The S3 destination bucket to use.
    :param movelogdir: The directory to which move logs should be written.
    :param stripuri: The URI prefix to strip from S3 keys when moving files.
    :param extrameta: (Optional) A dictionary of extra metadata to include in the run.
    :param fetchlocal: (Optional) A path to locally avail files rather than a URI

    :returns: A dictionary containing the bundled parameters.
    """
    # core items needed for any run, None = defined at runtime
    sinfo = {
        "s3staging": s3staging,
        "s3destination": s3destination,
        "movelogdir": movelogdir,
        "stripuri": stripuri,
        "extrameta": extrameta,
        "filetype": None,
        "registryloc": None,
        "fetchlocal": fetchlocal,
    }
    return sinfo


def init_logger(logfile=None, loglevel=None):
    if loglevel.lower() == "info":
        loglevel = logging.INFO  # default is INFO+ERROR msgs
    elif loglevel.lower() == "debug" or loglevel.lower() == "status":
        loglevel = logging.DEBUG  # everything gets logged
    elif loglevel.lower() == "error":
        loglevel = logging.ERROR  # absolute min, only error
    else:
        loglevel = logging.INFO  # default is INFO+ERROR msgs

    if logfile:
        logging.basicConfig(
            filename=logfile, level=loglevel, format="%(levelname)s %(message)s %(asctime)s"
        )
    else:
        logging.basicConfig(level=loglevel, format="%(levelname)s %(message)s %(asctime)s")
    multiprocessing_logging.install_mp_handler()  # put after basicConfig


def logme(message: str, data: str = "", type: str = "status") -> None:
    """
    Logs a message to the console with a specified type.

    :param message: The message to log.
    :param data: Additional data to include in the log message.
    :param type: The type of log message (either "status", "error", or "log").
    """
    if type == "error":
        logging.error(f"Error: {message} {data}")
    elif type == "log" or type == "info":
        logging.info(f"Log: {message} {data}")
    else:  # just an onscreen status
        logging.debug(f"Status: {message} {data}")


def getHAPIIDs(lasttime: str, catalogurl: str) -> Optional[List[str]]:
    """
    Gets a list of HAPI IDs from the specified HAPI catalog URL.

    :param lasttime: A string representing the last time the catalog was accessed, in ISO format (YYYY-MM-DDTHH:MM:SSZ).
    :param catalogurl: The URL of the HAPI catalog to query.

    :returns: A list of HAPI IDs that have been modified since the last time the catalog was accessed, or None if the request fails.
    """
    # fetch last modified date for that ID
    # for now, use current time
    lasttime = datetime.datetime.utcnow().isoformat()
    return lasttime


def filetype(fname):
    # 'imghdr' is useless and 'magic' thinks CDF is 'FoxPro FPT'
    ##cmd = shlex.split('file --mime-type {0}'.format(fname))
    ##result = subprocess.check_output(cmd)
    ##mime_type=result.split()[-1]
    result = magic.from_file(fname).split(" ")[0]
    if result.startswith("Fox"):
        result = "CDF"
    if result.startswith("Hierarch"):
        result = "HDF5"
    return result


def get_lastModified(item):
    # fetch last modified for that ID, default is current time
    lasttime = datetime.datetime.utcnow().isoformat()
    return lasttime


def getHAPIIDs(lasttime, catalogurl):
    # no longer needed, we fetch from CDAWeb itself now
    # catalogurl = "https://cdaweb.gsfc.nasa.gov/hapi/catalog"
    res = requests.get(catalogurl)
    j = res.json()
    if res.status_code == 200:
        return [item["id"] for item in j["catalog"] if get_lastModified(item) > lasttime]
    else:
        return None


def s3url_to_bucketkey(s3url: str) -> Tuple[str, str]:
    """
    Extracts the S3 bucket name and file key from an S3 URL.

    S3 paths are weird, bucket + everything else, e.g.
    s3://b1/b2/b3/t.txt would be bucket b1, file b2/b3/t.txt

    :param s3url: The S3 URL to extract the bucket name and file key from.

    :returns: A tuple containing the S3 bucket name and file key.
    """
    # S3 paths are weird, bucket + everything else, e.g.
    # s3://b1/b2/b3/t.txt would be bucket b1, file b2/b3/t.txt
    name2 = re.sub(r"s3://", "", s3url)
    s = name2.split("/", 1)
    mybucket = s[0]
    myfilekey = s[1] if len(s) > 1 else ""  # Want None if no key?
    return mybucket, myfilekey


def fetch_and_register(
    filelist: Dict[str, Any], sinfo: Dict[str, Any], logstring: str = ""
) -> Tuple[str, List[str]]:
    """
    Fetches files from URLs specified in a list of file descriptions, uploads them to an S3 staging bucket,
    and generates a list of strings for the CSV registry of the uploaded files.

    :param filelist: A dictionary containing the file descriptions.
        - "data": A list of dictionaries containing the file descriptions.
            - "URI": The URL of the file to fetch.
            - "start": The start date of the file.
            - "filesize": The size of the file.
            - Other optional metadata keys.
        - "key": The key to use in the file description dictionaries for the URI.
        - "start": The key to use in the file description dictionaries for the start date.
        - "filesize": The key to use in the file description dictionaries for the file size.
        - Other optional metadata keys (stop, checksum, checksum_algorithm) or None values if none exist.
    :param sinfo: A dictionary containing the S3 staging and destination bucket information and the URI prefix to strip from S3 keys.
                  Need keys: stripuri (fetching URL to replace with s3staging), s3staging, s3destination, and optionally extrameta and fetchlocal
          "fetchlocal": mode made initially for CDAWeb to fetch data via networked disk rathe than URI; field represents the path to replace the 'https://.*/' with

    :returns: A tuple containing the final destination directory for the uploaded files and the strings for the CSV registry of the uploaded files.
    """
    lastpath = "/"  # used later to avoid os calls
    logtime1 = datetime.datetime.now()
    logfsize = 0
    csvregistry = []

    startkey = filelist["start"]
    filesizekey = filelist["filesize"]

    if sinfo["s3staging"].startswith("s3://"):
        mys3 = boto3.client("s3")

    # start a Session object for efficiency
    mysession = requests.Session()

    for item in filelist["data"]:
        url_to_fetch = item[filelist["key"]]
        logme("fetching ", url_to_fetch)
        s3stub = re.sub(sinfo["stripuri"], "", url_to_fetch)
        stagingkey = sinfo["s3staging"] + s3stub
        s3key = sinfo["s3destination"] + s3stub
        # make any necessary subdirectories in staging
        (head, tail) = os.path.split(stagingkey)
        try:
            registryloc = re.sub(r"\d{4}$", "", registryloc)  # remove any year stub
            registryloc = os.path.commonprefix([registryloc, head])
        except:
            registryloc = head

        if stagingkey.startswith("s3://"):
            mybucket, myfilekey = s3url_to_bucketkey(stagingkey)
            tempfile = "/tmp/" + re.sub(r"/|:", "_", stagingkey)
        else:
            tempfile = stagingkey
            if head != lastpath:
                os.makedirs(head, exist_ok=True)
                lastpath = head

        if sinfo["fetchlocal"] != None:
            """
            Instead of fetching a URI and writing to 'tempfile', we
            copy a file on an adjascent disk.  Two cases as usual:
            (a) copy adjascent disk file to our local disk destination
            (b) copy adjascent disk file to S3 destination
            """
            # localfile = sinfo["fetchlocal"] + urllib.parse.urlparse(url_to_fetch).path
            # localfile = re.sub(r"//", "/", localfile)  # cleanup
            localfile = sinfo["fetchlocal"] + re.sub(sinfo["stripuri"], "", url_to_fetch)

            if stagingkey.startswith("s3://"):
                tempfile = localfile  # will later be staged
            else:
                if exists_anywhere(localfile, sinfo):
                    shutil.copy2(localfile, tempfile)
                else:
                    continue
            remaining_download_tries = 0  # skips the later URI download step
        else:
            # normal web fetch
            remaining_download_tries = 5

        while remaining_download_tries > 0:
            try:
                with mysession.get(url_to_fetch, stream=True) as r:
                    with open(tempfile, "wb") as f:
                        shutil.copyfileobj(r.raw, f)
                # print("Wrote ",tempfile)
                break
            except:
                # print("Error Timeout, trying again for ",url_to_fetch)
                remaining_download_tries -= 1
                time.sleep(1)
            """
            r=mysession.get(url_to_fetch)
            if r.status_code == 200:
                try:
                    with open(tempfile, 'wb') as fd:
                        fd.write(r.raw)
                    print("Good: ",r.status_code)
                    print("Good pt 2: ",r.headers)

                except:
                    print("***************")
                    print("Bad: ",r.status_code)
                    print("Bad pt 2: ",r.headers)
                    print("Bad pt 3: ",r.raw)
                    exit()
            else:
                error_msg = str(r.status_code) + ' ' + url_to_fetch
                remaining_download_tries -= 1
                time.sleep(1)
            """
            """ # original version
            try:
                urllib.request.urlretrieve(url_to_fetch,tempfile)
                
            except urllib.error.URLError as e:
                #print("FAILED, not got ",url_to_fetch)
                remaining_download_tries -= 1
                time.sleep(1)
                error_msg = e.reason
                #print("Error ",error_msg," tries is ",remaining_download_tries," for ",url_to_fetch)
            else:
                break
            """

        if remaining_download_tries <= 0 and sinfo["fetchlocal"] == None:
            logme("Failed to fetch", url_to_fetch, "error")
            continue

        if sinfo["filetype"] == None:  # define from 1st fetch
            sinfo["filetype"] = filetype(tempfile)

        if stagingkey.startswith("s3://"):
            mys3.upload_file(tempfile, mybucket, myfilekey)
            if sinfo["fetchlocal"] == None:
                os.remove(tempfile)

        csvitem = item[startkey] + "," + s3key + "," + str(item[filesizekey])
        logfsize += item[filesizekey]
        if filelist["stop"] != None:
            csvitem += "," + item[filelist["stop"]]
        if filelist["checksum"] != None:
            csvitem += "," + item[filelist["checksum"]]
        if filelist["checksum_algorithm"] != None:
            csvitem += "," + item[filelist["checksum_algorithm"]]
        if sinfo["extrameta"] != None:
            for extrakey in sinfo["extrameta"]:
                csvitem += "," + item[extrakey]

        logme(csvitem)
        csvregistry.append(csvitem)

    # registryloc = re.sub(r"\d{4}$","",registryloc) # remove any year stub
    if not re.search(r"/$", registryloc):
        registryloc += "/"  # all locs end in a "/"
    logme(logstring + " final destination was: ", registryloc + " ,", "log")

    sinfo["registryloc"] = registryloc

    logstr = f"{logfsize}:{datetime.datetime.now().timestamp()-logtime1.timestamp()} ,"
    logme(logstring + " Performance (bytes:seconds:timestamp):", logstr, "log")

    return csvregistry, sinfo


def registryname(id, year):
    return f"{id}_{year}.csv"


def exists_anywhere(fname: str) -> bool:
    """
    Checks if a file exists in S3 or locally.

    :param fname: The file path to check.

    :returns: True if the file exists, False otherwise.
    """
    if fname.startswith("s3://"):
        s3_client = boto3.client("s3")
        mybucket, myfilekey = s3url_to_bucketkey(fname)
        try:
            s3_client.get_object(Bucket=mybucket, Key=myfilekey)
            return True
        except s3_client.exceptions.NoSuchKey:
            return False
    else:
        if os.path.exists(fname):
            return True
        else:
            return False


def dataingest(fname: str, jsonflag: bool = False) -> Union[str, dict]:
    """
    Reads data from a file or S3 object and optionally parses it as JSON.

    :param fname: The path or S3 URL of the file/object to read.
    :param jsonflag: Whether to parse the data as JSON. Default is False.

    :returns: The data read from the file/object, either as a string or dictionary depending on the value of jsonflag.
    """
    if fname.startswith("s3://"):
        s3 = boto3.resource("s3")
        mybucket, myfilekey = s3url_to_bucketkey(fname)
        s3object = s3.Object(mybucket, myfilekey)
        # tempdata = s3object.get()["Body"].read().decode("utf-8")
        tempdata = s3object.get()["Body"].read().decode()
        if jsonflag:
            tempdata = json.loads(tempdata)
    else:
        with open(fname, "r") as fin:
            if jsonflag:
                tempdata = json.load(fin)
            else:
                tempdata = fin.read()

    return tempdata


def datadump(fname: str, tempdata: Union[str, dict], jsonflag: bool = False) -> None:
    """
    Writes data to a file, either locally or on S3.

    :param fname: The file path to write the data to.
    :param tempdata: The data to write to the file.
    :param jsonflag: Whether the data should be written as JSON. Defaults to False.
    """
    # works for local or S3
    # later, debate adding open(fname,"w",encoding="utf-8")
    if fname.startswith("s3://"):
        mys3 = boto3.client("s3")

        mybucket, myfilekey = s3url_to_bucketkey(fname)
        tempfile = "/tmp/" + re.sub(r"/|:", "_", fname)
        with open(tempfile, "w") as fout:
            if jsonflag:
                json.dump(tempdata, fout, indent=4, ensure_ascii=False)
            else:
                fout.write(tempdata)
        mys3.upload_file(tempfile, mybucket, myfilekey)
        os.remove(tempfile)
    else:
        with open(fname, "w") as fout:
            if jsonflag:
                json.dump(tempdata, fout, indent=4, ensure_ascii=False)
            else:
                fout.write(tempdata)  # local write


def uniquejson(jobj: Dict[str, Any], masterkey: str, uniquekey: str) -> Dict[str, Any]:
    """
    Removes duplicate items in a JSON object based on a specified key.

    :param jobj: The JSON object to be processed.
    :param masterkey: The key in the JSON object that contains the list of items.
    :param uniquekey: The key in the list of items that contains the unique identifier.
       or None if all keys must match
    :returns: A new JSON object with duplicate items removed. Also, edits jobj inplace (bug?).
    """
    movie = jobj[masterkey]
    unique_stuff = {elem[uniquekey]: elem for elem in movie}.values()

    jobj[masterkey] = []
    # cannot just assign jobj[masterkey]=unique_stuff as that breaks json.dump
    for ele in unique_stuff:
        jobj[masterkey].append(ele)
    return jobj


def replaceIsotime(catData: Dict[str, Any], mykey: str, maybetime: str) -> Dict[str, Any]:
    """
    Replaces an old isotime in the catalog data with maybetime if keys match.

    :param catData: The catalog data to modify.
    :param mykey: The key of the isotime to replace or add.
    :param maybetime: The new isotime value.

    :return: The modified catalog data.
    """
    # non-optimized way to conditionally replace an isotime in the catalog
    # if isotime does not exist, it gets added as an element
    unfound = True
    for i in range(len(catData["catalog"])):
        for key in catData["catalog"][i]:
            if key == mykey:
                unfound = False
                if maybetime > catData["catalog"][i][key]:
                    catData["catalog"][i][key] = maybetime

    if unfound:
        catData["catalog"].append({mykey: maybetime})

    return catData


def cda2iso(timey: str) -> str:
    """
    Converts a datetime string in ISO format to our specific string format (YYYY-MM-DDTHH:MM:SSZ).

    :param timey: The ISO datetime string to convert.
    :returns: The datetime string in our specific format.
    """
    # converts most any isotime to our specific string format
    # annoying reformat of YYYYMMDDTHHMMSSZ to YYYY-MM-DDTHH:MM:SSZ
    r = parser.parse(timey)
    timey = r.strftime("%Y-%m-%dT%H:%M:%SZ")
    # timey=datetime.datetime.fromisoformat(timey).isoformat() # only for python>3.11
    # timey = timey[0:4]+":"+timey[4:6]+":"+timey[6:11]+":"+timey[11:13]+":"+timey[13:]
    return timey


def iso2nodash(timey: str) -> str:
    """
    Removes dashes from an ISO-formatted datetime string.
    (for what cdaweb curl expects)
    time1="2022-01-01T00:00:00Z" -> time1="20220101T000000Z"

    :param timey: The ISO-formatted datetime string to remove dashes from.

    :returns: The datetime string with dashes removed: YYYYMMDDTHHMMSSZ
    """
    # removes dashes to what cdaweb curl expects
    # so time1="2022-01-01T00:00:00Z" -> time1="20220101T000000Z"
    r = parser.parse(timey)
    timey = r.strftime("%Y%m%dT%H%M%SZ")
    return timey


def write_registries(id: str, sinfo: Dict[str, Any], csvregistry: List[str]) -> None:
    """
    Creates files <id>_YYYY.csv with designated entries in the temporary directory s3staging+id/,
    which will later be moved (by the separate staging-to-production code) to the location as defined in catalog.csv
    as the field 'index'.

    :param id: The identifier for the registry file.
    :param registryloc: The directory where the registry file will be created.
    :param csvregistry: The list of entries to be included in the registry file.
    :param extrameta: (Optional) A list of extra metadata fields to include in the registry file.
    """
    keyset = ["start", "key", "filesize"]
    if sinfo["extrameta"] != None:
        keyset += sinfo["extrameta"]

    currentyear = "1000"  # junk year to compare against

    if not sinfo["s3staging"].startswith("s3://"):
        os.makedirs(sinfo["registryloc"], exist_ok=True)

    for line in csvregistry:
        year = line[0:4]
        if year != currentyear:
            try:
                datadump(fname, tempdata)
            except:
                pass
            currentyear = year
            fname = sinfo["registryloc"] + registryname(id, currentyear)
            logme("Creating registry ", fname, "status")
            header = "#" + ",".join(keyset) + "\n"
            tempdata = header
        line += "\n"
        tempdata += line
    datadump(fname, tempdata)


def fetch_catalogkeys():
    catalogkeys = [
        "id",
        "index",
        "title",
        "start",
        "stop",
        "modification",
        "indextype",
        "filetype",
        "description",
        "resource",
        "creation",
        "citation",
        "contact",
        "about",
    ]
    return catalogkeys


def create_catalog_stub(dataid, sinfo, catmeta, startDate, stopDate, appendflag=False):
    """Generates a catalog_stub.json file, suitable for
    adding to the s3destination catalog.json after merging.
    Location is s3staging+dataid+"/"+catalog_stub.json

    We use read-add-write rather than append because S3 lacks append-ability

    Note if you append it will add the same id and info to the catalog stub
    everytime you run the code.  The default is append=False because typical
    use is to create 1 stub in the destination that matches that one dataset.
    But I kept the append flag in case it is needed for future cases.

    typical optional catmeta are from the spec, e.g.:
    "catalog": [
            "id": "euvml",
            "index": "gov-nasa-helio-public/euvml/",
            "title": "EUV-ML dataset",
            "start": "1995-01-01T00:00Z",
            "stop": "2022-01-01T00:00Z",
            "modification": "2022-01-01T00:00Z",
            "indextype": "csv",
            "filetype": "fits",
            "description": "Optional description for dataset",
            "resource": "optional identifier e.g. SPASE ID",
            "creation": "optional ISO 8601 date/time of the dataset creation",
            "citation": "optional how to cite this dataset, DOI or similar",
            "contact": "optional contact info, SPASE ID, email, or ORCID",
            "about": "optional website URL for info, team, etc"
    """

    catalogkeys = fetch_catalogkeys()

    fstub = sinfo["registryloc"] + "catalog_stub.json"
    if appendflag and exists_anywhere(fstub):
        catData = dataingest(fstub, jsonflag=True)
    else:
        # new catalog
        catData = {"catalog": []}

    dstub = sinfo["registryloc"]
    dstub = re.sub(sinfo["s3staging"], sinfo["s3destination"], dstub)
    catmeta["index"] = dstub  # defined at runtime
    catmeta["filetype"] = sinfo["filetype"]  # defined at runtime

    for mykey in catalogkeys:
        if mykey in catmeta:
            catData["catalog"].append({mykey: catmeta[mykey]})

    catData = replaceIsotime(catData, "start", startDate)
    catData = replaceIsotime(catData, "stop", stopDate)

    datadump(fstub, catData, jsonflag=True)
    logme("Wrote catalog stub ", fstub, "status")


def blank_catalog(dataid):
    catalog = {}
    catalog["id"] = dataid
    return catalog


def hapi_info_to_catdata(dataid, hapiurl):
    """
    For datasets that have a HAPI info endpoint, fetch metadata from it
    Other datasets will need their own parsers for getting metadata
    """

    catalogkeys = fetch_catalogkeys()

    headers = {"Accept": "application/json"}
    res = requests.get(hapiurl, headers=headers)
    if res.status_code == 200:
        try:
            j = res.json()
            print("hapi j", j)
        except:
            print("failed with url ", hapiurl)
            j = blank_catalog(dataid)

    catmeta = {}
    for mykey in catalogkeys:
        if mykey in j["parameters"]:
            catmeta[mykey] = j[mykey]

    return catmeta


def gatherkeys(sinfo, flist):
    # THIS IS TERRIBLE CODE
    optkeys = []
    if flist["stop"] != None:
        optkeys.append("stop")
    if flist["checksum"] != None:
        optkeys.append("checksum")
    if flist["checksum_algorithm"] != None:
        optkeys.append("checksum_algorithm")
    if sinfo["extrameta"] != None:
        extrameta = optkeys + sinfo["extrameta"]
    elif len(optkeys) > 0:
        extrameta = optkeys
    return extrameta


def remove_processed(movelogdir: str, allIDs: List[str]) -> List[str]:
    """
    Remove the IDs that are already completed by checking the move log files.

    :param movelogdir: The directory containing the move log files.
    :param allIDs: A list of all the IDs.

    :return: A list of IDs that are ready to process

    Checks first for the optional "movelog_mastercache.json" of all previously
    processed IDs; anything not in there, it checks if a per-dataset movelog
    exists.
    """
    if exists_anywhere(movelogdir):
        delist = []

        mcache = name_movelog(movelogdir)
        try:
            tj = dataingest(mcache, jsonflag=True)
            delist = [item["dataid"] for item in tj["movelist"]]
        except:
            for dataid in allIDs:
                movelog = name_movelog(movelogdir, dataid)
                if exists_anywhere(movelog):
                    delist.append(dataid)

        allIDs = [id for id in allIDs if id not in delist]

    return allIDs


def getmeta(dataid, sinfo, allIDs_meta):
    if dataid in allIDs_meta:
        catmeta = allIDs_meta[dataid]
    else:
        # try HAPI
        try:
            hapiurl = "https://cdaweb.gsfc.nasa.gov/hapi/info?id=" + dataid
            catmeta = hapi_info_to_catdata(dataid, hapiurl)
        except:
            catmeta = blank_catalog(dataid)

    catmeta["index"] = None
    catmeta["filetype"] = None
    catmeta["indextype"] = "csv"  # hard-coded, for now

    return catmeta


def name_movelog(movelogdir, dataid="mastercache"):
    # default if no ID is the global aggregate movelog
    return f"{movelogdir}movelog_{dataid}.json"


def move_to_arch(fname, basedir):
    """Moves file to an 'arch' subdir below its current location
    fname is {basedir}stuff and we want {basedir}arch/stuff
    """
    altfname = re.sub(basedir, basedir + "arch/", fname)
    if fname.startswith("s3://"):
        mybucket, fnamekey = s3url_to_bucketkey(fname)
        mybucket, altfnamekey = s3url_to_bucketkey(altfname)
        s3_res = boto3.resource("s3")
        copy_source = {"Bucket": mybucket, "Key": fnamekey}
        s3_res.meta.client.copy(copy_source, mybucket, altfnamekey)
        s3_res.meta.client.delete_object(Bucket=mybucket, Key=fnamekey)
    else:
        os.makedirs(basedir + "arch", exist_ok=True)
        os.rename(fname, altfname)


def mastermovelog(movelogdir, allIDs):
    """
    Assembles all the individual movelogs into a single file.
    """

    mcache = name_movelog(movelogdir)

    try:
        tj = dataingest(mcache, jsonflag=True)
    except:
        pass

    flist = [name_movelog(movelogdir, id) for id in allIDs]
    for fname in flist:
        if exists_anywhere(fname):
            tempdata = dataingest(fname, jsonflag=True)
            try:
                tj["movelist"].extend(tempdata["movelist"])
            except:
                tj = tempdata
    try:
        tj = uniquejson(tj, "movelist", "dataid")
        datadump(mcache, tj, jsonflag=True)
        for fname in flist:
            if exists_anywhere(fname):
                move_to_arch(fname, movelogdir)
    except:
        logme("Unable to collate movelogs to ", mcache, "error")


def ready_migrate(dataid, sinfo, startDate, stopDate, catmeta={}):
    """
    once an item is properly staged, this lists it in a file
    so the subsequent migration to the s3 destination knows what to
    process.
    Format of each entry is:
    dataid, s3destination, s3staging, s3subdir, registrloc, catalog_stub
    where
    s3destination is where the files will ultimately reside
    s3staging+s3subdir is where they currently are
    registryloc is location of staging <id>_YYYY.csv files
    catalog_stub is the part to amalgamate into the final catalog.json

    migration is simply
    'cp -r <s3staging>/<s3subdir> <s3destination>/<s3subdir>'
    then update <s3destination>/catalog.json with <catalog_stub>

    """

    create_catalog_stub(dataid, sinfo, catmeta, startDate, stopDate)

    # elegant way to get the first subdir where the actual data exists
    s3subdir = sinfo["registryloc"].split(sinfo["s3staging"])[1].split("/")[0] + "/"

    entry = {
        "dataid": dataid,
        "s3destination": sinfo["s3destination"],
        "s3staging": sinfo["s3staging"],
        "s3subdir": s3subdir,
        "registryloc": sinfo["registryloc"],
        "catalog_stub": sinfo["registryloc"] + "catalog_stub.json",
    }

    movelog = name_movelog(sinfo["movelogdir"], dataid)
    if exists_anywhere(movelog):
        logme("Updating movelog ", movelog + " with " + dataid, "status")
        movelist = dataingest(movelog, jsonflag=True)
    else:
        logme("Creating movelog ", movelog + " with " + dataid, "status")
        movelist = {"movelist": []}

    movelist["movelist"].append(entry)
    movelist = uniquejson(movelist, "movelist", "dataid")
    datadump(movelog, movelist, jsonflag=True)


def migrate_staging_to_s3():
    """
    The later actual migration will involve
     a) move files to appropriate s3destination
     b) take new fileRegistry and add to canonical fileRegistry
     c) update db:catalog.json with any change in stop, modification, start
     d) optionally, S3 Inventory or other check that files were copied over successfully
     e) write destination catalog.json from DB:catalog.json
     f) clean out staging area once safely done
     g) delete and deregister obsolete files from 'deleteme' list
    """
    pass
