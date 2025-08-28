""" dumps latest SPDF filelist to a DB
database design is filename, full_name_with_path, filesize, status

We read the latest cdaweb filelist.gz to update the database.
Items in the database with status=0 are in ODR and indexed.
Items with status=1 need to be copied over (via this script, usually)

It uses a flat-file database (which cannot be located on S3!)
and saves catalog updates to a CSV file for later processing by
the separate catalog updater.

Changed base index locations-- originally it was by the data, e.g.
   spdf/cdaweb/data/mms/mms1/feeps/srvy/l2/YYYY/*.cdf indices at
   spdf/cdaweb/data/mms/mms1/feeps/srvy/l2/
Now shifted to be as top level as feasible and in an indices subdir, e.g.
   spdf/cdaweb/data/mms/mms1/feeps/srvy/l2/YYYY/*.cdf indices at
   spdf/cdaweb/data/mms/indices/


(We tested duckdb vs sqlite3 and sqlite3 perfroms 25%-100% faster.
Note sqlite3 TINYINT and TINYTEXT, duckdb requires SMALLINT and VARCHAR
instead, which sqlite3 also supports. Neither changes performance.)

It requires the ability to extract both the DATAID and the STARTTIME from
the FILENAME of each item, via an appropriate regex describing the
filename format.  For each 'dataid' there must be a 'base' and a 'pattern'.
e.g.
  base = "pub/data/ace/orbit/level_2_cdaweb/or_scc"
  pattern = "ac_or_ssc_%Y%m%d_%Q.cdf"
The system determines which dataid applies by (1) matching filenames that
startwith('base') versus the item's full filename,
then (2) extracts the appropriate regex to use later.
So in essence it uses filepaths to back-lookup 'dataid', then applies
that dataid's regex.
It is assuming that each 'dataid' has a unique path.  Data full filenames
tend to follow patterns of 'mission/moremissiondetails/[dataid]_[time]_other.ending, and specifically CDAWeb has in their XML definitions the
'URL' element.
Future work could include a 'filename->dataid' regex followed by the
necessary 'dataid -> timeformat regex', but for now this works for
known cases.

The mapping file for 'base' to dataid and timestamp 'pattern' from dataid
can be either as a set of CSV files of format 'dataid, base, pattern',
or the CDAWEB all.xml file, or from an existing catalog.json that includes
the optional 'filenaming' field holding the regex.

Really good would be a single regex of '.*%Q.*%timestuff.*' where %Q is the dataid and %timestuff the time part.  Basically splitting the startswith() and regex into one expression.  That would require some re-parsing of the
CDAWeb items so I can save for later.


It allows for copying to a staging directory while indexing for the
actual final destination directory.  if staging_prefix=None, they two
are the same.

    defaults = {'db_name' : 'spdf_filelist.db',
                'catalog_stub' : 'catalog_updates.csv',
                'transfer_cap_gb' : 2000,
                'strip_me' : 'pub/data/',
                'source_prefix' : 'https://spdf.gsfc.nasa.gov/pub/data/',
                'staging_prefix': 's3://helio-data-staging/spdf/cdaweb/data/',
                'dest_prefix' : 's3://gov-nasa-hdrl-data1/spdf/cdaweb/data/',
                'xml_path' : '.',
                'filelist' : 'filelist.gz',
                'force_uppercase':True}


Sample line from CDAWeb:
2017-08-29T05:06:00.2723147540 GMT      89536 pub/data/munin/cdfs/m1/oa/2001/munin_m1_oa_20010104193411_v01.cdf

For the scope of this problem, the CDAWeb 'filelist.gz' is around 1GB
compressed and 10GB uncompressed. It's in alphabetical order and includes
many files besides CDF and NetCDF data.

Reading 'filelist.gz' and generating our by-dataID indices takes ~2 minutes.

CHANGE:
we do the CDAWeb bit of

1) get the newest index
 wget -O - "https://spdf.gsfc.nasa.gov/pub/catalogs/filelist.gz" | gunzip | egrep "\.cdf|\.nc" | sort -k4 >   spdf_curr
2) generate the diff
 diff spdf_prev spdf_curr >   spdf_diff
 egrep "^< " spdf_diff >   spdf_deleted_files
 egrep "^> " spdf_diff >   spdf_new_files
3) load spdf_new_files and, by ID, do web copy to staging:
  cat spdf_new_files|awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"


parse the 'filelist.gz' into an individual manifest for each dataID (for missions other than MMS, the mission is the dataID; for MMS each spacecraft +
instrument is a dataID).

We then go through each dataID to compare against our current holdings
as indexed by 'cloudcatalog', and from that generate the list of files
to fetch-then-index (and also a list of 'files we have that are no longer
in CDAWeb, to schedule for deletion).

We do the web copy to S3 and also generate an updated index for that dataID.

TBD: update the global 'catalog.json' with the last modified time
for that dataID.

This allows for interrupted or incremental updates at any time.  We
commit per dataID.  So, if at any point the operation times out or
fails or reaches the data transfer cap CDAWeb prefers, we can restart
and it will resume from the last dataID processed.  It will
inefficiently re-process the full set of intermediate indices, but as
we've registered the prior dataIDs they won't be re-fetched of data.

Later we can deal with 'spdf_deleted_files' aka files we delisted but
have not yet deleted.

Note cdaweb filelist.gz will have duplicates with the same filename, e.g.
"i1_av_ott_1983351130734_v01.cdf" is in both /isis/* and /cdf_test_files/*

We force 'fullfile' + 'filesize' to be unique, to avoid adding dups

Also does compare of 'filelist' to 'cloudcatalog' holdings.

Processing cdaweb is cludgy because it has to be
1) push all existing contents to status=0
2) add all new as either status=1 or, if using date filter, status=3 iff
       the filelist timestamp is later than the last run
3) post-process:
   any status=0 that do not exist as status=1 go to status=-1/delete me
   remove any status=1 if they exist as status=0 (already exists)
   any status=3 that do not exist as status=0 go to status=1
   also remove any status=0 if they exist as status=3 (identical but later ver)
       and change status=3 to status=1 (fetch, it will overwrite)
4) Now in theory we copy over the status = 1 and we are updated


Uses status codes, where
(no entry = file does not exist in system anywhere)
status = -2 deleted (kept for record purposes)
status = -1 exists but schedule for deletion
status = 0 already exists (results from prior runs)
status = 1 new file/fetch

status = 2 (temporary category for new file but needs to filter first vs 1)
status = 3 (temporary category for identical file with later timestamp found)

edge case:
if status = 1 but later makes it for deletion, then it should go to 1 not 0
if un-deleted?

need category for 'new file/fetch got moved to delete before being brought
over, now need to be moved back to status=1 because a later fetch asks for it.'
This makes it truly re-entrant i.e. can be run regardless of whether copies
are carried out or not.

Logic is:
Read in new, status of priors is set to 0 and new files to 1
Only adds if fullname+filesize not already in DB

Later processing:
if fullnames match for status=0 & 1 but filesize differs,
  set the status=0 to status=-1/delete to keep the status=1

Note is very particular on source and destinations. Generally they should
be directories that end in a '/', such that appending the data filename will
be a proper location.  We do not enforce the ending slash, since some users
may want to make test appending.  Be wary.

Profiling:
 * it takes around 1 minute to re-order an 8Mil CSV file before processing
 * each 1M files takes around 2.1 minutes to ingest/parse (200K = 18 sec)


"""
global_database = "sqlite3"
# global_database = 'duckdb'
if global_database == "sqlite3":
    import sqlite3
    from collections import defaultdict
elif global_database == "duckdb":
    import duckdb
else:
    print("Error, code should define a viable database")
    exit()
from dateutil import parser
from datetime import datetime
import gzip
import json
import os
import re
import shutil
import sys
import time
import requests
import smart_open
import cdaweb_xml_checker as cxc

global_badregexes = {}

def query_file_only_cdaweb(db_Cursor):
    query = "SELECT t1.filename from entries t1 LEFT JOIN cloudcatalog t2"
    query += " ON t1.filekey = t2.filekey where t2.filekey IS NULL;"
    db_Cursor.execute(query)
    new_filenames = [row[0] for row in db_Cursor.fetchall()]
    return new_filenames


def query_file_only_cloudcatalog(db_Cursor):
    query = "SELECT t1.filename from cloudcatalog t1 LEFT JOIN entries t2"
    query += " ON t1.filekey = t2.filekey where t2.filekey IS NULL;"
    db_Cursor.execute(query)
    deletable_filenames = [row[0] for row in db_Cursor.fetchall()]
    return deletable_filenames


def query_file_in_both(db_Cursor):
    query = "SELECT t1.filename from entries t1 INNER JOIN cloudcatalog t2"
    query += " ON t1.filename = t2.filename;"
    db_Cursor.execute(query)
    common_filenames = [row[0] for row in db_Cursor.fetchall()]
    return common_filenames


def connectDB(db_name):
    if global_database == "duckdb":
        db_Conn = duckdb.connect(db_name)
    else:
        db_Conn = sqlite3.connect(db_name)
    db_Cursor = db_Conn.cursor()
    return db_Conn, db_Cursor


def createDB_safe(db_name, debug=False):
    db_Conn, db_Cursor = connectDB(db_name)
    # only creates if it does not yet exist
    query = """
        CREATE TABLE IF NOT EXISTS entries
        (filename VARCHAR(255), fullname VARCHAR(255), filesize BIGINT, starttime VARCHAR(23), dataid VARCHAR(90), status TINYINT NOT NULL, queueset VARCHAR(255))
    """
    db_Cursor.execute(query)
    query = "CREATE INDEX IF NOT EXISTS idx_ff ON entries (fullname, filesize)"
    db_Cursor.execute(query)
    query = "CREATE UNIQUE INDEX IF NOT EXISTS idx_uni ON entries (fullname, filesize, status)"
    db_Cursor.execute(query)
    query = "CREATE INDEX IF NOT EXISTS idx_status ON entries (status)"
    db_Cursor.execute(query)
    query = "CREATE INDEX IF NOT EXISTS idx_queueset ON entries (queueset)"
    db_Cursor.execute(query)

    query = """
        CREATE TABLE IF NOT EXISTS refresh_indices
        (dataid VARCHAR(255),
        year SMALLINT,
        PRIMARY KEY (dataid, year))
    """
    db_Cursor.execute(query)

    query = """
        CREATE TABLE IF NOT EXISTS settings 
        ( id INTEGER PRIMARY KEY CHECK (id=1),
        lastdate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        transfer_cap_gb FLOAT,
        strip_me VARCHAR,
        source_prefix VARCHAR,
        staging_prefix VARCHAR,
        dest_prefix VARCHAR,
        xml_path VARCHAR,
        filelist VARCHAR,
        catalog_stub VARCHAR,
        db_name VARCHAR,
        force_uppercase BOOL)
    """
    db_Cursor.execute(query)
    db_Cursor.execute("SELECT COUNT(*) FROM settings;")
    if db_Cursor.fetchone()[0] == 0:
        db_Cursor.execute("INSERT INTO settings (id) VALUES (1);")
    db_Conn.commit()
    db_Conn.close()


def fetchDB_defaults(db_name):
    db_Conn, db_Cursor = connectDB(db_name)
    query = "SELECT * from settings where id=1"
    db_Cursor.execute(query)
    row = db_Cursor.fetchone()
    columns = [desc[0] for desc in db_Cursor.description]
    db_Conn.close()
    return dict(zip(columns, row))


def updateDB_all_defaults(defaults):
    for key in defaults:
        updateDB_defaults(defaults["db_name"], key, defaults[key])


def updateDB_defaults(db_name, key, value):
    db_Conn, db_Cursor = connectDB(db_name)
    if key == "transfer_cap_gb" or key == "force_uppercase":
        query = f"UPDATE settings SET {key} = {value} WHERE id=1"
    else:
        query = f"UPDATE settings SET {key} = '{value}' WHERE id=1"
    db_Cursor.execute(query)
    db_Conn.commit()
    db_Conn.close()


def fetchDB_time(db_Cursor):
    db_Cursor.execute("SELECT lastdate FROM settings WHERE id=1;")
    try:
        ttime = datetime.strptime(db_Cursor.fetchone()[0], "%Y-%m-%d %H:%M:%S")
    except:
        ttime = None
    return ttime


def updateDB_time(db_Conn, db_Cursor, now=None, db_name=None):
    if db_Conn == None:
        if db_name:
            db_Conn, db_Cursor = connectDB(db_name)
        else:
            return
    if now == None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query = f"UPDATE settings SET lastdate = '{now}' WHERE id=1;"
    db_Cursor.execute(query)
    db_Conn.commit()


def insertDB_data(db_Cursor, filename, fullname, filesize, starttime, dataid, queueset, status=2):
    query = f"""
        INSERT OR IGNORE INTO entries
        (filename, fullname, filesize, starttime, dataid, status, queueset)
        VALUES ('{filename}', '{fullname}', {filesize}, '{starttime}', '{dataid}', {status}, '{queueset}')
    """
    db_Cursor.execute(query)


def db_missions_to_queue(db_Conn=None, db_Cursor=None, status=-1, db_file=None):
    """updates queue of which indices to regenerate to add items or
    to delist any scheduled deletes.  Default behavior is to add the
    mission ids of indices where files are scheduled for deletion aka
    to delist items.
    When delisting, even though files may still exist in the
    system, they will not be in the index when indices are next remade.
    You can also call this to re-create the index of all the files
    actually on disk by giving it 'status=0' (but the subsequent actual
    index generation may take a while as it will have to regenerate all
    indices).
    """
    if db_Conn == None:
        if db_file == None:
            return ""
        closeme = True
        db_Conn, db_Cursor = connectDB(db_file)
    else:
        closeme = False
    query = f"SELECT DISTINCT queueset from entries WHERE status={status}"
    db_Cursor.execute(query)
    queuesets = db_Cursor.fetchall()
    if queuesets:
        for qs in queuesets:
            dataid, year = qs[0].split(":")
            try:
                query = "INSERT OR IGNORE INTO refresh_indices (dataid, year) "
                query += f"VALUES ('{dataid}',{int(year)})"
                db_Cursor.execute(query)
            except:
                print(f"Error inserting {dataid}, {year} into refresh_indices")
        db_Conn.commit()
    if closeme:
        db_Conn.close()


def db_reconcile_updates(db_Conn, db_Cursor, debug=False):
    """Coming in we have 3 (overwrite any existing), 2 (new file, valid),
    1 (was scheduled for copy, but may no longer be needed so check)
    0 EXISTING INDEXED HOLDINGS
    -1 scheduled for deleting and de-indexing
    """
    if debug:
        print("\tDB reconcile beginning, may take a while...")
    times = []
    now = time.time()
    # clean slate: delete all 1s cuz if not tranferred yet they are irrelevant
    query = "DELETE FROM entries WHERE status = 1;"
    db_Cursor.execute(query)

    # if 3/overwrite and 0/exists, nuke 0;
    # if 3/overwrite and -1/queue for delete, remove the -1 for logic reasons
    times.append(f"%.2f" % (time.time() - now))
    query = """
        DELETE FROM entries
        WHERE status IN (-1, 0)
        AND EXISTS (
            SELECT 1 FROM entries f2
            WHERE f2.fullname = entries.fullname
            AND f2.filesize = entries.filesize
            AND f2.status = 3
        );
    """
    db_Cursor.execute(query)

    # purge: if 0 and does not exist as 3/2, is no longer needed so set to -1
    times.append(f"%.2f" % (time.time() - now))
    query = """
        UPDATE entries
        SET status = -1
        WHERE status = 0
        AND NOT EXISTS (
            SELECT 1 FROM entries f2
            WHERE f2.fullname = entries.fullname
            AND f2.filesize = entries.filesize
            AND f2.status IN (2, 3)
        );
    """
    db_Cursor.execute(query)

    # create queue: set all 3 and 2 to 1
    times.append(f"%.2f" % (time.time() - now))
    query = "UPDATE entries set status = 1 WHERE status IN (2, 3);"
    db_Cursor.execute(query)

    # de-delist: goal = if 1 & -1, nuke -1 and set 1 to 0
    # easy logic: if 1 & -1, set -1 to 0 so next step resolves the 2nd part
    times.append(f"%.2f" % (time.time() - now))
    query = """
        UPDATE entries
        SET status = 0
        WHERE status = -1
        AND EXISTS (
            SELECT 1 FROM entries f2
            WHERE f2.fullname = entries.fullname
            AND f2.filesize = entries.filesize
            AND f2.status = 1
        );
    """
    query = """
        UPDATE entries SET status = 0 WHERE status = -1
        AND fullname in (SELECT fullname FROM entries WHERE status = 1)
    """
    db_Cursor.execute(query)

    # already exists: if 1 and 0, nuke 1
    times.append(f"%.2f" % (time.time() - now))
    query = """
        DELETE FROM entries
        WHERE status = 1
        AND EXISTS (
            SELECT 1 FROM entries f2
            WHERE f2.fullname = entries.fullname
            AND f2.filesize = entries.filesize
            AND f2.status = 0
        );
    """
    db_Cursor.execute(query)

    if debug:
        print("\t\t(seconds per DB action:", times, ")")
    if debug:
        print(
            f"\t... DB reconcile complete, took %.2f minutes"
            % ((time.time() - now) / 60)
        )
    db_Conn.commit()





def parse_line(
    line, regex_base, regex_pattern, lastdate=None, strip_me=None, csvflag=False
):
    # takes a valid line from filelist.gz and extracts relevant info
    # format can have arbitrary initial elements but must END with
    # 'filesize, fullname'
    valid = True
    if csvflag:
        lineset = line.rstrip().split(",")
    else:
        # CDAWeb space-delinated format
        lineset = line.rstrip().split()
    fullname = lineset[-1]
    if strip_me != None and fullname.startswith(strip_me):
        #fullname = re.sub(f"^{strip_me}", "", fullname)
        fullname = fullname[len(strip_me):]
    filename = os.path.basename(fullname)
    filesize = lineset[-2]
    dataid, indexbase, x_regex = cxc.extract_regex(regex_base, regex_pattern, fullname)
    if dataid == None:
        valid = False
        dataid = "None"
        starttime = "None"
        datayear = "0000"
    else:
        starttime = cxc.extract_datetime(filename, x_regex, form="str")
        if starttime == None:
            global_badregexes[x_regex] = filename
            valid = False
            # edge case e.g. regex has %Q but not %Y
            # makes assumptions, but works so very often (note we do set valid=False)
            matches = re.search(r"\d{4}", filename)
            datayear = matches.group(0) if matches else 'static'
        else:
            datayear = starttime[:4]
    queueset = dataid + ":" + datayear

    #print("fake",dataid,indexbase,filename,x_regex,lastdate,lineset[0])

    if len(lineset) > 3 and lastdate != None and str2datetime(lineset[0]) > lastdate:
        status = 3
    else:
        status = 2
    return fullname, filename, filesize, starttime, dataid, queueset, status, valid


def ingest_parse_filelist(
        db_Conn,
        db_Cursor,
        fname="filelist.gz",
        lastdate=None,
        strip_me=None,
        xml_path=".",
        debug=False,
        limit=None
):
    regex_base, regex_pattern = cxc.load_fromxml(xml_path, strip_me=strip_me)
    i = 0
    newfiles = 0
    now = time.time()
    if fname.endswith(".gz"):
        fin = gzip.open(fname, "rt")
    else:
        fin = open(fname)
    if fname.endswith(".csv"):
        csvflag = True
    else:
        csvflag = False
    errors = []
    t_db = 0
    t_parse = 0
    for line in fin:
        if line.endswith(".cdf\n") or line.endswith(".nc\n"):
            line = line.rstrip()
            t_t = time.time()
            fullname, filename, filesize, starttime, dataid, queueset, status, valid = parse_line(
                line, regex_base, regex_pattern, lastdate, strip_me, csvflag
            )
            t_parse += time.time() - t_t
            if valid:
                t_t = time.time()
                insertDB_data(db_Cursor, filename, fullname, filesize, starttime, dataid, queueset, status=status)
                t_db += time.time() - t_t
            else:
                errors.append(
                    f"{line},{starttime},{dataid},{queueset},{status},{filename},{filesize},{fullname}\n"
                )
            i += 1
            if debug:
                if i % 500000 == 1:
                    print(
                        "... %s lines, %.2f minutes, %d errors (%.1f sec regex, %.1f sec DB)"
                        % ('{:,}'.format(i), ((time.time() - now) / 60), len(errors), t_parse, t_db)
                    )
            if limit != None and i > limit: break # for testing
    db_Conn.commit()
    if debug:
        print(
            "\t%s files commited, total time %.2f min" % ('{:,}'.format(i), (time.time() - now) / 60)
        )
    if len(errors) > 0:
        fname = "errors_badlines.log"
        print(
            f"Warning, storing {len(errors):,} parsing errors/lines ignored in {fname}"
        )
        with open(fname, "a") as ferror:
            ferror.writelines(errors)

    if len(global_badregexes) > 0:
        fname = "errors_badregex.log"
        print(
            f"Warning, storing {len(global_badregexes)} regex pattern errors ignored in {fname}"
        )
        with open(fname, "a") as ferror:
            ferror.write(json.dumps(global_badregexes, indent=4))


def db_unsafe_mark_all_as_copied(db_name=None):
    """only used for bulk external transfers, otherwise the code will
    update the DB while doing copies.
    So only run after actually copying the files over by hand
    """
    if db_name == None:
        defaults = default_defaults()
        db_name = defaults["db_name"]
    db_Conn, db_Cursor = connectDB(db_name)
    query = f"UPDATE entries SET status=0 WHERE status=1"
    db_Cursor.execute(query)
    db_Conn.commit()
    db_Conn.close()


def db_unsafe_mark_all_as_deleted(db_name):
    """only used for bulk external transfers, otherwise the code will
    update the DB while doing copies.
    So only run if you deleted all the files by hand
    """
    db_Conn, db_Cursor = connectDB(db_name)
    query = "UPDATE entries SET status = -2 where status = -1"
    db_Cursor.execute(query)
    db_Conn.commit()
    db_Conn.close()


def print_db_statuses(db_Cursor=None, extrastr=None, db_file=None, noisy=False):
    # Two modes: active cursor (default), or open/close DB file for standalone
    if db_Cursor == None:
        if db_file == None:
            return ""
        closeme = True
        db_Conn, db_Cursor = connectDB(db_file)
    else:
        closeme = False

    if noisy:
        if extrastr != None:
            print(extrastr)
    query = "SELECT status, COUNT(*), SUM(filesize) FROM entries GROUP BY status;"
    db_Cursor.execute(query)
    status_counts = db_Cursor.fetchall()
    retval = ""
    for status, count, mysum in status_counts:
        mysum /= 1000000000
        if noisy:
            print(f"\tFiles of status {status}: {count:,} entries, {mysum} GB")
        retval += f" {count:,} status={status} gb={mysum},"

    if closeme:
        db_Conn.close()

    return retval


def validation_show_entries(db_name, limit=10, status=None):
    # prints a random sample of entries, to spot check by eye
    print(f"\tSpot check validation, printing {limit} random entries")
    db_Conn, db_Cursor = connectDB(db_name)
    query = "SELECT * from entries"
    if status != None:
        query += f" where status={status}"
    query += f" LIMIT {limit}"
    db_Cursor.execute(query)
    rows = db_Cursor.fetchall()
    for row in rows:
        print(row)
    db_Conn.close()
    print("... done spot check.")


def validation_show_dups(db_name, limit=None, noisy=False):
    db_Conn, db_Cursor = connectDB(db_name)
    """
    # duckdb requires a different query, but we abandoned duckdb for speed
    query = "SELECT fullname, filesize, ANY_VALUE(status) as status, COUNT(*)
       as duplicate_count from entries
       GROUP BY fullname, filesize HAVING COUNT(*) > 1
    "
    """
    query = """
       SELECT fullname, filesize, status, COUNT(*) as duplicate_count from entries
       GROUP BY fullname, filesize HAVING COUNT(*) > 1
    """
    db_Cursor.execute(query)
    dups = db_Cursor.fetchall()
    if dups:
        if limit == None:
            limit = len(dups)
        print("Warning, some duplicate files, # of dups:", len(dups))
        for i in range(limit):
            if noisy:
                print(entry[i])
    else:
        if noisy:
            print(
                "Check passed, no duplicates (i.e. same fullname+filesize but differing by processing status"
            )
    db_Conn.close()


def ui_show_tables(db_name):
    db_Conn, db_Cursor = connectDB(db_name)
    query = "SELECT name from sqlite_master WHERE type='table' ORDER by name;"
    db_Cursor.execute(query)
    tables = db_Cursor.fetchall()
    print([table[0] for table in tables])
    db_Conn.close()


def ui_show_refresh_indices(db_name):
    db_Conn, db_Cursor = connectDB(db_name)
    query = "select * from refresh_indices ORDER BY dataid"
    db_Cursor.execute(query)
    allrows = db_Cursor.fetchall()
    for row in allrows:
        print(row, end=", ")
    print("")
    db_Conn.close()


def db_clear_refresh_indices(db_name):
    """Be careful, only do this after actually generating indices!
    If you screw up, you can recreate the indices from scratch
    by re-listing all items already on disk,
    but beware the actual generation of indices later will take a while,
    because they will be recreating potentially the entire index set.
    But it will be accurate (listing all valid files on disk)
        db_missions_to_queue(status = 0, db_file=db_name)
    Note this means, if you do not know the current state of the indices,
    you can also do the pair of:
      db_clear_refresh_indices(db_name)
      db_missions_to_queue(status = 0, db_file=db_name)
    to restore the baseline set.
    """
    db_Conn, db_Cursor = connectDB(db_name)
    query = "delete from refresh_indices"
    db_Cursor.execute(query)
    db_Conn.commit()
    db_Conn.close()


def ui_prompt_for_defaults(db_name):
    defaults = fetchDB_defaults(db_name)
    for key in defaults.key():
        newval = input(
            f"Input value for {key} (currently {defaults[key]}) or hit return to keep: "
        )
        if len(newval) > 0:
            updateDB_defaults(db_name, key, newval)


def str2datetime(cdastr):
    try:
        dt = datetime.strptime(cdastr[:16], "%Y-%m-%dT%H:%M")
    except:
        dt = datetime.strptime(cdastr[:16], "%Y-%m-%d %H:%M")
    return dt


def datetime2str(cdadt):
    dt = parser.isoparse(cdadt).strftime("%Y-%m-%dT%H:%M")
    return dt

def dump_to_queue(fout, source, dest):
    if source.startswith("http"):
        cmd = 'curl -L ' + source + '| aws s3 cp - ' + dest + '\n'
    elif source.startswith("s3://"):
        cmd = 'aws s3 cp ' + source + ' ' + dest + '\n'
    else:
        # assume local copy
        cmd = 'cp ' + source + ' ' + dest + '\n'
    fout.write(cmd)
    return True

def smart_cp(source, dest, debug=False):
    # wrapper for file copies S3-local, local-S3, S3-S3, or local-local
    try:
        if debug:
            print(f"\tspot check: sample cp is {source} {dest}")
        if not dest.startswith("s3://"):
            dest_dirs = os.path.dirname(dest)
            if dest_dirs:
                os.makedirs(dest_dirs, exist_ok=True)
        with smart_open.open(source, "rb") as fin:
            with smart_open.open(dest, "wb") as fout:
                for line in fin:
                    fout.write(line)
        return True
    except Exception as e:
        if debug:
            print(f"Failed to copy {source} to {dest}, error {e}")
        return False


def transfer_over(db_name, checkpoint=1000, debug=False, limit=None, bulk=False):
    """This does the heavy lifting, the actual copying over of data
    from the DB-stored source to the destination.  It enforces transfer
    limits (if any) by stopping after the given TB are brought over.
    It does not do any filesize or checksum verification, but relies on
    boto3 to return a fail if a transfer did not work.
    It updates the database with a commit every (default) 1000 files,
    to balance DB performance while avoiding excess refetches for killed jobs.
    If limit=(int) it will only transfer a max of limit files, use for testing
    """
    now = time.time()
    if debug:
        print("\tStarting file transfers...")
    prefs = fetchDB_defaults(db_name)
    tcap = prefs["transfer_cap_gb"]
    db_Conn, db_Cursor = connectDB(db_name)
    commit_count, total_size, successes, fails = 0, 0, 0, 0
    flag_for_reindexing = []
    printone = debug

    query = "SELECT filename, fullname, filesize, queueset FROM entries WHERE status=1 ORDER BY filename ASC"
    if limit != None:
        query += f" LIMIT {limit}"
    db_Cursor.execute(query)
    allrows = db_Cursor.fetchall()

    if bulk:
        fout = open("queue.bat","a")
    
    for row in allrows:
        fullname, filesize, queueset = row[1], row[2], row[3]
        if queueset not in flag_for_reindexing:
            dataid, year = queueset.split(":")
            query = "INSERT OR IGNORE INTO refresh_indices (dataid, year) "
            try:
                iy=int(year)
            except:
                iy=0000
            #query += f"VALUES ('{dataid}',{int(year)})"
            query += f"VALUES ('{dataid}',{iy})"
            db_Cursor.execute(query)
            db_Conn.commit()
            flag_for_reindexing.append(queueset)
            commit_count = 0  # reset counter to avoid slow excess commits
        source = prefs["source_prefix"] + fullname
        dest = prefs["staging_prefix"]
        if dest == None:
            dest = prefs["dest_prefix"]
        dest += fullname
        if bulk:
            retstat = dump_to_queue(fout, source, dest)
        else:
            retstat = smart_cp(source, dest, debug=printone)
        printone = False  # only print the first copy, as a sanity check
        if retstat:
            query = f"UPDATE entries SET status=0 WHERE fullname='{fullname}'"
            db_Cursor.execute(query)
            successes += 1
            total_size += filesize / 1000000000
            commit_count += 1
            if commit_count % checkpoint == 0:
                db_Conn.commit()
            if tcap != None and total_size > tcap:
                if debug:
                    print(f"Reached bandwidth cap of {tcap}TB, ending cleanly.")
                break
        else:
            fails += 1
        if debug:
            if (successes + fails) % checkpoint == 0:
                print(
                    "\t\tcopy so far: %d successes, %d fails, %.2f minutes"
                    % (successes, fails, ((time.time() - now) / 60))
                )
                
    db_Conn.commit()
    db_Conn.close()
    if bulk:
        fout.close()
    if fails > 0:
        print("***Warning*** Not all files got copied over.")
    if debug or fails > 0:
        print(
            "Status: %d files copied, %d failed copies, %d unique dataset/years, took %.2f minutes"
            % (successes, fails, len(flag_for_reindexing), (time.time() - now) / 60)
        )
        if bulk:
            qname = "queue_" + str(int(successes/1000)) + "K_" + str(len(flag_for_reindexing)) + "sets.bat"
            version_file(qname)
            os.rename("queue.bat",qname)


def ingest_and_reconcile(defaults, debug=False, limit=None):
    db_Conn, db_Cursor = connectDB(defaults["db_name"])
    if debug:
        print("\tBeginning on ", defaults["filelist"])
    lastdate = fetchDB_time(db_Cursor)
    if debug:
        print("\t\tLast update was at ", lastdate)
    if debug:
        ignore = print_db_statuses(db_Cursor, "\tLoaded DB", noisy=True)
    ingest_parse_filelist(
        db_Conn,
        db_Cursor,
        defaults["filelist"],
        lastdate,
        strip_me=defaults["strip_me"],
        xml_path=defaults["xml_path"],
        debug=debug,
        limit=limit
    )
    if debug:
        ignore = print_db_statuses(
            db_Cursor, f"\t\tDone read of {defaults['filelist']}", noisy=True
        )
    db_reconcile_updates(db_Conn, db_Cursor, debug=debug)
    retvals = print_db_statuses(
        db_Cursor, "\tDone reconciling updates, final count:", noisy=debug
    )
    db_missions_to_queue(db_Conn, db_Cursor, status=-1)
    db_Conn.close()
    return retvals


def cdaweb_date_patterns():
    # these patterns work 99.95% (all but 16K files out of 30,500,000 files)
    # the fails are legit bad, e.g. '19780732' or '00000000' or '19990200'
    pattern2 = re.compile(
        r"^(.*?)_[d]?(\d{4})(\d{2})(\d{2})[Tt]?(\d{2})?(\d{2})?(\d{2})?(\d{3})?[_-].*"
    )
    pattern4 = re.compile(
        r"^[d]?(\d{4})(\d{2})(\d{2})[Tt]?(\d{2})?(\d{2})?(\d{2})?[_-].*"
    )
    ypattern1 = re.compile(r"^(.*?)[_-](\d{4})[_-](\d{3})_(\d{2})?[_]?(\d{2})?.*")
    ypattern2 = re.compile(r"^(.*?)_(\d{4})(\d{3})[Tt](\d{2})(\d{2})(\d{2}).*")
    ypattern4 = re.compile(r"^(.*?)_(\d{4})(\d{3})[Tt]?(\d{2})?(\d{2})?(\d{2})?[_-].*")
    ypattern5 = re.compile(r"^(\d{4})(\d{3})[Tt]?(\d{2})?(\d{2})?(\d{2})?[_-].*")
    patternset = [pattern2, pattern4, ypattern1, ypattern2, ypattern4, ypattern5]
    return patternset


def debug_show_indices(db_name):
    # little debug function to print which indices need to be regenerated
    db_Conn, db_Cursor = connectDB(db_name)
    query = "SELECT dataid, year FROM refresh_indices ORDER by dataid ASC"
    db_Cursor.execute(query)
    rowset = db_Cursor.fetchall()
    print(rowset)
    db_Conn.close()


def generate_catalog(defaults, debug=False, allindices=False, limit=None,
                     track_indices = None):
    """Regenerates entire catalog.json stub for all CDAWebs that exist"""
    regex_base, regex_pattern = cxc.load_fromxml(
        defaults["xml_path"], strip_me=defaults["strip_me"]
    )
    db_Conn, db_Cursor = connectDB(defaults["db_name"])
    modstamp = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%dT%H:%M:%SZ")
    now = time.time()
    if allindices:
        query = f"SELECT DISTINCT queueset from entries WHERE status=0"
        db_Cursor.execute(query)
        queuesets = db_Cursor.fetchall()
        if queuesets:
            queuesets = [qs[0] for qs in queuesets]
    elif track_indices != None:
        queuesets = [qs[0] + ":" + str(qs[1]) for qs in track_indices]
    else:
        # only process subset of indices that have new files as per db tracking
        query = "SELECT dataid, year FROM refresh_indices ORDER by dataid ASC"
        if limit != None:
            query += f" LIMIT {limit}"
        db_Cursor.execute(query)
        queuesets = db_Cursor.fetchall()
        if queuesets:
            queuesets = [qs[0] + ":" + str(qs[1]) for qs in queuesets]
    # need first and last filename for a given dataid, so we can get
    # first and last times.  Also need filepath for index.
    starts, stops, bases = {}, {}, {}
    if debug: print(f"\tabout to process %s entries into %s" % (len(queuesets),defaults["catalog_stub"] ))
    if queuesets:
        for qs in queuesets:
            dataid, year = qs.split(":")
            query = f"""
            SELECT fullname FROM (
            SELECT fullname FROM entries WHERE queueset='{qs}' and status=0 ORDER BY fullname ASC LIMIT 1
            )
            UNION ALL
            SELECT fullname FROM (
            SELECT fullname FROM entries WHERE queueset='{qs}' and status=0 ORDER BY fullname DESC LIMIT 1
            );
            """
            db_Cursor.execute(query)
            result = [row[0] for row in db_Cursor.fetchall()]
            if result:
                # because sometimes 'deletes' are in refresh queue but do not need an index
                dataid, indexbase, x_regex = cxc.extract_regex(
                    regex_base, regex_pattern, result[0]
                )
                starttime = cxc.extract_datetime(result[0], x_regex, form="str")
                stoptime = cxc.extract_datetime(result[1], x_regex, form="str")
                bases[dataid] = indexbase
                if dataid not in starts or starttime > starts[dataid]:
                    starts[dataid] = starttime
                if dataid not in stops or stoptime > stops[dataid]:
                    stops[dataid] = stoptime
    # now output the dataids
    catfile = defaults["catalog_stub"]
    if len(bases) > 0:
        version_file(catfile)
        if not catfile.startswith("s3://"):
            catdirs = os.path.dirname(catfile)
            if catdirs != "":
                os.makedirs(catdirs, exist_ok=True)
        entries = []
        for dataid in sorted(bases.keys()):
            index = gen_index_name(defaults, bases[dataid])
            start, stop = starts[dataid], stops[dataid]
            csv = ",".join([dataid, start, stop, index, modstamp]) + '\n'
            entries.append(csv)
        with open(catfile, "a") as fout:
            fout.writelines(entries)
        if debug:
            print(
                f"\t... %s complete with %d updated indices (removed %d), took %.2f minutes"
                % (catfile, len(bases), len(queuesets)-len(bases), (time.time() - now) / 60)
            )
    else:
        print(
            f"\tWarning, %s empty, %d entries, took %.2f minutes"
            % (catfile, len(queuesets), (time.time() - now) / 60)
        )

def gen_index_name(defaults,indexbase,dataid=None,year=None,short=True):
    """ old was entirepathtodata/*.csv
        new 'short' is basepath+firstpartofname_aka_dataid/indices/*.csv
    e.g. was s3://heliodata/spdf/cdaweb/data/mms/mms1/feeps/srvy/l2/ion/*.csv
         new 'short' is s3://heliodata/spdf/cdaweb/data/mms/indices/*.csv
    """
    if dataid == None:
        if short:
            hometop = indexbase.split('/')[0] + '/indices' + '/'
            index = defaults["dest_prefix"] + hometop
        else:
            index = defaults["dest_prefix"] + indexbase + "/"
        return index
    if defaults["force_uppercase"]:
        dataid = dataid.upper()
    staging = defaults["staging_prefix"]
    if staging == None:
        staging = defaults["dest_prefix"]
    if short:
        hometop = indexbase.split('/')[0] + '/indices'
        indexname = f"{staging}{hometop}/{dataid}_{year}.csv"
    else:
        indexname = f"{staging}{indexbase}/{dataid}_{year}.csv"
    if not indexname.startswith("s3://"):
        #print(indexname,'fake','making it',os.path.dirname(indexname))
        #os.makedirs(staging + indexbase, exist_ok=True)
        os.makedirs(os.path.dirname(indexname), exist_ok=True)
    return indexname

# fake

def process_and_write_index(queueset, rows, regex_base, regex_pattern, defaults):
    dataid, year = queueset.split(":")
    fdata = []
    endtime = None
    priorindex = "ignore"
    for fullname, filename, filesize, starttime in rows:
        dataid, indexbase, x_regex = cxc.extract_regex(
            regex_base, regex_pattern, fullname
        )
        if starttime is None:
            if priorindex != indexbase:
                print("Error extracting time from", filename)
            priorindex = indexbase
            continue
        fullpath = defaults["dest_prefix"] + fullname
        if endtime is None:
            endtime = starttime
        fdata.append(f"{starttime},{endtime},{fullpath},{filesize}\n")
        endtime = starttime
    fdata.append("#start, stop, datakey, filesize\n")
    fdata.reverse()

    indexname = gen_index_name(defaults, indexbase, dataid, year)
    if not indexname.startswith("s3://"):
        os.makedirs(os.path.dirname(indexname), exist_ok=True)
    with smart_open.open(indexname, "w") as fout:
        fout.writelines(fdata)

def botched_generate_indices(defaults, debug=False, limit=None):
    conn = sqlite3.connect(defaults["db_name"])
    cursor = conn.cursor()

    regex_base, regex_pattern = cxc.load_fromxml(
        defaults["xml_path"], strip_me=defaults["strip_me"]
    )

    query = """
        SELECT queueset, fullname, filename, filesize, starttime
        FROM entries
        WHERE status=0
        ORDER BY queueset, filename DESC
    """
    if limit != None:
        query += f" LIMIT {limit}"
    cursor.execute(query)

    current_qs = None
    current_rows = []
    batch_size = 1000
    icount = 0
    now = time.time()
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for qs, fullname, filename, filesize, starttime in rows:
            if current_qs is None:
                current_qs = qs
            if qs != current_qs:
                process_and_write_index(current_qs, current_rows, regex_base, regex_pattern, defaults)
                current_qs = qs
                current_rows = []
                icount += 1
                if debug and icount % 100 == 0:
                    print(f"  creating {icount} indices in time {time.time()-now} s")
            current_rows.append((fullname, filename, filesize, starttime))

    # Final group flush
    if current_rows:
        process_and_write_index(current_qs, current_rows, regex_base, regex_pattern, defaults)

    conn.close()
    return 0, [] # fake, need realy errorcount, track_indices

#fake

def generate_indices(defaults, debug=False, limit=None):
    """Generates or re-generates the final cloudcatalog indices.
    CDAWeb forces all dataIDs to uppercase, so we added a flag for that.
    Earlier variant guessed at IDs and did reasonable date work.
    Now we use the all.xml file provided by CDAWeb to better match.
    """
    dest = defaults["dest_prefix"]
    now = time.time()

    regex_base, regex_pattern = cxc.load_fromxml(
        defaults["xml_path"], strip_me=defaults["strip_me"]
    )

    db_Conn, db_Cursor = connectDB(defaults["db_name"])
    query = "SELECT dataid, year FROM refresh_indices ORDER by dataid ASC"
    if limit != None:
        query += f" LIMIT {limit}"
    db_Cursor.execute(query)
    rowset = db_Cursor.fetchall()
    print(f"\t(Re)generating {len(rowset)} indices...")
    # walk through each mission/year combo to make a new index
    errorcount = 0
    errorlist = []
    icount = 0
    irows = 0
    track_indices = []
    priorindex = "ignore"
    for row in rowset:
        dataid, year = row[0], row[1]
        queueset = f"{dataid}:{year}"
        query = f"SELECT fullname, filename, filesize, starttime from entries WHERE status=0 AND queueset='{queueset}' ORDER by filename DESC"
        db_Cursor.execute(query)
        fdata = []
        endtime = None
        """
        Faster approach: read all into a numpy, re-order cols, dump as unit?
        """
        for item in db_Cursor.fetchall():
            fullname, filename, filesize, starttime = item[0], item[1], item[2], item[3]
            if endtime == None:
                # only need this once per queueset
                dataid, indexbase, x_regex = cxc.extract_regex(
                    regex_base, regex_pattern, fullname
                )
            if starttime == None:
                if priorindex != indexbase:
                    # only print one warning per set of dataid files
                    if debug:
                        print(
                            "Error extracting time from e.g.: ",
                            filename,
                            "with",
                            x_regex,
                        )
                priorindex = indexbase
                errorcount += 1
                errorlist.append(filename)
            fullname = dest + fullname  # expand out
            if endtime == None:
                endtime = starttime  # as of yet no solution for last file
            fdata.append(f"{starttime},{endtime},{fullname},{filesize}\n")
            endtime = starttime  # save for next entry
            irows += 1
        fdata.append("#start, stop, datakey, filesize\n")
        fdata.reverse()
        if len(fdata) > 1:
            track_indices.append( (dataid,year) )
            icount += 1
            indexname = gen_index_name(defaults,indexbase,dataid,year)
            if not indexname.startswith("s3://"):
                index_dirs = os.path.dirname(indexname)
                if index_dirs:
                    os.makedirs(index_dirs, exist_ok=True)
            with smart_open.open(indexname, "w") as fout:
                fout.writelines(fdata)
            if debug and icount % 100 == 0:
                print(f"    created {icount} of {len(rowset)} indices in {time.time()-now} seconds")
    db_Conn.close()
    if icount > 0:
        if debug:
            print(
                f"\t... success making %d indices totaling %d files, took %.2f minutes"
                % (icount, irows, (time.time() - now) / 60)
            )
    elif errorcount > 0:
        print(f"Error, {errorcount} files could not be indexed, please redo.")
    return errorcount, track_indices

def slower_generate_indices(defaults, debug=False, limit=None):
    """Generates or re-generates the final cloudcatalog indices.
    CDAWeb forces all dataIDs to uppercase, so we added a flag for that.
    Earlier variant guessed at IDs and did reasonable date work.
    Now we use the all.xml file provided by CDAWeb to better match.
    """
    dest = defaults["dest_prefix"]
    now = time.time()

    regex_base, regex_pattern = cxc.load_fromxml(
        defaults["xml_path"], strip_me=defaults["strip_me"]
    )

    db_Conn, db_Cursor = connectDB(defaults["db_name"])
    query = "SELECT dataid, year FROM refresh_indices ORDER by dataid ASC"
    if limit != None:
        query += f" LIMIT {limit}"
    db_Cursor.execute(query)
    rowset = db_Cursor.fetchall()
    print(f"\t(Re)generating {len(rowset)} indices...")
    # walk through each mission/year combo to make a new index
    errorcount = 0
    errorlist = []
    icount = 0
    irows = 0
    track_indices = []
    priorindex = "ignore"
    import pandas as pd
    for row in rowset:
        dataid, year = row[0], row[1]
        queueset = f"{dataid}:{year}"
        query = f"SELECT starttime, fullname, filesize from entries WHERE status=0 AND queueset='{queueset}' ORDER by starttime"
        df = pd.read_sql_query(query, db_Conn)
        df['endtime'] = df['starttime'].shift(-1)
        df['fullname'] = dest + df['fullname']
        df.index.name = '#'
        fullname = df.loc[df.index[0],'fullname']
        dataid, indexbase, x_regex = cxc.extract_regex(
            regex_base, regex_pattern, fullname
        )
        indexname = gen_index_name(defaults,indexbase,dataid,year)
        df.to_csv(indexname,index=True)
        track_indices.append( (dataid,year) )
        icount += 1
        #with smart_open.open(indexname, "w") as fout:
        #        fout.writelines(fdata)
        if debug and icount % 100 == 0:
            print(f"    created {icount} indices in {time.time()-now} seconds")
    db_Conn.close()
    if icount > 0:
        if debug:
            print(
                f"\t... success making %d indices totaling %d files, took %.2f minutes"
                % (icount, irows, (time.time() - now) / 60)
            )
    elif errorcount > 0:
        print(f"Error, {errorcount} files could not be indexed, please redo.")
    return errorcount, track_indices


def guess_datetime(filename, patternset):
    for pattern in patternset:
        try:
            match = pattern.search(filename)
            results = list(match.groups())
            results = [x for x in results if x is not None]
            for i in range(4):
                results.append("00")  # zero pad
            if len(results[2]) == 3:
                doy = True
            else:
                doy = False
            if doy:
                timestamp = "-".join(results[1:6])
                timestamp = datetime.strptime(timestamp, "%Y-%j-%H-%M-%S").strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            else:
                timestamp = "-".join(results[1:7])
                timestamp = datetime.strptime(timestamp, "%Y-%m-%d-%H-%M-%S").strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            return timestamp
        except:
            pass
    """ best guess parser of CDAWeb filenames into an ISO time of
    YYYY-MM-DDTHH:MM:SSZ
    """
    return None


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


"""
Testing: (assuming no existing db)
filelist_100k -> 100k as 1
then filelist_back30k -> 100k become -1, 30k become 1
then filelist_100k ->100k of the prior -1 get dropped and 100k become 0
30k become -1 plus legacy 100k so 130k -1, 70k as 1
then filelist_140k -> 40k as 1, 100k as 0, legacy 100k remain -1
then filelist_100k -> 100k as 0, 140k as -1
"""

# filelist = "filelist_07nov24.gz"
# filelist = "filelist_100k"
# filelist = "filelist_140k"

def prod(
        defaults=None,
        debug=False,
        limit=None,
        checkpoint=1000,
        steps=[True, True, True, True, True],
        bulk=False
):
    track_indices = None
    if defaults == None:
        defaults = default_defaults()
    if debug: print(f"\tUsing database {defaults['db_name']}")
    createDB_safe(defaults["db_name"], debug=debug)
    updateDB_all_defaults(defaults)
    if steps[0]:
        retvals = ingest_and_reconcile(defaults, debug=debug, limit=limit)
    if steps[1]:
        transfer_over(
            defaults["db_name"], checkpoint=checkpoint, debug=debug, limit=limit, bulk=bulk)
    if steps[2]:
        errorstat, track_indices = generate_indices(defaults, debug=debug, limit=limit)
        # note 'track_indices' can be passed to next step as a perf aid
        track_indices = None # fake
    if steps[3]:
        generate_catalog(defaults, debug=debug, limit=limit, track_indices=track_indices)
    if steps[4]:
        db_clear_refresh_indices(defaults["db_name"])
        updateDB_time(None, None, None, db_name=defaults["db_name"])

    validation_show_dups(defaults["db_name"], 5)


def ingest_s3_inventory(
        db_name="db_s3.db",
        infile="manifest.csv",
        strip_me="pub/data/",
        debug=True,
        reorder=None,
        resub=(r"^spdf/cdaweb/", r"pub/"),
        staging_prefix=None,
        limit=None,
        ingest=True
):
    # variant of prod sans copying, takes a filelist of ".*, filesize, s3key"
    # to CREATE a new DB with all of them as 'status=0' (copied over),
    # then re-generates the entire set of indices
    # If file is not in that order, do separately:
    #     import reorder_csv_columns as r
    #     r.reorder_csv_columns("manifest.csv","manifestR.csv",[1, 0])
    # (or similar)
    # in this case, that means give the argument reorder=[0,1] or similar
    # For a default S3 MANIFEST.csv, the reorder matrix is [0,3,4,5,6,7,8,2,1]
    # This will copy the original under a new name (thus safe)
    # It also safely versions the original database so it can start clean,
    # allowing for offline recovery if needed.
    # if 'ingest', it reads in a manifest, if not, it just creates indices
    # from the existing DB
    defaults = default_defaults()
    defaults["db_name"] = db_name
    defaults["filelist"] = infile
    defaults["strip_me"] = strip_me
    if not staging_prefix == None:
        defaults["staging_prefix"] = staging_prefix
    if ingest:
        version_file(defaults["db_name"])
        createDB_safe(defaults["db_name"])
    now = time.time()
    if reorder != None:
        import reorder_csv_columns as r

        origname, infile = infile, "reordered" + infile
        r.reorder_csv_columns(origname, infile, reorder, resub)
        defaults["filelist"] = infile
        if debug:
            print("Reordered input CSV, took %.2f minutes" % ((time.time() - now) / 60))
    if ingest:
        prod(defaults, debug=debug, steps=[True, False, False, False, False],limit=limit)
    db_unsafe_mark_all_as_copied(defaults["db_name"])
    db_missions_to_queue(db_file=defaults["db_name"],status=0) # force index regens later
    prod(defaults, debug=debug, steps=[False, False, True, True, True],limit=limit)


def default_defaults(filelist=None, dest_prefix=None):
    defaults = {
        "db_name": "db_s3.db",
        "catalog_stub": "catalog_updates.csv",
        "transfer_cap_gb": 2000,
        "strip_me": "pub/data/",
        "source_prefix": "https://spdf.gsfc.nasa.gov/pub/data/",
        "staging_prefix": "s3://helio-data-staging/spdf/cdaweb/data/",
        "dest_prefix": "s3://gov-nasa-hdrl-data1/spdf/cdaweb/data/",
        "xml_path": ".",
        "filelist": "filelist.gz",
        "force_uppercase": True,
    }
    if filelist != None:
        defaults["filelist"] = filelist
    if dest_prefix != None:
        defaults["dest_prefix"] = dest_prefix
    return defaults


def testcase_filelist_tiny_copy(limit=12):
    dest = "./cdaweb-test/"  # local test
    # dest = 's3://helio-data-staging/sandytest/cdaweb-test/' # S3 test
    defaults = default_defaults(filelist="filelist_2k",dest_prefix=dest)
    defaults["transfer_cap_gb"] = 0.2
    steps = [True, True, True, True, False]
    prod(defaults, debug=True, limit=limit, steps=steps)

def testcase_filelist_tiny_nocopy():
    # doesn't copy, just tests parsing and index generation
    defaults = default_defaults(filelist="filelist_2k")
    prod(defaults, debug=True, steps=[True, False, False, False, False])
    db_unsafe_mark_all_as_copied()
    prod(defaults, debug=True, steps=[False, False, True, True, False])
    
def testcase_filelist_full_nocopy():
    # doesn't copy, just tests parsing and index generation
    dest = "./cdaweb-test/"  # local test
    defaults = default_defaults(filelist="filelist-filtered-rev",dest_prefix=dest)
    prod(defaults, debug=True, steps=[True, False, False, False, False])
    db_unsafe_mark_all_as_copied()
    prod(debug=True, steps=[False, False, True, True, False])
    
def testcase_filelistS3_merge(defaults=None):
    # runs after 'ingest_s3manifest_nocopy() to do big reconcile and queue
    if defaults == None:
        defaults = default_defaults(filelist="filelist-filtered")
        defaults["staging_prefix"] = 's3://helio-data-staging/spdf/cdaweb/'
        defaults["db_name"] = "db_s3.db"
    if not os.path.exists(defaults["db_name"]):
        print("Warning, ingest of S3 manifest not yet done. Starting now.")
        ingest_s3manifest_nocopy(defaults["db_name"])
    lasttime = "2024-09-15 00:00:00" # orig was 'up to mid-September'
    updateDB_time(None, None, now=lasttime, db_name=defaults["db_name"])
    prod(defaults, debug=True, steps=[True, False, False, False, False])

def ingest_s3manifest_nocopy(db_name,debug=True,limit=None):
    if os.path.exists("reorderedmanifest.csv"):
        infile="reorderedmanifest.csv"
        reorder = None
    else:
        infile="manifest.csv"
        #reorder = [1,0]
        reorder = [3,0,4,5,6,7,8,2,1]

    ingest_s3_inventory(db_name=db_name,debug=debug,infile=infile,reorder=reorder,limit=limit)
    
def test_dbonly():
    testloop = [
        ("filelist_100k", "100k status=1"),
        ("filelist_back30k", "100k status=-1, 30k status=1"),
        ("filelist_100k", "30k status=-1, 100k status=0"),
        ("filelist_140k", "30k status=-1,100k status=0, 40k status=1"),
        ("filelist_200k", "170k status=0, 30k status=1"),
        ("filelist_back30k", "170k status=-1, 30k status=0"),
        ("filelist_100k", "100k status=-1, 100k status=0"),
    ]
    homepath = "."  # location where DBs should be stored, globally for a HC
    db_name = f"{homepath}/spdf_filelist_duck.db"
    dest = "./cdaweb-test/"
    # dest='s3://gov-nasa-hdrl-data1/spdf/cdaweb/data/'
    createDB_safe(db_name)
    defaults = default_defaults()
    defaults["db_name"] = db_name
    defaults["dest_prefix"] = dest
    defaults["filelist"] = "filelist_2k"
    updateDB_defaults(defaults)

    i = 0
    debug = True
    for fpair in testloop:
        db_unsafe_mark_all_as_copied(defaults["db_name"])
        retvals = ingest_and_reconcile(
            db_name, fpair[0], strip_me=defaults["strip_me"], debug=True
        )
        print(i, fpair[0], "expected:", fpair[1], "got:", retvals)
        i += 1

    ignore = print_db_statuses(db_file=defaults["db_name"])
    validation_show_dups(db_name, 5)


# if __name__ == "__main__":
#    db_name = f"{homepath}/spdf_filelist.db"
#    prod(db_name=db_name,filelist='filelist.gz',debug=True)

# tbd:
#  * generate new indices via refresh_indices, then clear refresh_indices
#  * ingesting current holdings to intialize the database-- should we
#    do this from the MANIFEST or the catalog?
#

if __name__ == "__main__":
    # steps: 'ingestS3', 'ingestfilelist', 'copyscript' OR 'copyactual',
    #        'makeindices','finalize'
    try:
        teststep = str(sys.argv[1])
    except:
        teststep = None

    defaults = default_defaults()
    defaults["staging_prefix"] = 's3://helio-data-staging/spdf/cdaweb/'
    defaults["staging_prefix"] = './data/'
    # typical inputs are filelist.gz, filelist, filelist-filtered,
    #     manifest.csv, or reorderedmanifest.csv
    defaults["filelist"] = "filelist-filtered" # or 'manifest.csv'
    defaults["db_name"] = "db_s3_limit20k.db"
    #defaults["db_name"] = "db_s3_04212025.db"
    #defaults["db_name"] = "db_s3_04232025.db"
    limit=None # 2000 for testing, set limit=None for production
    try:
        ignore = print_db_statuses(db_file=defaults["db_name"], noisy=True)
    except:
        print("(No DB yet, continuing)")
    print(f"Engaging in {teststep}")
    if teststep == 'ingestS3':
        # ingest S3 manifest
        # expects either "manifest.csv" or "reorderedmanifest.csv"
        ingest_s3manifest_nocopy(defaults["db_name"],debug=True,limit=limit)
    elif teststep == 'ingestfilelist':
        # now ingest then reconcile filelist.gz
        lasttime = "2024-09-15 00:00:00" # orig was 'up to mid-September'
        updateDB_time(None, None, now=lasttime, db_name=defaults["db_name"])
        prod(defaults, debug=True, steps=[True, False, False, False, False],limit=limit)
    elif teststep == 'copyscript':
        # EITHER generate copy script rather than doing actual copy
        prod(defaults,debug=True,steps=[False, True, False, False, False], bulk=True, checkpoint=100000,limit=limit)
    elif teststep == 'copyactual':
        # OR do actual copying instead of generating copy script
        prod(defaults,debug=True,steps=[False, True, False, False, False],limit=limit)
    elif teststep == 'makeindices':
        #indexmaking
        prod(defaults,debug=True,steps=[False, False, True, True, False],limit=limit)
    elif teststep == 'finalize':
        # finalizing, only do after copying and indexing is verified!!!
        prod(defaults,debug=True,steps=[False, False, False, False, True])
    else:
        print("Nothing done, please give an option in code")
