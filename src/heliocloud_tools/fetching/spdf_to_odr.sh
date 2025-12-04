#!/bin/bash

# Usage: ./spdf_to_odr.sh [1/2] [manifest_file]
# Default manifest file is "manifest.csv"
# Output is 'fetchme.list'
#
# After running, to fetch, add https://spdf.gsfc.nasa.gov/pub/[FILE]
# For destination, add s3://gov-nasa-hdrl-data1/spdf/cdaweb/[FILE]
#
# prompts for the AWS manifest name, default 'manifest.csv'
#
# Combines CDAWeb's spdf_filelist.sh for fetching, removes the fetch part,
# then adds comparison code against our AWS manifest to generate a list
# of files to fetch in 'data/*' format.  Also does a pre-extraction of
# only spdf/cdaweb/data entries from the manifest.csv, so it is safe to
# run on a full manifest.

# note it can take 5-10 minutes to fetch the listings file from SPDF,
# then 10-15 minutes to filter and produce the fetch list.
#

# This script for creating a list of new files to fetch from CDAWeb has
# two options.  The process is: fetch the latest CDAWeb holdings
# ('spdf_curr'), then either (1) compare it against the previous
# 'spdf_curr', or (2) compare it against the AWS 'manifest.csv'.  Either
# way, the output goes into 'fetchme.list' and is one FILENAME per line
# in the form of 'data/*'.

# The script prompts you to choose either mode 1 or 2.  It exits if you
# choose mode 1/CDAWeb diff but there isn't an existing 'spdf_curr',
# or if you choose mode 2/AWS manifest but there isn't an existing
# 'manifest.csv'. (Optionally, you can specify an alternative manifest
# filename when you invoke it on the command line).  You can also give
# the 1/2 argument at the command line.

# Optional argument --local uses an already-fetched filelist.gz instead
# of curl/wget fetch from SPDF.

# After running this script, you separately can do Fetching--
# you grab from 'https://spdf.gsfc.nasa.gov/pub/[FILENAME]' and 
# you put to 's3://gov-nasa-hdrl-data1/spdf/cdaweb/[FILENAME].

USE_LOCAL_FILELIST=0   # default: use curl

POSITIONALS=""

# First pass: collect options anywhere in the argument list
for arg in "$@"; do
    case "$arg" in
        --local)
            USE_LOCAL_FILELIST=1
            ;;
        --remote)
            USE_LOCAL_FILELIST=0
            ;;
        --)  # explicit end of options, everything after is positional
            SHIFT_POS=1
            ;;
        -*)
            echo "Unknown option: $arg"
            exit 1
            ;;
        *)
            POSITIONALS="$POSITIONALS $arg"
            ;;
    esac
done

# Convert back to positional parameters
set -- $POSITIONALS

if [ "$1" = "1" ] || [ "$1" = "2" ]; then
    MODE="$1"
    shift  # shift so next argument (if any) becomes manifest file
else
    # No valid mode on command line → ask user
    # Run off prior CDAWeb list (spdf_curr) or AWS manifest (manifest.csv)
    echo "Select mode:"
    echo "  1 = diff off previous CDAWeb list"
    echo "  2 = diff off entire AWS manifest"
    printf "Enter 1 or 2: "
    read MODE
fi

if [ "$MODE" != "1" ] && [ "$MODE" != "2" ]; then
    echo "Error: invalid mode '$MODE'. Please enter 1 or 2."
    exit 1
fi

if [ "$MODE" = "1" ]; then
    if [ ! -e spdf_curr ]; then
	echo "Error: no 'spdf_curr' exists, exiting."
	exit 1
    fi
    echo "Using prior file: spdf_curr"
fi
       
if [ "$MODE" = "2" ]; then
    MANIFEST="${1:-manifest.csv}"
    # Check manifest file exists
    if [ ! -f "$MANIFEST" ]; then
	echo "Error: Manifest file '$MANIFEST' not found."
	exit 1
    fi    
    echo "Using manifest file: $MANIFEST"
fi

START=$(date +%s)
step_time() {
    NOW=$(date +%s)
    ELAPSED=$((NOW - START))
    echo "[${ELAPSED}s] $1"
}

TMP_NEW="/tmp/new_paths.txt"
TMP_OLD="/tmp/old_paths.txt"

# part 1
# Example script to read the SPDF filelist and new download CDFs and netCDFs since the last time the script is run 
# original 2016 June 2 Robert.M.Candey@nasa.gov, updated 2022 June 7
# have to run twice the first time
# example selects only CDF and netCDF files, but that can be removed from selection process
# outputs spdf_new_files and spdf_deleted_files from the difference of new and old SPDF file lists
# REMOVED also retrieves new files, skipping any patterns in spdf_skipfiles 
#                        or selecting only files matching patterns in spdf_choosefiles
# remove -p from xargs command to automate
# wget can be replaced with "curl -O" (-O has opposite meanings with curl/wget)]
if [ -e spdf_diff ]; then
         rm spdf_diff
fi
if [ -e spdf_deleted_files ]; then
         rm spdf_deleted_files
fi
if [ -e spdf_new_files ]; then
         rm spdf_new_files
fi
if [ -e spdf_curr ]; then
    mv spdf_curr spdf_prev
fi

URL="https://spdf.gsfc.nasa.gov/pub/catalogs/filelist.gz"
#URL="https://heliocloud.org/sandbox/filelist.gz"

if [ "$USE_LOCAL_FILELIST" = "1" ]; then
    step_time "Using local filelist.gz"
    cat filelist.gz | gunzip | egrep '\.cdf|\.nc' | sort -k4 > spdf_curr
else
    step_time "Fetching $URL"
    curl "$URL" | gunzip | egrep '\.cdf|\.nc' | sort -k4 > spdf_curr
    #wget -O - "$URL" | gunzip | egrep '\.cdf|\.nc' | sort -k4 > spdf_curr
fi

step_time "CDAWeb holdings now fetched as spdf_curr"

if [ "$MODE" = "1" ]; then
    if [ ! -e spdf_prev ]; then
        echo "Error: no 'spdf_prev' exists; you need at least one prior run."
        exit 1
    fi

    diff spdf_prev spdf_curr >   spdf_diff
    egrep "^< " spdf_diff >   spdf_deleted_files
    egrep "^> " spdf_diff | awk '{print $NF}' | sed 's:^pub/::' > fetchme.list

    step_time "Created fetchme.list using prior CDAWeb list"
    exit 0
fi
#    if [ -e spdf_skipfiles ]; then
#         if [ -e spdf_choosefiles ]; then
#             cat spdf_new_files|grep -v -f spdf_skipfiles |grep -f spdf_choosefiles |awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"
#         else
#             cat spdf_new_files|grep -v -f spdf_skipfiles |awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"
#         fi
#    elif [ -e spdf_choosefiles ]; then
#        cat spdf_new_files|grep -f spdf_choosefiles |awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"
#    else
#        cat spdf_new_files|awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"
#    fi
#fi

# Part 2, compare the fetched CDAWeb list above to the AWS Manifest,
# which is in the form of 'filename, filesize'

# Extract paths from newest list (last field)
awk '{print $NF}' spdf_curr | sort > "$TMP_NEW"

grep "spdf/cdaweb/data" "$MANIFEST" > manifest_cdaweb.csv

step_time "CDAWeb-only manifest generated as manifest_cdaweb.csv"

# Extract & normalize paths from manifest
awk -F',' '{
    path = $1
    sub(/^spdf\/cdaweb\/data\//, "pub/data/", path)
    print path
}' manifest_cdaweb.csv | sort > "$TMP_OLD"

rm manifest_cdaweb.csv

# Output: files in spdf_curr but not in manifest.csv
comm -23 "$TMP_NEW" "$TMP_OLD" \
	| sed 's:^pub/::' \
	> fetchme.list

rm "$TMP_NEW" "$TMP_OLD"

step_time "Created fetchme.list off AWS $MANIFEST"
