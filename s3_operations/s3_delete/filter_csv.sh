#!/bin/bash

# Check if at least two arguments are provided
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 [-keep] search_string file1.csv file2.csv ..."
    exit 1
fi

# Check for --keep flag
keep=false
if [ "$1" == "-keep" ]; then
    keep=true
    shift
fi

# Read search string from the first argument
search_string="$1"
shift

# Create tempFilter directory if --keep is set
if [ "$keep" = true ]; then
    mkdir -p ./tempFilter
fi

# Loop through all provided CSV files
for file in "$@"; do
    # Check if file exists
    if [ ! -e "$file" ]; then
        echo "File $file not found, skipping."
        continue
    fi
    
    # Search for the string in the file, stop at first match
    if grep -q "$search_string" "$file"; then
        echo "Match found in $file, keeping file."
    else
        if [ "$keep" = true ]; then
            echo "No match found in $file, moving to ./tempFilter/."
            mv "$file" ./tempFilter/
        else
            echo "No match found in $file, deleting file."
            rm "$file"
        fi
    fi
done
