#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <filename>"
    exit 1
fi

FILE="$1"
TEMP_DIR="./temp_sort_dir"

mkdir -p "$TEMP_DIR"

# Strip the size and save to a new file
sed 's/,.*//' "$FILE" > "processed_${FILE}"

# Sort the file in reverse order
temp_sorted="$TEMP_DIR/sorted"
temp_dirs="$TEMP_DIR/dirs"
temp_objs="$TEMP_DIR/objs"

sort -r "processed_${FILE}" > "$temp_sorted"

# Separate dirs from objs
grep '/$' "$temp_sorted" > "$temp_dirs"
grep -v '/$' "$temp_sorted" > "$temp_objs"

# Group objs first then dirs
cat "$temp_objs" "$temp_dirs" > "processed_${FILE}"

# Clean up temporary directory
# rm -r "$TEMP_DIR"

