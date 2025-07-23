import csv
from datetime import datetime
import os
import re
import sys

def is_iso_time(value):
    """Check if a string is in ISO 8601 format."""
    try:
        datetime.fromisoformat(value.replace('Z', '+00:00'))  # Handle 'Z' as UTC
        return True
    except ValueError:
        return False

def verify_csv(file_path):
    """Verify the first two columns are ISO times and the third column starts with 's3://'."""
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader)  # Skip the header row if present

        for row_number, row in enumerate(reader, start=2):  # Start from 2 to account for header
            if len(row) < 3:
                print(f"Row {row_number} has fewer than 3 columns.")
                continue

            time1, time2, path = row[:3]

            if not is_iso_time(time1):
                #print(f"Row {row_number}, Column 1 is not a valid ISO time: {time1}")
                return False
            if not is_iso_time(time2):
                #print(f"Row {row_number}, Column 2 is not a valid ISO time: {time2}")
                return False
            if not path.startswith('s3://'):
                #print(f"Row {row_number}, Column 3 does not start with 's3://': {path}")
                return False
    return True

def reorder_csv(file_path, output_path):
    """Reorder the columns of a CSV file and write to a new file."""
    with open(file_path, 'r') as csv_file, open(output_path, 'w', newline='') as output_file:
        reader = csv.reader(csv_file)
        writer = csv.writer(output_file)

        header = next(reader)
        new_header = [header[0], header[3], header[1], header[2]]  # Reorder header
        writer.writerow(new_header)

        for row in reader:
            if len(row) < 4:
                print(f"{file_path}: Row {reader.line_num} has fewer than 4 columns.")
                continue
            new_row = [row[0], row[3], row[1], row[2]]  # Reorder row
            writer.writerow(new_row)


if __name__ == "__main__":
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]

    if not csv_files:
        print("No CSV files found in the current directory.")
        sys.exit(0)

    igood, ibad = 0, 0
    for csv_file in csv_files:
        #print(f"Processing file: {csv_file}")
        status = verify_csv(csv_file)
        if status:
            igood += 1
        else:
            ibad += 1
            output_file = f"reordered/{csv_file}"
            reorder_csv(csv_file, output_file)
            #print(f"Failed for file {csv_file_path}")

    print(f"For {igood+ibad} files, {igood} were good and {ibad} were bad")
        
