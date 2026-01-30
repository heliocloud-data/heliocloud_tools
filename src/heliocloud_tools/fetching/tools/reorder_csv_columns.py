"""
Simple routine to re-order the columns in a CSV file.

Usage: reorder_csv_columns(input_file, output_file, new_order, resub=None)
  where order is e.g. [1, 0, 2] to put the original 0,1,2 in order 1,0,2

  optionally, resub is a list of 2 regexes to apply to the last of the
  new columns
    e.g.  resub=(r"^spdf/cdaweb/", r"pub/")
"""

import pandas as pd

def reorder_csv_columns(input_file, output_file, new_order, resub=None):
    df = pd.read_csv(input_file, header=None)
    df = df.iloc[:, new_order]
    if resub != None:
        df = df_str_replace(df,resub[0],resub[1])
    df.to_csv(output_file, index=False)
    print(f"CSV file reordered and saved as: {output_file}")

def df_str_replace(df,bad_pattern, good_pattern, index=None):
    # useful for altering in bulk, such as to match later regexes
    if index == None:
        index = list(df.columns)[-1] # default is primary data is last entry
    df[index] = df[index].astype(str).str.replace(bad_pattern,good_pattern,regex=True)
    return df

def testme():
    # Create a simple test CSV file
    test_csv = "test.csv"
    with open(test_csv, "w") as f:
        f.write("A,B,C\n1,2,3\n4,5,6\n7,8,9\n")
    # Call function to reorder columns (swap columns 1 & 2)
    reorder_csv_columns("test.csv", "test_reordered.csv", [1, 0, 2])
