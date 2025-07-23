#
# script to compare inventories of 2 buckets. You will need to supply the manifest files
# from S3 inventory for both buckets along with the locations (bucket names) where the 
# manifest files were downloaded from. For example:

# > compare_buckets.sh <src_manifest file> <dest_manifest file>

# NOTE: you *MUST* run aws-mfa first in order to credential in as an AWS user with read access to
# the buckets OR run this script on an EC2 instance which has an IAM role which allows read access 
# to the buckets being compared.

src_manifest=$1
dest_manifest=$2

echo "1. Download the list files for each manifest" 

cmd="python download_s3_inventory.py -m $src_manifest" 
echo $cmd 
src_files=`$cmd`

cmd="python download_s3_inventory.py -m $dest_manifest"
echo $cmd 
dest_files=`$cmd`


echo "2. read separate file lists and merge into unified list"
cmd="python merge_inv_files.py -o merged_src_files.csv $src_files"
echo $cmd
merged_src_files=`$cmd`

cmd="python merge_inv_files.py -o merged_dest_files.csv $dest_files"
echo $cmd
merged_dest_files=`$cmd`


echo "3. Compare bucket file lists and determine which are unique to the first bucket"
cat merged_src_files.csv | sort > merged_src_files_sorted.csv
cat merged_dest_files.csv | sort > merged_dest_files_sorted.csv

echo "Running: comm -23 merged_src_files_sorted.csv merged_dest_files_sorted.csv"
comm -23 merged_src_files_sorted.csv merged_dest_files_sorted.csv > files_missing_in_destination.txt

echo "4. clean up temporary files"
rm *.csv.gz
rm merged_src_files.csv merged_src_files_sorted.csv 
rm merged_dest_files.csv merged_dest_files_sorted.csv 


