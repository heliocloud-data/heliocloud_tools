
BUCKET=$0
OUTPUT_LIST=$1

# 1. get a list of all current object on bucket
python make_objlist.py -b $BUCKET -a -o $OUTPUT_LIST

# 2. Create a list of shell commands to execute
cmds=`python make_delete_script.py -b helio-data-staging -o $OUTPUT_LIST`
echo $cmds

# 3. Execute commands to delete data
$cmds

echo "Finished"
