# given 'manifest.csv' and a new 'fetchme.list', tells us how many
# of the new fetchme files are just version updates of existing files.
# Does so by converting the manifest to the file-only format of fetchme,
# stripping everything before a _v#.cdf/nc in filenames,
# then counting the number in 'fetchme' that are already in 'manifest'.

cut -d',' -f1 manifest.csv | sed 's#^spdf/cdaweb/##'  > manifest.list
sed -E 's/_v[0-9]+\.cdf$//; s/_v[0-9]+\.nc$//' manifest.list >manifest.norm
sed -E 's/_v[0-9]+\.cdf$//; s/_v[0-9]+\.nc$//' fetchme.list >fetchme.norm
comm -12 manifest.norm fetchme.norm | wc -l
