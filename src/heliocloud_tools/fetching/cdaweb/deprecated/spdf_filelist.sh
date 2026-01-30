#!/bin/bash
# Example script to read the SPDF filelist and new download CDFs and netCDFs since the last time the script is run 
# original 2016 June 2 Robert.M.Candey@nasa.gov, updated 2022 June 7
# have to run twice the first time
# example selects only CDF and netCDF files, but that can be removed from selection process
# outputs spdf_new_files and spdf_deleted_files from the difference of new and old SPDF file lists
# also retrieves new files, skipping any patterns in spdf_skipfiles 
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
wget -O - "https://spdf.gsfc.nasa.gov/pub/catalogs/filelist.gz" | gunzip | egrep "\.cdf|\.nc" | sort -k4 >   spdf_curr
#curl "https://spdf.gsfc.nasa.gov/pub/catalogs/filelist.gz" | gunzip | grep "\.cdf" | sort -k4 >   spdf_curr
if [ -e spdf_prev ]; then
    diff spdf_prev spdf_curr >   spdf_diff
    egrep "^< " spdf_diff >   spdf_deleted_files
    egrep "^> " spdf_diff >   spdf_new_files
    if [ -e spdf_skipfiles ]; then
         if [ -e spdf_choosefiles ]; then
             cat spdf_new_files|grep -v -f spdf_skipfiles |grep -f spdf_choosefiles |awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"
         else
             cat spdf_new_files|grep -v -f spdf_skipfiles |awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"
         fi
    elif [ -e spdf_choosefiles ]; then
        cat spdf_new_files|grep -f spdf_choosefiles |awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"
    else
        cat spdf_new_files|awk '{print $NF}'|xargs -I{} -p -t wget "https://spdf.gsfc.nasa.gov/{}"
    fi
fi

