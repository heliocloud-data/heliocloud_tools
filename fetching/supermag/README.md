# JHUAPL datasets

This repo is for mission datasets contributed from JHUAPL, including SuperMAG.

Within data
/disks/d0510/project/supermag/deployed/data/
There is
allnetcdf/  mag.01s/  mag.60s/  magncdf.60s/  ulf.01s/  ulfncdf.01s/
You would probably want the mag.01s directory, ulf.01s, and ulfbcdf.01s
 
mag.01s has global/ and station/ in it you would probably want both.

 
Inside of each directory are years directories i.e. 2024/ inside of that 20240101/, etc, and then the data which is in dmap format
Examples:
20240101_TUC.mag.01s.rev-0006.inx
20240101_KEP.mag.01s.rev-0006.dmap  20240101_UPS.mag.01s.rev-0006.dmap

aws s3 cp -r /disks/d0510/project/supermag/deployed/data/ulfncdf.01s/*           s3://gov-nasa-hdrl-data1/contrib/jhuapl/supermag/ulfncdf.01s/
aws s3 cp -r /disks/d0510/project/supermag/deployed/data/ulf.01s/*                   s3://gov-nasa-hdrl-data1/contrib/jhuapl/supermag/ulf.01s/
aws s3 cp -r /disks/d0510/project/supermag/deployed/data/mag.01s/station/*  s3://gov-nasa-hdrl-data1/contrib/jhuapl/supermag/mag.01s/station/
aws s3 cp -r /disks/d0510/project/supermag/deployed/data/mag.01s/global/*   s3://gov-nasa-hdrl-data1/contrib/jhuapl/supermag/mag.01s/global/


The ulfncdf.01s directory is 3.2T. There is also an /xdr directory in mag.01s that has the a year’s worth of data for each station and that is small, like 0.5T. They are in IDL xdr format.

bash-4.4$ du -sh *
409G	allnetcdf
*21T	mag.01s  (station = 17TB) (global = 3.7TB) (xdr, not fetched = 345GB)
1.2T	mag.60s
2.4T	magncdf.60s
19T	ulf.01s (station = 14TB) (global = 5.2TB)
*3.2T	ulfncdf.01s

to copy: mag.01s/global/, mag.01s/station/, ulf.01s/, ulfncdf.01s/
global is size 3.7T,  has subdirs 1998-2024 of size typically 17G?


script
aws s3 cp --recursive --quiet ulfncdf.01s/ s3://helio-data-staging/jhuapl/supermag/ulfncdf.01s/
date
# mag.01s/global/
aws s3 cp --recursive --quiet 2012/ s3://helio-data-staging/jhuapl/supermag/mag.01s/global/2012/


No 2018, 2023 data at all for ulfncdf.01s?

took about 1 day to get 3TB over


COPIED SO FAR:
ulfncdf.01s (3.2 TB)
mag.01s/global (3.7 TB)


tree -sf ulfncdf.01s/ >~/ulfncdf.01s_MANIFEST.txt
tree -sf global/ >~/mag.01s-global_MANIFEST.txt
tree -sf station/ >~/mag.01s-station_MANIFEST.txt
