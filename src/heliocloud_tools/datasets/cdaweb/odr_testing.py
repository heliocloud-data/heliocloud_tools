"""
Several examples:
  getting the metadata for an ID
  getting a sample codeblock with the ID metadata
  using a local or alt catalog file

"""
import cloudcatalog as cc
#import cloudcat_indexer as ci

fr = cc.CloudCatalog("s3://gov-nasa-hdrl-data1/")
id = "GENESIS_3DL2_GIM" # id = "aia_0094" # id = "MMS1_ASPOC_SRVY_L2"
meta = fr.get_entry(id)
print(meta)

#html = ci.fetch_code(meta)
#print(html)

#html = ci.render_index_html(meta)
#print(html)

id = fr.reverse_lookup_ids("genesis/gim/3dl2_gim")
print("Got id",id)



import cloudcatalog as cc
#fr = cc.CloudCatalog("s3://gov-nasa-hdrl-data1",cache=False)
#fr = cc.CloudCatalog("s3://gov-nasa-hdrl-data1",altcatalog="catalog-20260428.json",cache=False)
fr = cc.CloudCatalog(".",altcatalog="catalog-jun11.json",cache=False)

dataid = "OMNI_HRO2_1MIN"
start= "2018-09-29T00:00:00Z"
stop="2018-10-10T00:00:00Z"
fk = fr.request_cloud_catalog(dataid,start_date=start,stop_date=stop)
print(dataid)
print(fk)
print(fk['datakey'])
sample = fr.sample_file(dataid)
print(sample)

dataset_id1 = 'MMS1_FEEPS_BRST_L2_ELECTRON'
start = '2025-02-01T00:00:00Z'
stop =   '2025-02-02T00:00:00Z'
filekeys_id1 = fr.request_cloud_catalog(dataset_id1,start_date=start,stop_date=stop)
print("filekeys for ",dataset_id1,start,stop,":\n",filekeys_id1)
sample = fr.sample_file(dataid)
print(sample)

dataid = "PARKERSOLARPROBE_WISPR_FITS_LEVEL1_PT30M"
meta = fr.get_entry(dataid)
start= "2020-01-18T00:00:00Z"
stop="2021-12-10T00:00:00Z"
fk = fr.request_cloud_catalog(dataid,start_date=start,stop_date=stop)
print(f"There are {len(fk)} files for {dataid} over {meta['start']}-{meta['stop']}")
sample = fr.sample_file(dataid)
print(sample)
#"index": "s3://gov-nasa-hdrl-data1/sdac/psp_wispr/indices/"





""" Simple example reading (from cloud, no local copying needed) data from:
    SDO/AIA 0171, Stereo+SOHO ML-ready 0171, WISRP L2, and MMS FEEPS BRST L2
    It fetches the full filelist for the given date range, then peeks at the
    first two of each list and prints the filename plus summary information
    derived from reading in the data.

    All CDAWeb data is in HelioCloud.
    VSO data currently in HelioCloud: SDO aia/hmi, EUVML STEREO/SOHO, PSP WISPR
    Waiting on SDO metadata for: fermi, hinode sot/eis, iris, solar_orbiter,
                                 trace, yohkoh

    To look up specific dataIDs for fetching, use e.g.:

    import cloudcatalog
    mysearch = cloudcatalog.EntireCatalogSearch()
    items = mysearch.search_by_id('aia')
    print("\n".join(f"{d['id']}: {d['title']}" for d in items))
"""

""" on pyspedas:
Cloud Repositories
SPEDAS_DATA_DIR and mission specific data directories can also be the URI of a cloud repository (e.g., an S3 repository). If this data directory is set to an URI, files will be downloaded from the data server to the URI location. The data will then be streamed from the URI without needing to download the file locally.

In order to successfully access the specified cloud repository, the user is required to correctly set up permissions to be able to read and write to that cloud repository on their own. Refer (here)[https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html] for how to prepare your AWS configuration and credentials.
"""

import cloudcatalog
import cdflib
import sunpy.map
 
#fr = cloudcatalog.CloudCatalog("s3://gov-nasa-hdrl-data1/") # initialize
fr = cloudcatalog.CloudCatalog(".",altcatalog="catalog-jun11.json") # initialize

start, stop = "2021-08-04T00:00:00Z", "2021-08-15T23:59:59Z" # Encounter 9

dataid = "aia_0171"
meta = fr.get_entry(dataid)
print(meta)
aia_filekeys = fr.request_cloud_catalog(dataid,start_date=start,stop_date=stop)
print(aia_filekeys.iloc(0))
for fname in aia_filekeys['datakey'].to_list()[0:2]:
    print(fname)
    mydata = sunpy.map.Map(fname, fsspec_kwargs={"anon": True})
    print(mydata)

dataid = "euvml_171" # stereo + soho, ML-ready
meta = fr.get_entry(dataid)
print(meta)
euvml_filekeys = fr.request_cloud_catalog(dataid,start_date=start,stop_date=stop)
for fname in euvml_filekeys['datakey'].to_list()[0:2]:
    print(fname)
    mydata = sunpy.map.Map(fname, fsspec_kwargs={"anon": True})
    print(mydata)
       
dataid = "PARKERSOLARPROBE_WISPR_FITS_LEVEL2_PT30M"
wispr_filekeys = fr.request_cloud_catalog(dataid,start_date=start,stop_date=stop)
meta = fr.get_entry(dataid)
print(meta)
print(wispr_filekeys)
for fname in wispr_filekeys['datakey'].to_list()[0:2]:
    print(fname)
    mydata = sunpy.map.Map(fname, fsspec_kwargs={"anon": True})
    print(mydata)
 
dataid='MMS1_FEEPS_BRST_L2_ELECTRON'
meta = fr.get_entry(dataid)
print(meta)
mms_filekeys = fr.request_cloud_catalog(dataid,start_date=start,stop_date=stop)
for fname in mms_filekeys['datakey'].to_list()[0:2]:
    print(fname)
    mydata = cdflib.CDF(fname)
    print(mydata.cdf_info())



import cloudcatalog as cc

fr = cc.CloudCatalog("s3://gov-nasa-hdrl-data1",cache=False)

dataid = "PARKERSOLARPROBE_WISPR_FITS_LEVEL1_PT30M"

meta = fr.get_entry(dataid)

df_psp  = fr.request_cloud_catalog(dataid,
                                   start_date=meta['start'],
                                   stop_date=meta['stop'])
print(f"ODR dataid: {dataid}\n")
print(f"There are {len(df_psp)} files for {dataid} over {meta['start']}-{meta['stop']}")

iframe = df_psp.iloc[0]
print(f"\nSample single element:\n{iframe}\n")

print(f"Metadata:\n{meta}")

#"index": "s3://gov-nasa-hdrl-data1/sdac/psp_wispr/indices/"

