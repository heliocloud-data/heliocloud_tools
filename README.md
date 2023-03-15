# HelioCloud Data Uploads

Resource for how to upload data into the GSFC HelioCloud S3 storage, and for [requesting help](https://git.mysmce.com/heliocloud/heliocloud-data-uploads/-/issues) for data uploads.  We recommend you start an 'issue' for your data upload as a way for tracking which data are being moved into the GSFC HelioCloud. To populate a data upload request, required information includes:
* The type of file transfer (web server, S3 transfer + region & account, Snowball transfer, local drive transfer)
* File formats present
* Estimated number of files
* Estimated average file size
* Meta data variables in each file
 
The team is focused on the MMS uploads currently.  New upload requests will be evaluated at our periodic team meetings as to when in 2023-2024 they may be schedulable.

## Methods -- Data upload scenarios:

HelioCloud requires data, a catalog.json entry for each dataset, and (for each dataset) file manifests called the fileRegistry of the name <id>_YYYY.csv in the CloudMe format (generally 'startdate, s3key, filesize').  Acquiring the 'startdate' is the only metadata not available via S3 Inventory, and is often the missing gap in registering a dataset.

Note that providers who create the fileRegistry for us can add additional metadata past the core 3.


### Method 1) Staged S3 dataset (e.g. EUV-ML)

Data owner provides data in an S3 bucket with the required catalog.json entry and fileRegistry <id>_YYYY.csv files in the CloudMe format.

HelioCloud updates the bucket name in the catalog.json and <id>_YYYY.csv files to reflect the new S3 location, copies the data and <id>_YYYY.csv files to the public S3, and updates the existing bucket catalog.json file with the new or additional entries.

HelioCloud uses S3 Inventory to verify all contents were copied across before deleting the staged data.

case 1: Updates are provided in the same fashion; the same process is followed
case 2: Updates are fetched via script; use Method 2


### Method 2) Script-fetched dataset (e.g. CDAWeb)

Data owner (in consultation with HelioCloud) defines or creates a Python script that (a) fetches the data and (b) generates an appropriate catalog.json entry and fileRegistry <id>_YYYY.csv files in the CloudMe format.  HelioCloud executes the script; the fetched items are put in a temporary S3 bucket.

Once contents arrive, Method 1 proceeds.

Optionally, a 'drop' list may also be provided indicating HelioCloud archive files to remove (due to versioning etc).  This should be a simple .txt file, 1 line per file, with either the relative path or the fully qualified S3 path indicated.  For relative paths, HelioCloud will append the S3 bucket/subbucket.  HelioCloud will then perform the drop.


### Method 3) Snowball or other unsorted dataset (e.g. SDO)

Data owner has provided data in S3 without the specified catalog.json entry or fileRegistry <id>_YYYY.csv files.

Someone writes a script that extracts the necessary metadata to create the catalog.json entry for each dataset, and the <id>_YYYY.csv files for each dataset.

If the files are in a staging area, Method 1 is performed. If the files are already in the destination bucket, the destination bucket catalog.json is updated with the new entries and the task is complete.


## Getting started

First read the [how to upload slide deck](data_upload_requirements_110422.pdf) to understand the basic process for uploading a big data set to HelioCloud.

We recommend that every large data set its own staging bucket where they can upload their data. The bottleneck is just getting the data in a S3 bucket in the same region, then using step functions to do the registration will be extremely fast. Once the data is in S3 in the appropriate region, one can initiate the in-account transfer to the main data staging bucket using a s3-transfer lambda (very rapid).

The main things you need to know about your data are:
* What kind of data files are you using? CDF? Fits? H5?
* Meta data needed within the files:
    - zVariables: if a single file holds multiple datasets, then you need a meta-data field in the dataset listing all the variables it contains. This is called ‘zVariables’ in a CDF file.
    - Epoch: You need an Epoch variable demarcating the time of the file, since the Lambda functions will scrape out both the start and end time of the file.
    - Version: string representing the version number of the file
* Manifest data (see slide 5)

## MMS Pilot Data Upload (in progress) – all data products from the MMS mission will be uploaded and registered in DynamoDB tables
* Web server upload (CDAWEB)
* File manifest generated via web scraper
* Rely on following meta data variables within each file:
    - Epoch (time)
    - Version
    - zVariables (list of all variables contained within the file)
* CDF file reader

## Roadmap
After completion of the MMS upload, the data upload pipeline will be expanded to all of CDAWEB's holdings. Further generalization of the data upload/registration mechanism will be concentrated in the following areas:

Data formats
* Expand to support FITS, netCDF, H5 (will require additional capability in the Lambda functions, and added dependencies embedded within Lambda layers)

Transfer source points for the data
* Add S3 in-region/in-account transfer capability (i.e., Snowball)
* Add S3 out-of-region/out-of-account transfer capability

Meta data in the files
* Generalization of meta data requirements for different data sources (i.e., CDF files may have different variable names for meta data than other file types, meta data may or may not be present)
* Expand query capabilities of the DynamoDB dataframes based on user requests and additional supplied meta data

<!--
Future AWS architecture improvements
* Initiate AWS Batch jobs with initial file manifest rather than Lambda + SQS
* Create subscription services to indicate file upload has been completed (SNS) for a requested file manifest
* Create subscription services where users can subscribe to see updates in particular data products (SNS)
* Migrate entire upload pipeline to be infrastructure as code (AWS CDK)
-->

Informal Suggestions
* Remember AWS charges for storage, CPU, and also has microtransaction costs on per-file upload and per-file access.
* For data processing, you can (a) upload data then process in an EC2 or (better) create a Lambda function to handle the processing during the upload
* Lambdas require testing by the data owners
* Testing Lambdas with small batches recommended prior to the entire upload to reduce AWS costs

## Status (Nov 17 2022)
* Solved for MMS as the pilot, MMS official upload in progress.

## License
This work is licensed under the BSD-3 open source license.

