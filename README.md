# HelioCloud Data Uploads

Resource for how to upload data into the GSFC HelioCloud S3 storage, and for [requesting help](https://git.mysmce.com/heliocloud/heliocloud-data-uploads/-/issues) for data uploads.  We recommend you start an 'issue' for your data upload as a way for tracking which data are being moved into the GSFC HelioCloud. To populate a data upload request, required information includes:
* The type of file transfer (web server, S3 transfer + region & account, Snowball transfer, local drive transfer)
* File formats present
* Estimated number of files
* Estimated average file size
* Meta data variables in each file
 
The team is focused on the MMS uploads currently.  New upload requests will be evaluated at our periodic team meetings as to when in 2023-2024 they may be schedulable.

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

