# HelioCloud Data Uploads

Resource for how to upload data into the GSFC HelioCloud S3 storage, and for [requesting help](https://git.mysmce.com/heliocloud/heliocloud-data-uploads/-/issues) for data uploads.  We recommend you start an 'issue' for your data upload as a way for tracking which data are being moved into the GSFC HelioCloud.

## Getting started

First read the [how to upload slide deck](data_upload_requirements_110422.pdf) to understand the basic process for uploading a big data set to AWS S3.

We recommend that every large data set its own staging bucket where they can upload their data. The bottleneck is just getting the data in a S3 bucket in the same region, then using step functions to do the registration will be extremely fast. Once the data is in S3 in the appropriate region, one can initiate the in-account transfer to the main data staging bucket using a s3-transfer lambda (very rapid).

The main things you need to know about your data are:
* What kind of data files are you using? CDF? Fits? H5?
* Meta data needed within the files:
* zVariables: if a single file holds multiple datasets, then you need a meta-data field in the dataset listing all the variables it contains. This is called ‘zVariables’ in a CDF file.
* Epoch: You need an Epoch variable demarcating the time of the file, since the Lambda functions will scrape out both the start and end time of the file.
* Version: string representing the version number of the file
* Manifest data (see slide 5)

## Roadmap
We are completing the upload of MMS data as the pilot method.  Each data set is slightly different so, as additional datasets are added, please use the tickets function here to track problems and the contributions section to share solutions and advice.

Data formats
* Anticipate a need to add a separate file reader for each type of anticipated data format to the Lambda function as well as add the dependencies into the Lambda layer (we are opening up the file and scraping the meta data out)
* Right now, have only developed the CDF file reader
* Incorporating other types requires more development on the Lambdas and testing

Transfer source points for the data
* Various constituents have requested different upload mechanisms: local drive, CDAWEB-style web server, Snowball transfer, S3
* Transfer from out of account and region more difficult and costly than in same AWS region
* Generally recommended to keep the same file structure as the conventional archive data

Meta data in the files
* Need specific pieces of meta data to be present in the files in order for them to be registered
* Meta data may or may not be present depending on the mission, source, etc.
* Different search requests by different users and data types
* Can’t generalize without diving into each specific mission and its specific data
* HelioCloud upload and storage uses a simple File Registry. Adding a search layer on top would be a worthwhile project for a team. We recommend users embed the metadata you wish to search on within the files to enable later discovery.

AWS architecture issues
* Remember AWS charges for storage, CPU, and also has microtransaction costs on per-file upload and per-file access.
* For data processing, you can (a) upload data then process in an EC2 or (better) create a Lambda function to handle the processing during the upload
* Lambdas require testing by the data owners
* Testing Lambdas with small batches recommended prior to the entire upload to reduce AWS costs

## Status (Nov 17 2022)
* Solved for MMS as the pilot
* Expanding to CDAWEB requires ensuring the developed pipeline works for all the missions uploaded to CDAWEB
* Expanding to SDO, AIA, SDAC requires yet more analysis

## License
This work is licensed under the BSD-3 open source license.

