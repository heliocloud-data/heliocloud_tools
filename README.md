# HelioCloud User Tools
This repository contains tools and documentation for interacting with a HelioCloud instance.

Covering user onboarding, manipulating data in S3, setting up multifactor authentication and other functions.


- ## Importing your dataset into HelioCloud
    We have a full [README found here](s3_operations/importing_dataset.md).
    
    The README covers the possible scenarios and routes to having your data be part of HelioCloud, tools used to pull datasets are also archived in the `fetching`directory.

- ## S3 Operations
    We have a set of tools found in the `s3_operations` directory and an [accompaying readme](s3_operations/README.md).
    
    The tools are used to copy/delete a massive number of objects as well as compare two directories.

- ## Cloud tools
    We have a tool developed by Sandy Antunes to allow us to have a record of our holdings and make the data searchable.

    As well as a PDF [found here](cloud_tools/data_upload_requirements_110422.pdf) detailing the data upload requirements.

- ## User Onboarding and Cognito
    User onboarding is currently limited to a set of instructions outlined in the [onboarding README](users_cognito/README.md).
    
    Follow these instructions for setting up access to the portal and daskhub.

- ## Multifactor Authentication
    We also have an easy to use MFA setup to both authenticate to AWS and to export the appropriate environment variables dynamically which can be [found here](mfa/aws_mfa_setup_guide.md).
    
    There are also some supporting/alternative scripts.
