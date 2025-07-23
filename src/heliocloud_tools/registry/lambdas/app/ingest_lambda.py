"""
AWS Lambda implementation for running the Ingester service.
"""
import os

import boto3

from .aws_utils.lambdas import get_dataset_repository
from .aws_utils.s3 import get_dataset_entries_from_s3
from .aws_utils.s3 import get_manifest_from_s3
from .ingest.ingester import Ingester


# Ingester does not require context parameter, but AWS Lambda service still requires
# context as a parameter so the handler has the right method signature.
# pylint: disable=unused-argument
def handler(event, context) -> dict:
    """
    AWS Lambda handler for the Ingest service of HelioCloud.

    :param event: must contain the parameter 'job_folder' with its value (the folder in the ingest
        bucket to check for the ingest job
    :param context: n/a (required for method signature)
    :return: a dictionary containing two keys
             dataset_updated:  the name of the dataset in this HelioCloud's catalog that was
             updated by the job
             files_contributed:  the number of files contributed to that dataset
    """

    session = boto3.session.Session()

    # Get the Ingest bucket name & folder
    ingest_bucket = os.environ["ingest_bucket"]
    ingest_folder = str(event["job_folder"])

    # Get the entries dataset
    entry_key = os.path.join(ingest_folder, "entries.json")
    entry_ds_list = get_dataset_entries_from_s3(
        session=session, bucket_name=ingest_bucket, entry_key=entry_key
    )

    # For each entry in the entries file, get the manifest and run an Ingester instance
    updates = []
    s3_client = session.client("s3")
    for entry_ds in entry_ds_list:
        # join ingest folder (ex. my_job/), index split (ex. MMS/), and manifest.csv
        manifest_key = os.path.join(ingest_folder, entry_ds.index.rsplit("/", 1)[1], "manifest.csv")

        # Get the manifest file
        manifest_df = get_manifest_from_s3(
            session=session, bucket_name=ingest_bucket, manifest_key=manifest_key
        )

        # Run the Ingester, catching any exceptions that arise
        update = {"dataset": entry_ds.dataset_id, "num_files_updated": 0, "error": None}
        # pylint: disable=broad-exception-caught
        try:
            result = Ingester(
                session=session,
                ingest_bucket=ingest_bucket,
                ingest_folder=ingest_folder,
                entry_dataset=entry_ds,
                manifest_df=manifest_df,
                ds_repo=get_dataset_repository(),
            ).execute()
            update["num_files_updated"] = result.files_contributed
            s3_client.delete_object(Bucket=ingest_bucket, Key=manifest_key)

        # Ingester failed, so record the dataset impacted and the exception that occurred.
        # so it can be reported back
        except Exception as ex:
            update["error"] = str(ex)
        # pylint: enable=broad-exception-caught

        # Store a record of the update
        updates.append(update)

    # If all entries process successfully, remove the entries file
    if len(updates) == len(entry_ds_list):
        s3_client.delete_object(Bucket=ingest_bucket, Key=entry_key)

    s3_client.close()

    # Return a dictionary of the results
    return {"num_datasets_updated": len(updates), "updates": updates}


# pylink: enable=unused-argument
