"""
Tests the Registry module's Ingester function in a live AWS environment, using a
fully deployed HelioCloud instance.
"""
import os
import random
from unittest.mock import Mock

import boto3
import pytest

from registry.lambdas.app.aws_utils.s3 import (
    get_bucket_name,
    get_dataset_entries_from_s3,
    get_s3_manifest_key,
    get_manifest_from_s3,
)
from registry.lambdas.app.catalog.dataset_repository import DataSetRepository
from registry.lambdas.app.core.exceptions import IngesterException
from registry.lambdas.app.ingest.ingester import Ingester
from utils import (
    new_boto_session,
    get_hc_instance,
    get_region,
)

# Template for entries file to use in the upload to the ingest s3 bucket
ENTRIES_FILE_TEMPLATE = "test/integration/resources/s3/to_upload/entries_template.json"
ENTRIES_FILE = "test/integration/resources/s3/to_upload/entries.json"

# Bucket name prefixes
INGEST_BUCKET_PREFIX = "test-ingester-aws-ingest"
DATASET_BUCKET_PREFIX = "test-ingester-aws-dataset"

# Path in the AWS S3 bucket for ingest at which the upload job can be located
INGEST_FOLDER = "upload/"
# The entries file for the upload job
ENTRY_KEY = INGEST_FOLDER + "entries.json"

# BOTO_CONFIG
BOTO_S3_CLIENT_CONFIG = {"verify": False}


# Fixtures required by our tests. Most are declared module scope because creating new AWS S3
# resources can be an expensive operation.
@pytest.fixture(scope="module")
def session() -> boto3.Session:
    """
    Boto3 session for use in all tests for this module.
    """
    return new_boto_session(get_hc_instance())


# Create the dataset (destination) S3 bucket to use in our tests
@pytest.fixture(scope="module")
def dataset_bucket(session: boto3.Session) -> str:
    """
    Create the AWS S3 bucket for Dataset storage.
    """

    # Create the bucket and yield it back
    s3_client = session.client("s3")
    region = get_region(get_hc_instance(), True)
    bucket = DATASET_BUCKET_PREFIX + "-" + str(random.randint(0, 1000))
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region}
        )
    yield bucket

    # Delete the dataset bucket
    response = s3_client.list_objects(Bucket=bucket)
    if "Contents" in response:
        keys = [content["Key"] for content in response["Contents"]]
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in keys]},
        )
    s3_client.delete_bucket(Bucket=bucket)
    s3_client.close()


# Create the AWS S3 bucket Ingester tests will read from, and populate it with test data.
@pytest.fixture(scope="module")
def ingest_bucket(session: boto3.Session, dataset_bucket: str) -> str:
    """
    Creates the AWS S3 bucket used for data ingestion as required by the tests.
    Destroys it on module completion.
    """

    # Create the AWS S3 bucket to test the Ingester with
    s3_client = session.client("s3")
    region = get_region(get_hc_instance(), True)
    bucket_name = INGEST_BUCKET_PREFIX + "-" + str(random.randint(0, 100))
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region}
        )

    # Use the template to produce an entries.json file with the right dataset bucket name,
    # then upload it
    with open(ENTRIES_FILE_TEMPLATE, "r") as file:
        updated_entries_file = file.read().replace("${DATASET_BUCKET}", dataset_bucket)
        with open(ENTRIES_FILE, "w") as file:
            file.write(updated_entries_file)

    # Upload the entries file and delete the local copy
    s3_client.upload_file(Filename=ENTRIES_FILE, Bucket=bucket_name, Key=ENTRY_KEY)
    os.remove(ENTRIES_FILE)

    # Uploading data files
    to_upload_path = "test/integration/resources/s3/to_upload/"
    for root, dirs, files in os.walk(to_upload_path):
        for filename in files:
            local_path = os.path.join(root, filename)
            key = INGEST_FOLDER + local_path.split(to_upload_path)[1]
            s3_client.upload_file(Filename=local_path, Bucket=bucket_name, Key=key)
    yield bucket_name

    # Clean up the ingest bucket.
    response = s3_client.list_objects(Bucket=bucket_name)
    keys = [content["Key"] for content in response["Contents"]]
    s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": [{"Key": key} for key in keys]})
    s3_client.delete_bucket(Bucket=bucket_name)
    s3_client.close()


# Mocked up repo instance to inject into Ingester instances
@pytest.fixture(scope="module")
def dataset_repo() -> DataSetRepository:
    """
    Prepares a mock DataSetRepository for the Ingester instances that will be created
    in the tests.
    """
    # Only the save method will be invoked by the Ingester in these tests
    repo = Mock(DataSetRepository)
    repo.save.return_value = 1
    return repo


# Test Methods
def test_missing_file(
    session: boto3.Session, ingest_bucket: str, dataset_bucket: str, dataset_repo: DataSetRepository
) -> None:
    """
    Confirm the Ingester throws an exception if a file in the manifest is not present.
    """

    # The MMS dataset contains the test manifest to use
    entry_ds_list = get_dataset_entries_from_s3(
        session=session, bucket_name=ingest_bucket, entry_key=ENTRY_KEY
    )
    entry = next(entry for entry in entry_ds_list if entry.dataset_id == "MMS")
    manifest_key = get_s3_manifest_key(entry, INGEST_FOLDER).replace(
        "manifest.csv", "manifest_missing_file.csv"
    )
    manifest_df = get_manifest_from_s3(
        session=session, bucket_name=ingest_bucket, manifest_key=manifest_key
    )

    # Run the Ingester on the MMS dataset with a manifest listing a file that isn't present
    with pytest.raises(IngesterException):
        ingester = Ingester(
            ingest_bucket=ingest_bucket,
            ingest_folder=INGEST_FOLDER,
            entry_dataset=entry,
            manifest_df=manifest_df,
            ds_repo=dataset_repo,
        )
        ingester.execute()


def test_invalid_destination_bucket(
    session: boto3.Session, ingest_bucket: str, dataset_bucket: str, dataset_repo: DataSetRepository
) -> None:
    """
    Confirm that the Ingester throws an exception if the destination bucket is non-existent.
    """

    # Update the DataSet entries to reference a non-existent bucket
    def set_invalid_bucket(entry):
        entry.index = entry.index.replace(get_bucket_name(entry.index), "bucket.doesnt.exist")
        return entry

    entry_ds_list = [
        set_invalid_bucket(entry)
        for entry in get_dataset_entries_from_s3(
            session=session, bucket_name=ingest_bucket, entry_key=ENTRY_KEY
        )
    ]

    # Ingester should throw an exception if it encounters an invalid destination bucket
    with pytest.raises(IngesterException):
        for entry in entry_ds_list:
            manifest = get_manifest_from_s3(
                session=session,
                bucket_name=ingest_bucket,
                manifest_key=get_s3_manifest_key(entry, INGEST_FOLDER),
            )
            ingester = Ingester(
                ingest_bucket=ingest_bucket,
                ingest_folder=INGEST_FOLDER,
                entry_dataset=entry,
                manifest_df=manifest,
                ds_repo=dataset_repo,
            )
            ingester.execute()


def test_bad_extension(
    session: boto3.Session, ingest_bucket: str, dataset_bucket: str, dataset_repo: DataSetRepository
) -> None:
    """
    Confirm that the Ingester throws an exception if it encounters a file with a bad extension
    """
    entry_ds_list = get_dataset_entries_from_s3(
        session=session, bucket_name=ingest_bucket, entry_key=ENTRY_KEY
    )

    # MMS has the bad file & corresponding manifest
    entry = next(entry for entry in entry_ds_list if entry.dataset_id == "MMS")
    manifest_key = get_s3_manifest_key(entry, INGEST_FOLDER).replace(
        "manifest.csv", "manifest_bad_extension.csv"
    )
    manifest = get_manifest_from_s3(session, bucket_name=ingest_bucket, manifest_key=manifest_key)

    # Ingester should throw an exception if it encounters a bad file extension
    with pytest.raises(IngesterException):
        ingester = Ingester(
            ingest_bucket=ingest_bucket,
            ingest_folder=INGEST_FOLDER,
            entry_dataset=entry,
            manifest_df=manifest,
            ds_repo=dataset_repo,
        )
        ingester.execute()


def test_ingester_aws(
    session: boto3.Session, ingest_bucket: str, dataset_bucket: str, dataset_repo: DataSetRepository
) -> None:
    # Get the entry dataset and update the index to use the dataset bucket created
    # Create an Ingester instance and run it
    s3_client = session.client("s3")
    entry_ds_list = get_dataset_entries_from_s3(
        session=session, bucket_name=ingest_bucket, entry_key=ENTRY_KEY
    )
    for entry in entry_ds_list:
        # Get the manifest for this entry
        manifest_df = get_manifest_from_s3(
            session=session,
            bucket_name=ingest_bucket,
            manifest_key=get_s3_manifest_key(entry, INGEST_FOLDER),
        )

        ingester = Ingester(
            ingest_bucket=ingest_bucket,
            ingest_folder=INGEST_FOLDER,
            entry_dataset=entry,
            manifest_df=manifest_df,
            ds_repo=dataset_repo,
        )
        result = ingester.execute()

        # Check that the dataset was updated correctly
        assert result.dataset_updated == entry.dataset_id
        assert result.files_contributed == 6

        # Confirm the yearly index files for the ingested dataset are present
        # and of the correct size
        index_path = entry.index.replace(f"s3://{dataset_bucket}/", "")
        for year in [2015, 2019]:
            key = f"{index_path}/{entry.dataset_id}_{year}.csv"
            response = s3_client.get_object(Bucket=dataset_bucket, Key=key)

            if year == 2015:
                assert len(response["Body"].readlines()) - 1 == 4
            if year == 2019:
                assert len(response["Body"].readlines()) - 1 == 2

        # There should be 8 keys total for each dataset
        dataset_path = entry.index.replace(f"s3://{dataset_bucket}/", "")
        response = s3_client.list_objects(Bucket=dataset_bucket, Prefix=f"{dataset_path}")
        assert len(response["Contents"]) == 8

    # Check the upload bucket
    response = s3_client.list_objects(Bucket=ingest_bucket)
    keys = [entry["Key"] for entry in response["Contents"]]

    # Count of the only files that should be remaining (entries.json, manifests, etc)
    assert len(keys) == 8
    # Check that the Dataset repo had save called twice
    assert dataset_repo.save.call_count == len(entry_ds_list)

    s3_client.close()
