import os.path
import random
import pytest

from registry.lambdas.app.aws_utils.s3 import get_dataset_entries_from_s3, get_manifest_from_s3
from utils import (
    new_boto_session,
    get_hc_instance,
    get_region,
)

# AWS S3 bucket name to use for this test
BUCKET_NAME_PREFIX = "test-aws-utils"

# Files for entry dataset parsing test
ENTRIES_FILE = os.path.dirname(__file__) + "/resources/s3/to_upload/entries_template.json"
ENTRIES_KEY = "entries.json"

# Files for manifest parsing test
MANIFEST_FILE = os.path.dirname(__file__) + "/resources/s3/to_upload/MMS/manifest.csv"
MANIFEST_KEY = "manifest.csv"


@pytest.fixture(scope="module")
def session():
    """
    Boto 3 session for uses in these tests
    """
    return new_boto_session(get_hc_instance())


@pytest.fixture(scope="module")
def bucket(session):
    """
    Creates an S3 bucket and populates it with the files to be used when testing the AWS utils.
    """
    s3_client = session.client("s3")
    bucket_region = get_region(get_hc_instance(), True)
    bucket_name = BUCKET_NAME_PREFIX + "-" + str(random.randint(0, 1000))

    # Create the bucket
    if bucket_region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": bucket_region}
        )
    print(f"\nCreated {bucket_name} in {bucket_region} for testing.\n")

    # Upload the test files
    s3_client.upload_file(Filename=ENTRIES_FILE, Bucket=bucket_name, Key=ENTRIES_KEY)
    s3_client.upload_file(Filename=MANIFEST_FILE, Bucket=bucket_name, Key=MANIFEST_KEY)

    # Bucket is ready to use
    yield bucket_name

    # Clean up the bucket
    s3_client.delete_object(Bucket=bucket_name, Key=ENTRIES_KEY)
    s3_client.delete_object(Bucket=bucket_name, Key=MANIFEST_KEY)
    s3_client.delete_bucket(Bucket=bucket_name)
    s3_client.close()
    print(f"\nDestroyed bucket {bucket_name} in {bucket_region}.\n")


def test_manifest(bucket, session):
    """
    Check that the manifest can be retrieved properly from an AWS s3 bucket.
    """
    manifest_df = get_manifest_from_s3(
        session=session, bucket_name=bucket, manifest_key=MANIFEST_KEY
    )
    assert manifest_df.shape[0] == 6


def test_dataset_entries_s3(bucket, session):
    """
    Check that the dataset entries file can be retrieved properly from an AWS S3 bucket.
    """
    dataset_list = get_dataset_entries_from_s3(
        session=session, bucket_name=bucket, entry_key=ENTRIES_KEY
    )
    assert dataset_list[0].dataset_id == "MMS"
    assert dataset_list[0].resource == "SPASE-1234567"
