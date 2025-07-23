"""
Tests the Regsitry module of a live HelioCloud instance that has been deployed to AWS.
"""
import json
import os

import boto3
import pytest

from registry.lambdas.client.invoke import (
    get_ingester_function,
    get_cataloger_function,
    CatalogerResponse,
)
from utils import (
    new_boto_session,
    get_hc_instance,
    get_registry_s3_buckets,
    get_ingest_s3_bucket_name,
    get_region,
)

INGEST_FOLDER = "upload"
MMS_LOCAL_FILES_PATH = "test/integration/resources/s3/to_upload/MMS/"
MMS_ENTRIES_TEMPLATE = "test/integration/resources/s3/to_upload/entries_template_MMS.json"
MMS_ENTRIES_FILE = f"{MMS_LOCAL_FILES_PATH}entries.json"


@pytest.fixture(scope="module")
def session() -> boto3.Session:
    return new_boto_session(get_hc_instance())


@pytest.fixture(scope="module")
def dataset_bucket(session) -> str:
    """
    Returns the name of a single dataset bucket to use from this HelioCloud instance
    (the first bucket).

    Cleans up the dataset bucket after its been used in the tests for this module.
    """
    bucket_name = get_registry_s3_buckets(get_hc_instance())[0]
    yield bucket_name

    # Clean up the registry bucket
    s3_client = session.client("s3")
    response = s3_client.list_objects(Bucket=bucket_name)
    if "Contents" in response:
        keys = [content["Key"] for content in response["Contents"]]
        s3_client.delete_objects(
            Bucket=bucket_name, Delete={"Objects": [{"Key": key} for key in keys]}
        )
        s3_client.close()
    print(f"Finished emptying dataset bucket {bucket_name}.")


@pytest.fixture(scope="module")
def ingest_bucket(session, dataset_bucket) -> str:
    """
    Prepare the ingest bucket of the HelioCloud instance with the MMS data set
    for running an ingest and cataloging job against.
    """
    s3_client = session.client("s3")
    bucket_name = get_ingest_s3_bucket_name(get_hc_instance())

    # Upload the MMS files to the ingest bucket
    for root, dirs, files in os.walk(MMS_LOCAL_FILES_PATH):
        for filename in files:
            # Upload the MMS manifest file
            if filename == "manifest.csv":
                local_file = os.path.join(root, filename)
                key = f"{INGEST_FOLDER}/MMS/manifest.csv"
                s3_client.upload_file(Filename=local_file, Bucket=bucket_name, Key=key)
                print(f"Uploaded manifest file {filename} to s3://{bucket_name}/{key}")
                pass

            # Upload an MMS data file
            if filename.endswith(".cdf"):
                local_file = os.path.join(root, filename)
                key = f"{INGEST_FOLDER}/MMS/{local_file.split(MMS_LOCAL_FILES_PATH)[1]}"
                s3_client.upload_file(Filename=local_file, Bucket=bucket_name, Key=key)
                print(f"Uploaded data file {filename} to s3://{bucket_name}/{key}")
                pass

    # Use the template to produce an entries.json file with the right dataset bucket name,
    # then upload it
    with open(MMS_ENTRIES_TEMPLATE, "r") as entries_template:
        updated_template = entries_template.read().replace("${DATASET_BUCKET}", dataset_bucket)
        with open(MMS_ENTRIES_FILE, "w") as entries_file:
            entries_file.write(updated_template)

    # Upload the entries file and delete the local copy
    key = f"{INGEST_FOLDER}/entries.json"
    s3_client.upload_file(Filename=MMS_ENTRIES_FILE, Bucket=bucket_name, Key=key)
    os.remove(MMS_ENTRIES_FILE)
    print(f"Uploaded entries file {MMS_ENTRIES_FILE} to s3://{bucket_name}/{key}")

    # Return for use as a fixture
    yield bucket_name

    # Clean up the ingest bucket.
    response = s3_client.list_objects(Bucket=bucket_name)
    if "Contents" in response:
        keys = [content["Key"] for content in response["Contents"]]
        s3_client.delete_objects(
            Bucket=bucket_name, Delete={"Objects": [{"Key": key} for key in keys]}
        )
        s3_client.close()
    print(f"Ingest bucket at s3://{bucket_name} emptied.")


def test_registry(session, ingest_bucket, dataset_bucket):
    """
    End to end test of the Registry module
    """

    hc_instance = get_hc_instance()
    lambda_client = session.client("lambda")

    # Run the Ingester lambda
    ingest_function = get_ingester_function(hc_instance, get_region(hc_instance))
    payload = {"job_folder": f"{INGEST_FOLDER}/"}
    response = lambda_client.invoke(FunctionName=ingest_function, Payload=json.dumps(payload))
    assert response["StatusCode"] == 200
    print(f"Ingester function {ingest_function} ran successfully.")

    # Next, run the Cataloger
    cataloger_function = get_cataloger_function(hc_instance, get_region(hc_instance))
    response = CatalogerResponse(
        lambda_client.invoke(FunctionName=cataloger_function, Payload=json.dumps({}))
    )
    assert response.success
    print(f"Cataloger function {cataloger_function} ran successfully.")

    # Now check the results
    s3_client = session.client("s3")

    # 1 - Check that the ingest folder is empty
    response = s3_client.list_objects(Bucket=ingest_bucket, Prefix="upload/")
    assert "Contents" not in response
    print(f"Confirmed s3://{ingest_bucket}/{INGEST_FOLDER} is empty.")

    # 2 - Check the manifest files in the dataset bucket are correct
    #     and their corresponding files are present.

    # 2015 MMS data
    mms_2015_csv = "/tmp/MMS_2015.csv"
    s3_client.download_file(Bucket=dataset_bucket, Key="MMS/MMS_2015.csv", Filename=mms_2015_csv)
    with open(mms_2015_csv) as index_file:
        # Should be 5 lines: header and 4 files
        lines = [line for line in index_file]
        assert len(lines) == 5

        # Check that each file exists
        for line in lines[1:]:
            s3_file = line.split(",")[1]
            key = s3_file.replace(f"s3://{dataset_bucket}/", "").replace("'", "")
            response = s3_client.head_object(Bucket=dataset_bucket, Key=key)
            assert "ContentLength" in response and response["ContentLength"] > 0
            print(f"Confirmed s3://{dataset_bucket}/{key} exists.")
    os.remove(mms_2015_csv)
    print(f"Confirmed s3://{dataset_bucket}/MMS/MMS_2015.csv is correct")

    # 2019 MMS data
    mms_2019_csv = "/tmp/MMS_2019.csv"
    s3_client.download_file(Bucket=dataset_bucket, Key="MMS/MMS_2019.csv", Filename=mms_2019_csv)
    with open(mms_2019_csv) as index_file:
        # Should be 3 lines: header and 2 files
        lines = [line for line in index_file]
        assert len(lines) == 3

        # Check that each file exists
        for line in lines[1:]:
            s3_file = line.split(",")[1]
            key = s3_file.replace(f"s3://{dataset_bucket}/", "").replace("'", "")
            response = s3_client.head_object(Bucket=dataset_bucket, Key=key)
            assert "ContentLength" in response and response["ContentLength"] > 0
            print(f"Confirmed s3://{dataset_bucket}/{key} exists.")
    os.remove(mms_2019_csv)
    print(f"Confirmed s3://{dataset_bucket}/MMS/MMS_2019.csv is correct.")

    # 3 - Check that the catalog was created correctly
    catalog_json = "/tmp/catalog.json"
    s3_client.download_file(Bucket=dataset_bucket, Key="catalog.json", Filename=catalog_json)
    with open(catalog_json) as file:
        catalog = json.load(file)
        assert catalog["endpoint"] == f"s3://{dataset_bucket}"
        assert catalog["name"] is not None
        assert catalog["contact"] is not None

        if "catalog" in catalog:
            mms_found = False
            for ds_entry in catalog["catalog"]:
                if ds_entry["dataset_id"] == "MMS":
                    assert ds_entry["index"] == f"s3://{dataset_bucket}/MMS"
                    assert ds_entry["title"] == "MMS data"
                    assert ds_entry["indextype"] == "csv"
                    assert ds_entry["resource"] == "SPASE-1234567"
                    mms_found = True
            assert mms_found
        else:
            assert False, "Catalog didn't contain the catalog element"
    os.remove(catalog_json)
    print(f"Confirmed s3://{dataset_bucket}/catalog.json is correct.")
