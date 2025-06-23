"""
Helper methods for creating a DataSet instance from an entries.json file stored on AWS s3
or the local file system.
"""
import json
import boto3

from ..core.exceptions import IngesterException
from ..model.dataset import DataSet


def get_entry_from_s3(s3client: boto3.client, bucket_name: str, entry_key: str) -> DataSet:
    """
    Load the entries.json file from an S3 location, returning a DataSet instance.
    """
    if not entry_key.endswith(".json"):
        raise IngesterException(f"Expecting .csv extension for manifest file: {entry_key}.")

    response = s3client.get_object(Bucket=bucket_name, Key=entry_key)
    data = json.load(response["Body"])
    return DataSet.from_serialized_dict(data)
