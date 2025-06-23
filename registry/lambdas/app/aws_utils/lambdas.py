"""
Functions for helping with AWS Lambda operations in the Registry
"""
import os
import boto3
from .document_db import get_documentdb_client
from ..catalog.dataset_repository import DataSetRepository


def get_dataset_repository(
    catalog_db_secret: str = None, session: boto3.Session = None, tls_ca_file: str = None
) -> DataSetRepository:
    """
    Gets a DataSetRepository instance configured for the Registry's AWS environment.
    :param catalog_db_secret: the secret to use when fetching DocumentDB credentials from AWS
           Secrets Manager
    :param session: an optional instantiated and configured boto3.Session.
    :param tls_ca_file: an optional TLS CA file path. Defaults to using the CA file bundled in
                        /resources
    :return: an instantiated DataSetRepository
    """

    if catalog_db_secret is None:
        catalog_db_secret = os.environ["CATALOG_DB_SECRET"]

    if session is None:
        session = boto3.Session()

    if tls_ca_file is None:
        tls_ca_file = os.path.dirname(__file__) + "/../resources/global-bundle.pem"

    db_client = get_documentdb_client(
        session=session, secret_name=catalog_db_secret, tls_ca_file=tls_ca_file
    )

    return DataSetRepository(db_client=db_client)
