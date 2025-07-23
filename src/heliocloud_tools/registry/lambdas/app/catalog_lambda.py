"""
AWS Lambda implementation for running the Cataloger service.
"""
import os

import boto3

from .aws_utils.lambdas import get_dataset_repository
from .catalog.cataloger import Cataloger


# pylint: disable=unused-argument
# AWS Lambda requires the event & context parameters, even if they aren't used by the lambda
# handler for the Cataloger service.
def handler(event, context) -> dict:
    """
    Lambda handler function for invoking the Catalog generator
    :param event: n/a, but required by AWS Lambda
    :param context: n/a, but required by AWS Lambda
    :return: dictionary of S3 buckets updated and the number of datasets updated in each
    """

    # Steps:
    # 1 - Create a DataSetRepository instance pointing to the DocumentDB running in this HelioCloud
    # 2 - Instantiate a Cataloger instance
    # 3 - Execute the Cataloger

    # Boto3 session for AWS
    session = boto3.session.Session()

    # DataSetRepository creation
    ds_repo = get_dataset_repository(session=session)

    # Values to use for certain catalog fields
    name = os.environ["CATALOG_NAME"]
    contact = os.environ["CATALOG_CONTACT"]

    # Create and execute a Cataloger
    cataloger = Cataloger(dataset_repository=ds_repo, session=session, name=name, contact=contact)
    results = cataloger.execute()

    # Inform the caller how much the Catalog service processed
    return {
        "num_endpoints_updated": len(results),
        "updates": [
            {"endpoint": result.endpoint, "num_datasets_updated": result.num_datasets}
            for result in results
        ],
    }


# pylint: enable=unused-argument
