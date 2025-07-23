"""
A tool for running a HelioCloud instance's Ingester service from the command line.
"""
import argparse
import json
import sys
from pathlib import Path

import boto3

# pylint: disable=import-error, wrong-import-position
# The catalog_lambda module is in a separate, top level project directory unaffiliated with tools,
# thus importing it requires updating the sys.path with the project directory.
PROJECT_DIR = str((Path(__file__)).parent.parent)
sys.path.append(PROJECT_DIR)
from registry.lambdas.client.invoke import get_ingester_function, IngestResponse

# pylint: enable=import-error, wrong-import-position

# pylint: disable=duplicate-code
if __name__ == "__main__":
    # Get the instance name and job folder from the args
    parser = argparse.ArgumentParser(
        prog="HelioCloud Ingest Runner",
        description="Enables manual invocation of the HelioCloud ingest service against a "
        "specified folder in a"
        "HelioCloud instance's ingest S3 bucket.",
    )
    parser.add_argument(
        "region",
        type=str,
        help="Name of the AWS region in which the HelioCloud instance resides. This should "
        "match the region value configured in the HelioCloud's instance configuration.",
    )
    parser.add_argument(
        "instance",
        type=str,
        help="Name of the HelioCloud instance whose Ingest service should be run. "
        "This should match the instance configuration used to deploy the "
        "HelioCloud to AWS.",
    )
    parser.add_argument(
        "job_folder",
        type=str,
        help="Name of the folder in the ingest bucket (an AWS S3 bucket) provisioned for the "
        "HelioCloud instance being called.",
    )
    args = parser.parse_args()

    region = args.region
    instance = args.instance
    job_folder = args.job_folder

    # Check that the Ingester service can be located
    FUNCTION = get_ingester_function(hc_instance=instance, region=region)
    if FUNCTION is None or FUNCTION == "":
        print(
            f"No AWS Lambda function name found for the Cataloger service in the {instance} "
            f"HelioCloud instance.\n"
            "Are you sure the parameters are correct and/or the HelioCloud instance is "
            "correctly deployed?"
        )
        sys.exit()

    # Specifics of Ingest service instance being invoked
    print(
        f"Invoking HelioCloud Ingester service with parameters:\n"
        f"\tAWS Region: {region}\n"
        f"\tInstance: {instance}\n"
        f"\tLambda function name: {FUNCTION}\n"
        f"\tIngest job folder: {job_folder}\n"
    )

    # Get a lambda client and run the function
    client = boto3.Session(region_name=region).client("lambda")
    inv_response = client.invoke(
        FunctionName=FUNCTION, Payload=json.dumps({"job_folder": job_folder})
    )
    response = IngestResponse(invoke_response=inv_response)

    if not response.success:
        print("Ingester lambda call failed with error:\n" f"\t{response.error}\n")
    else:
        print("Ingester successful.")
# pylint: enable=duplicate-code
