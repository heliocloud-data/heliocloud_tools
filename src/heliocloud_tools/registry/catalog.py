"""
Simple script for invoking the HelioCloud registry's Cataloger service from the command line.
"""
import argparse
import sys
from pathlib import Path

import boto3

# pylint: disable=import-error, wrong-import-position
# The catalog_lambda module is in a separate, top level project directory unaffiliated with tools,
# thus importing it requires updating the sys.path with the project directory.
PROJECT_DIR = str((Path(__file__)).parent.parent)
sys.path.append(PROJECT_DIR)
from registry.lambdas.client.invoke import CatalogerResponse, get_cataloger_function

# pylint: enable=import-error, wrong-import-position

# pylint: disable-duplicate-code
if __name__ == "__main__":
    # Require an instance name argument
    parser = argparse.ArgumentParser(
        prog="HelioCloud Cataloger Service Runner",
        description="Runs a HelioCloud instance's cataloger service to generate catalog.json "
        "files for the registry S3 buckets.",
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
        help="Name of the HelioCloud instance whose Cataloger service should be run. "
        "This should match the instance configuration used to deploy the "
        "HelioCloud to AWS.",
    )

    # Get the args out.  Note that heliocloud instance names need any hyphens stripped
    # because of how
    args = parser.parse_args()
    region = args.region
    instance = args.instance

    # Get the name of the Cataloger AWS Lambda
    # Note: Already tested elsewhere
    FUNCTION = get_cataloger_function(hc_instance=instance, region=region)
    if FUNCTION is None or FUNCTION == "":
        print(
            f"No AWS Lambda function name found for the Cataloger service in the {instance} "
            f"HelioCloud instance.\n"
            "Are you sure the parameters are correct and/or the HelioCloud instance is "
            "correctly deployed?"
        )
        sys.exit()

    print(
        f"Invoking HelioCloud Cataloger service with parameters\n"
        f"\tAWS Region: {region}\n"
        f"\tInstance: {instance}\n"
        f"\tLambda function name: {FUNCTION}\n"
    )

    # Get a lambda client and run the function
    client = boto3.Session(region_name=region).client("lambda")
    inv_response = client.invoke(FunctionName=FUNCTION, Payload=bytes({}))
    response = CatalogerResponse(invoke_response=inv_response)

    print("Received response:\n" f"\t{response}")

    # If then for Cataloger running successfully
# pylint: enable=duplicate-code
