"""
A tool for uploading a custom auth ui to cognito.
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
from features.utils.aws_utils import (
    find_user_pool_id_from_stack_name,
    find_cloudformation_stack_name_starts_with,
    str_to_stack_name,
)


def upload_ui_customization(stack, region, image_path, css_path):
    """
    Uploads an image and css file to the specified HelioCloud instance userpool cognito hosted ui.
    """
    session = boto3.Session(region_name=region)

    cfn_client = session.client("cloudformation")

    # Get the userpool id
    name_starts_with = str_to_stack_name(f"{stack}/Auth")
    auth_cloudformation_stack = find_cloudformation_stack_name_starts_with(
        cfn_client, name_starts_with
    )

    user_pool_id = find_user_pool_id_from_stack_name(
        cfn_client, auth_cloudformation_stack["StackName"]
    )

    # Read the css file
    with open(css_path, "r", encoding="utf-8") as css_file:
        css_content = css_file.read()

    # Read the image file
    with open(image_path, "rb") as image_file:
        image_content = image_file.read()

    cognito_client = session.client("cognito-idp")

    # Set the UI customization for the user pool
    cognito_client.set_ui_customization(
        UserPoolId=user_pool_id, ClientId="ALL", CSS=css_content, ImageFile=image_content
    )

    print("UI uploaded successfully.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Cognito Hosted UI customization image uploader.")
    ap.add_argument(
        "-s",
        "--stack",
        type=str,
        required=True,
        help="Stack name. This is the same as the instance file name (no .yaml).",
    )
    ap.add_argument(
        "-r",
        "--region",
        type=str,
        required=True,
        help="Region where the stack exists.",
    )
    ap.add_argument(
        "-i",
        "--image",
        type=str,
        required=True,
        help="Path to logo to upload. Must be an image.",
    )
    ap.add_argument(
        "-c",
        "--css",
        type=str,
        required=True,
        help="Path to css file to upload.",
    )

    args = ap.parse_args()
    upload_ui_customization(*vars(args).values())
