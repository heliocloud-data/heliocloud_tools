"""
Functions and classes to assist client applications in invoking the Registry's
AWS Lambdas.
"""
import enum
import json
from abc import ABC
from dataclasses import dataclass, field
from typing import Any

import boto3

from features.utils.aws_utils import find_cloudformation_stack_name_starts_with


class RegistryServiceType(enum.Enum):
    """
    Enumeration for the Registry services that can be invoked
    """

    INGESTER = "Ingester"
    CATALOGER = "Cataloger"


@dataclass
class BaseResponse(ABC):
    """
    Abstract base class for handling responses from HelioCloud registry
    services running as AWS Lambdas.
    """

    # Response indicates success or not
    success: bool = field(default=False)

    # If there was a failure, an error object will have been stored in the response
    error: Any = None

    def __init__(self, invoke_response: dict):
        """
        Initialize a response
        """
        self.invoke_response = invoke_response

        # If the status code is not 200, then the payload field contains
        # a error object (probably a dict) to capture
        if invoke_response["StatusCode"] != 200:
            self.success = False
            self.error = json.loads(invoke_response["Payload"].read())

        # The lambda was started, but threw an error that ended its execution
        elif "FunctionError" in invoke_response:
            self.success = False
            self.error = json.loads(invoke_response["Payload"].read())
        else:
            self.success = True


@dataclass
class CatalogerResponse(BaseResponse):
    """
    Response object for understanding the results of invoking the Cataloger lambda.
    """

    # Number of endpoints updated
    num_endpoints_updated: int = 0

    # Updates applied
    updates: [list[dict[str, int]]] = field(default_factory=list)

    def __init__(self, invoke_response: dict):
        super().__init__(invoke_response)

        # Have to init to ensure the field exists, because we implemented init()
        self.updates = []

        if self.success:
            payload = json.loads(invoke_response["Payload"].read())
            self.num_endpoints_updated = payload["num_endpoints_updated"]
            self.updates = payload["updates"]


@dataclass
class IngestResponse(BaseResponse):
    """
    Response object for understanding the results of invoking the Ingest lambda
    """

    # Number of datasets updated
    num_datasets_updated: int = 0

    # Updates
    updates: [list[dict[str, int, str]]] = field(default_factory=list)

    def __init__(self, invoke_response: dict):
        super().__init__(invoke_response)

        # Have to init to ensure the field exists, because we implemented init()
        self.updates = []

        if self.success:
            payload = json.loads(invoke_response["Payload"].read())
            self.num_datasets_updated = payload["num_datasets_updated"]
            self.updates = payload["updates"]


def get_function_name(
    service_type: RegistryServiceType, hc_instance: str, session: boto3.session
) -> str:
    """
    Gets the name of the AWS Lambda from AWS for the requested Registry service,
    as required for invoking the AWS Lambda service through boto3.

    :param service_type: type of Registry service to look up
    :param hc_instance: name of the heliocloud instance to look up the function in
    :param session: an initialized boto3 session to use to query AWS with
    :return: the function name of the lambda from AWS, empty string if one is not found
    """

    cfn_client = session.client("cloudformation")

    stack_name = find_cloudformation_stack_name_starts_with(
        cfn_client, hc_instance.replace("-", "")
    )["StackName"]

    resources = cfn_client.describe_stack_resources(StackName=stack_name)

    for resource in resources["StackResources"]:
        if (
            resource["LogicalResourceId"].startswith(service_type.value)
            and resource["ResourceType"] == "AWS::Lambda::Function"
        ):
            return resource["PhysicalResourceId"]

    # Couldn't find the function name
    return ""


def get_cataloger_function(hc_instance: str, region: str) -> str:
    """
    Returns the function name of the Cataloger in AWS's Lambda service.

    :param hc_instance: name of the HelioCloud instance to refer to
    :param region: name of the AWS region to do the lookup in
    :return: function name of the Cataloger service in AWS Lambda
    """
    return get_function_name(
        RegistryServiceType.CATALOGER, hc_instance, boto3.Session(region_name=region)
    )


def get_ingester_function(hc_instance: str, region: str) -> str:
    """
    Returns the function name of the Ingester in AWS's Lambda service.

    :param hc_instance: name of Heliocloud instance to refer to
    :param region: name of the AWS region to do the lookup in
    :return: function name of the Ingester service in AWS Lambda
    """
    return get_function_name(
        RegistryServiceType.INGESTER, hc_instance, boto3.Session(region_name=region)
    )
