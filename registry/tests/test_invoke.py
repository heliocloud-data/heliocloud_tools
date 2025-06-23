import io
import json
import unittest
import datetime
from unittest.mock import patch, Mock

import boto3
from botocore.config import Config
from botocore.stub import Stubber

from registry.lambdas.client.invoke import (
    CatalogerResponse,
    IngestResponse,
    get_function_name,
    RegistryServiceType,
)


class TestResponses(unittest.TestCase):
    """
    Unit tests for the various response types
    """

    # Needed for client initialization
    config = Config(region_name="us-east-1")

    def test_failed_response(self):
        """
        Basic test of a failed response
        """

        # Mock up a client with a payload indicating the lambda function
        # couldn't be invoked
        client = boto3.client("lambda", config=self.config)
        stubber = Stubber(client)
        payload = {"error_message": "error_message", "More": 12}
        invoke_response = {
            "StatusCode": 201,
            "Payload": io.BytesIO(json.dumps(payload).encode("UTF-8")),
        }
        stubber.add_response("invoke", service_response=invoke_response)
        stubber.activate()

        # Check the response for an error
        invoke_response = client.invoke(FunctionName="Cataloger")
        response = CatalogerResponse(invoke_response=invoke_response)
        assert response.success is False
        assert response.error["error_message"] == "error_message"

    def test_success_catalog_response(self):
        """
        Test of a Catalog lambda invocation that was successful.
        """
        client = boto3.client("lambda", config=self.config)
        stubber = Stubber(client)
        payload = {
            "num_endpoints_updated": 2,
            "updates": [
                {"endpoint": "s3://a_bucket", "num_datasets_updated": 10},
                {"endpoint": "s3://b_bucket", "num_datasets_updated": 20},
            ],
        }
        invoke_response = {
            "StatusCode": 200,
            "Payload": io.BytesIO(json.dumps(payload).encode("UTF-8")),
        }
        stubber.add_response("invoke", service_response=invoke_response)
        stubber.activate()

        catalog_response = CatalogerResponse(client.invoke(FunctionName="Cataloger"))

        # Confirm correctness of parsed response
        assert catalog_response.success is True
        assert catalog_response.num_endpoints_updated == 2
        assert len(catalog_response.updates) == 2

    def test_failed_ingest_response(self):
        """
        Test of an Ingest lambda invocation that failed with the handler throwing
        an exception
        """
        client = boto3.client("lambda", config=self.config)
        stubber = Stubber(client)
        payload = {"errorMessage": "list index out of range", "errorType": "IndexError"}
        invoke_response = {
            "StatusCode": 200,
            "FunctionError": "Unhandled",
            "Payload": io.BytesIO(json.dumps(payload).encode("UTF-8")),
        }
        stubber.add_response("invoke", service_response=invoke_response)
        stubber.activate()

        ingester_response = IngestResponse(client.invoke(FunctionName="Ingester"))
        assert ingester_response.success is False
        assert ingester_response.error["errorType"] == "IndexError"
        assert ingester_response.num_datasets_updated == 0
        assert len(ingester_response.updates) == 0

    def test_success_ingest_response(self):
        """
        Test of an Ingest lambda invocation that was successful.
        """
        client = boto3.client("lambda", config=self.config)
        stubber = Stubber(client)
        payload = {
            "num_datasets_updated": 2,
            "updates": [
                {"dataset": "set_1", "num_files_updated": 20, "error": None},
                {"dataset": "set_2", "num_files_updated": 0, "error": "Couldn't find file XYZ"},
            ],
        }
        invoke_response = {
            "StatusCode": 200,
            "Payload": io.BytesIO(json.dumps(payload).encode("UTF-8")),
        }
        stubber.add_response("invoke", service_response=invoke_response)
        stubber.activate()

        ingester_response = IngestResponse(client.invoke(FunctionName="Ingester"))
        assert ingester_response.success
        assert ingester_response.num_datasets_updated == 2
        assert len(ingester_response.updates) == 2

    @patch("boto3.Session")
    def test_get_function_name(self, session: boto3.Session):
        """
        Test for getting function names correctly
        """

        # Setup to only have the Ingester lambda listed
        cfn_client = boto3.client("cloudformation", config=self.config)
        stubber = Stubber(cfn_client)
        # Create mocked responses. Some values are required per boto3 validation
        stacks_response = {
            "StackSummaries": [
                {
                    "StackName": "instancetest",
                    "CreationTime": datetime.datetime(1234, 1, 1),
                    "StackStatus": "NOT_CHECKED_BUT_REQUIRED",
                }
            ]
        }
        resources_response = {
            "StackResources": [
                {
                    "LogicalResourceId": "Ingester",
                    "PhysicalResourceId": "instancetestIngester",
                    "ResourceType": "AWS::Lambda::Function",
                    "Timestamp": datetime.datetime(1234, 1, 1),
                    "ResourceStatus": "NOT_CHECKED_BUT_REQUIRED",
                }
            ]
        }

        # Need 2 responses for each call for our test
        stubber.add_response("list_stacks", service_response=stacks_response)
        stubber.add_response("describe_stack_resources", service_response=resources_response)
        stubber.add_response("list_stacks", service_response=stacks_response)
        stubber.add_response("describe_stack_resources", service_response=resources_response)
        stubber.activate()
        session.client = Mock(return_value=cfn_client)

        # Should find the Ingester function, but not the Cataloger
        ingester_name = get_function_name(RegistryServiceType.INGESTER, "instance-test", session)
        assert ingester_name == "instancetestIngester"
        cataloger_name = get_function_name(RegistryServiceType.CATALOGER, "instance-test", session)
        assert cataloger_name == ""
