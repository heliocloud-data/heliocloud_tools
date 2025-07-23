import inspect
import json
import unittest

import aws_cdk as cdk
from aws_cdk.assertions import Template, Match

from app_config import load_configs
from base_aws.base_aws_stack import BaseAwsStack
from registry.lambdas.app.core.constants import (
    DEFAULT_PANDA_LAYERS_ARN,
)
from registry.registry_stack import RegistryStack
from utils import create_dumpfile


class TestRegistryStack(unittest.TestCase):
    def test_default_synthesis(self) -> None:
        """
        Test synthesis of a Registry stack instance using a default HelioCloud instance
        configuration.
        """

        # Startup a CDK app and load the default HelioCloud config
        app = cdk.App()
        env = cdk.Environment(region="us-east1", account="unit-test")
        cfg = load_configs()
        cfg["registry"]["ingestBucketName"] = "ingest"
        cfg["registry"]["datasetBucketNames"] = ["bucket1", "bucket2"]

        # Stack dependencies
        aws_stack = BaseAwsStack(app, "Base-Portal-Test", description="", config=cfg, env=env)
        registry_stack = RegistryStack(
            app, "Registry-Test", description="", config=cfg, env=env, base_aws_stack=aws_stack
        )

        # Generate the template and dump a copy of it for inspection if needed
        template = Template.from_stack(registry_stack)
        create_dumpfile(
            test_class=self.__class__.__name__,
            test_name=inspect.currentframe().f_code.co_name,
            data=json.dumps(template.to_json(), indent=2),
        )

        # Check for the ingest bucket
        template.has_resource(
            "AWS::S3::Bucket", {"DeletionPolicy": "Delete", "Properties": {"BucketName": "ingest"}}
        )

        # Check for the dataset buckets
        template.has_resource(
            "AWS::S3::Bucket", {"DeletionPolicy": "Retain", "Properties": {"BucketName": "bucket1"}}
        )
        template.has_resource(
            "AWS::S3::Bucket", {"DeletionPolicy": "Retain", "Properties": {"BucketName": "bucket2"}}
        )

        # Check for Ingester & Cataloger lambdas
        # w/ default layers
        template.has_resource(
            type="AWS::Lambda::Function",
            props={
                "Properties": {
                    "Handler": "app.ingest_lambda.handler",
                    "Layers": Match.array_with([DEFAULT_PANDA_LAYERS_ARN]),
                }
            },
        )
        template.has_resource(
            type="AWS::Lambda::Function",
            props={
                "Properties": {
                    "Handler": "app.catalog_lambda.handler",
                    "Layers": Match.array_with([DEFAULT_PANDA_LAYERS_ARN]),
                }
            },
        )

        # Check for the Catalog database
        template.has_resource(
            type="AWS::DocDB::DBCluster",
            props={"Properties": {"DeletionProtection": True}, "DeletionPolicy": "Retain"},
        )

    def test_destroy_on_removal(self):
        """
        Check that resources declared by this stack are set correctly for destruction on removal.
        """

        # Startup a CDK app and load the default HelioCloud config
        app = cdk.App()
        env = cdk.Environment(region="us-east1", account="unit-test")
        cfg = load_configs()
        cfg["registry"]["ingestBucketName"] = "ingest"
        cfg["registry"]["datasetBucketNames"] = ["bucket1"]
        cfg["registry"]["destroyOnRemoval"] = True

        # Stack dependencies
        aws_stack = BaseAwsStack(app, "Base-Portal-Test", description="", config=cfg, env=env)
        registry_stack = RegistryStack(
            app, "Registry-Test", description="", config=cfg, env=env, base_aws_stack=aws_stack
        )

        # Generate the template and dump a copy of it for inspection if needed
        template = Template.from_stack(registry_stack)
        create_dumpfile(
            test_class=self.__class__.__name__,
            test_name=inspect.currentframe().f_code.co_name,
            data=json.dumps(template.to_json(), indent=2),
        )

        # Catalog DB should be deleted
        template.has_resource(
            type="AWS::DocDB::DBCluster",
            props={"Properties": {"DeletionProtection": False}, "DeletionPolicy": "Delete"},
        )

        # Dataset bucket should be deleted
        template.has_resource(
            "AWS::S3::Bucket", {"DeletionPolicy": "Delete", "Properties": {"BucketName": "bucket1"}}
        )

    def test_pandas_layer_set(self) -> None:
        """
        Test specifying the Pandas AWS Lambda Layer to use.
        """
        # Startup a CDK app and load the default HelioCloud config
        app = cdk.App()
        env = cdk.Environment(region="us-east1", account="unit-test")
        cfg = load_configs()

        # Provide required overrides
        cfg["registry"]["ingestBucketName"] = "ingest"
        cfg["registry"]["datasetBucketNames"] = ["bucket1"]

        # Set the Pandas ARN
        pandas_arn = "arn:aws:lambda:pkm-alola-1:000000000760:layer:BEWEAR-Python39:6"
        cfg["registry"]["layers"] = {"pandas": pandas_arn}

        # Generate the template and dump a copy of it for inspection if needed
        aws_stack = BaseAwsStack(app, "Base-Portal-Test", description="", config=cfg, env=env)
        registry_stack = RegistryStack(
            app, "Registry-Test", description="", config=cfg, env=env, base_aws_stack=aws_stack
        )
        template = Template.from_stack(registry_stack)
        create_dumpfile(
            test_class=self.__class__.__name__,
            test_name=inspect.currentframe().f_code.co_name,
            data=json.dumps(template.to_json(), indent=2),
        )

        # Check that the specified pandas ARN was used
        template.has_resource(
            type="AWS::Lambda::Function",
            props={
                "Properties": {
                    "Handler": "app.ingest_lambda.handler",
                    "Layers": Match.array_with([pandas_arn]),
                }
            },
        )
