import json
import os
import unittest

import pandas as pd

import botocore.exceptions
from unittest.mock import patch, MagicMock
from registry.lambdas.app.ingest.ingester import Ingester
from registry.lambdas.app.local_utils.entry import get_entries_from_fs
from registry.lambdas.app.ingest.manifest import get_manifest_from_fs
from registry.lambdas.app.aws_utils.s3 import get_bucket_subfolder
from registry.lambdas.app.core.exceptions import IngesterException


class TestIngester(unittest.TestCase):
    """
    Unit test for the ingester class
    """

    entries_local_file = "test/unit/resources/test_registry/ingest/entry/valid.json"
    resource_path = "test/unit/resources/test_registry/ingest/manifest/"
    ingest_bucket = "s3://my_bucket_name"
    ingest_folder = "ingest_folder/"
    client_error = botocore.exceptions.ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "oops"}}, "testing"
    )
    error_template = (
        "Error validating manifest entries. Only 0 records were valid out of 4 files checked.\n"
        + "\tFile: ingest_folder/MMS/mms1/fgm/brst/l2/2015/09/01/mms1_fgm_brst_l2_20150901121114_v4.18.0.{0} - Status: {1}\n"
        + "\tFile: ingest_folder/MMS/mms1/fgm/brst/l2/2015/09/01/mms1_fgm_brst_l2_20150901122054_v4.18.0.{0} - Status: {1}\n"
        + "\tFile: ingest_folder/MMS/mms1/fgm/brst/l2/2015/09/01/mms1_fgm_brst_l2_20150901123044_v4.18.0.{0} - Status: {1}\n"
        + "\tFile: ingest_folder/MMS/mms1/fgm/brst/l2/2015/09/01/mms1_fgm_brst_l2_20150901174500_v4.18.0.{0} - Status: {1}"
    )

    def create_ingester(self, session, ds_repo, manifest_filename="valid.csv"):
        """ """
        manifest_file = TestIngester.resource_path + manifest_filename
        manifest_df = get_manifest_from_fs(manifest_file=manifest_file)
        TestIngester.entry_ds_list = get_entries_from_fs(TestIngester.entries_local_file)
        TestIngester.some_value = 0
        base_ingester = Ingester(
            ingest_bucket=self.ingest_bucket,
            ingest_folder=self.ingest_folder,
            entry_dataset=TestIngester.entry_ds_list[0],
            manifest_df=manifest_df,
            ds_repo=ds_repo,
            session=session,
        )
        return base_ingester

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_destination_client_error(self, session, ds_repo) -> None:
        """
        Catch an IngesterException on a mocked file not found
        """

        # Boto3 throws a client error
        session.client().head_object = MagicMock(side_effect=self.client_error)

        base_ingester = TestIngester.create_ingester(self, session, ds_repo)

        with self.assertRaises(IngesterException):
            base_ingester._Ingester__validate_destination()

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_destination_not_found(self, session, ds_repo) -> None:
        """
        Catch an IngesterException on a destination not found
        """

        # Boto3 returns a 404 status code when a bucket is not accessible
        session.client().head_bucket = MagicMock(
            return_value={"ResponseMetadata": {"HTTPStatusCode": 404}}
        )

        base_ingester = TestIngester.create_ingester(self, session, ds_repo)

        with self.assertRaises(IngesterException):
            base_ingester._Ingester__validate_destination()

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_manifest_file_not_found(self, session, ds_repo) -> None:
        """
        Check record status to confirm file not found
        """
        # A client error is thrown when boto3 can't find a file
        session.client().head_object = MagicMock(side_effect=self.client_error)

        base_ingester = TestIngester.create_ingester(self, session, ds_repo)

        try:
            base_ingester._Ingester__validate_manifest()
        except IngesterException as exception:
            self.assertEqual(exception.message, self.error_template.format("cdf", "NOT_FOUND"))

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_manifest_file_wrong_size(self, session, ds_repo) -> None:
        """
        Check record status to confirm wrong size
        """
        session.client().head_object = MagicMock(
            return_value={"ContentLength": 0, "ResponseMetadata": {"HTTPStatusCode": 200}}
        )

        base_ingester = TestIngester.create_ingester(self, session, ds_repo)

        try:
            base_ingester._Ingester__validate_manifest()
        except IngesterException as exception:
            self.assertEqual(exception.message, self.error_template.format("cdf", "WRONG_SIZE"))

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_manifest_bad_extension(self, session, ds_repo) -> None:
        """
        Check record status to confirm bad extension
        """
        # bad_extension.csv content lengths are set to 1 to avoid manually checking each value
        session.client().head_object = MagicMock(
            return_value={"ContentLength": 1, "ResponseMetadata": {"HTTPStatusCode": 200}}
        )

        base_ingester = TestIngester.create_ingester(
            self, session, ds_repo, manifest_filename="bad_extension.csv"
        )

        try:
            base_ingester._Ingester__validate_manifest()
        except IngesterException as exception:
            self.assertEqual(exception.message, self.error_template.format("bad", "BAD_EXTENSION"))

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_dataset_install(self, session, ds_repo) -> None:
        """
        Check the calling parameters to boto3's copy function
        Assert that the source and destination information are correct
        """

        base_ingester = TestIngester.create_ingester(self, session, ds_repo)

        base_ingester._Ingester__install_dataset()

        self.assertEqual(session.client().copy.call_count, 4)

        manifest_file = TestIngester.resource_path + "valid.csv"
        manifest_df = get_manifest_from_fs(manifest_file=manifest_file)
        manifest_s3_keys = manifest_df["s3key"]

        destination_folder = get_bucket_subfolder(self.entry_ds_list[0].index)

        self.assertEqual(
            session.client().copy.call_args_list[0][1]["CopySource"]["Bucket"], self.ingest_bucket
        )
        self.assertEqual(
            session.client().copy.call_args_list[0][1]["CopySource"]["Key"],
            os.path.join(self.ingest_folder, self.entry_ds_list[0].id, manifest_s3_keys[0]),
        )
        self.assertEqual(
            session.client().copy.call_args_list[0][1]["Key"],
            os.path.join(destination_folder, manifest_s3_keys[0]),
        )

    # install_index_files
    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_install_index_files(self, session, ds_repo) -> None:
        """
        Check the calling parameters to boto3's put_object function
        Assert that the destination information is correct
        """

        base_ingester = TestIngester.create_ingester(self, session, ds_repo)

        # install_index_files depends on install_dataset to be called
        base_ingester._Ingester__install_dataset()
        base_ingester._Ingester__install_index_files()

        self.assertEqual(session.client().put_object.call_count, 1)

        # manually check these to avoid duplicating tested code
        self.assertEqual(
            session.client().put_object.call_args_list[0][1]["Bucket"],
            "test",
        )
        self.assertEqual(
            session.client().put_object.call_args_list[0][1]["Key"],
            "base_data/resources/ingest/dataset_bucket/MMS_2015.csv",
        )
        self.assertEqual(
            session.client().put_object.call_args_list[0][1]["Body"].name, "/tmp/MMS_2015.csv"
        )

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_install_index_files_multiple_years(self, session, ds_repo) -> None:
        """
        Check the calling parameters to boto3's put_object function when multiple years are found
        Assert that the destination information is correct
        """

        base_ingester = TestIngester.create_ingester(
            self, session, ds_repo, manifest_filename="valid_multiple_years.csv"
        )

        # install_index_files depends on install_dataset to be called
        base_ingester._Ingester__install_dataset()
        base_ingester._Ingester__install_index_files()

        self.assertEqual(session.client().put_object.call_count, 2)

        args_list = session.client().put_object.call_args_list

        # manually check these to avoid duplicating tested code
        self.assertEqual(
            args_list[0][1]["Bucket"],
            "test",
        )
        self.assertEqual(
            args_list[0][1]["Key"], "base_data/resources/ingest/dataset_bucket/MMS_2015.csv"
        )
        self.assertEqual(args_list[0][1]["Body"].name, "/tmp/MMS_2015.csv")

        self.assertEqual(
            args_list[1][1]["Bucket"],
            "test",
        )
        self.assertEqual(
            args_list[1][1]["Key"], "base_data/resources/ingest/dataset_bucket/MMS_2016.csv"
        )
        self.assertEqual(args_list[1][1]["Body"].name, "/tmp/MMS_2016.csv")

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test_update_catalog(self, session, ds_repo) -> None:
        """
        Check the calling parameters to the heliocloud data set repo
        Assert that the entry file saved is correct
        """

        base_ingester = TestIngester.create_ingester(self, session, ds_repo)

        base_ingester._Ingester__update_catalog()

        args, _ = ds_repo.save.call_args

        self.assertEqual(args[0], self.entry_ds_list)

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    def test__clean_up(self, session, ds_repo) -> None:
        """
        Check the calling parameters to boto3's delete_object function
        Assert that the keys are correct
        """

        base_ingester = TestIngester.create_ingester(self, session, ds_repo)

        base_ingester._Ingester__clean_up()

        manifest_file = TestIngester.resource_path + "valid.csv"
        manifest_df = get_manifest_from_fs(manifest_file=manifest_file)
        manifest_s3_keys = manifest_df["s3key"]

        for i, call in enumerate(session.client().delete_object.call_args_list):
            _, kwargs = call
            self.assertEqual(
                kwargs["Key"],
                os.path.join(self.ingest_folder, self.entry_ds_list[0].id, manifest_s3_keys[i]),
            )
