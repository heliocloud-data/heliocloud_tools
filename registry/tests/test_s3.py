import unittest
import json
from unittest.mock import patch, MagicMock

from registry.lambdas.app.aws_utils import s3 as s3_util
from registry.lambdas.app.core.exceptions import RegistryException


class TestS3(unittest.TestCase):
    """
    Unit test for the DataSet S3 Utils
    """

    @patch("boto3.session.Session")
    @patch("botocore.client.BaseClient")
    def test_get_dataset_entries_from_s3_bad_filename(self, client, session) -> None:
        """
        Confirm that the S3 util raises an exception on wrong file extensions.
        """
        with self.assertRaises(RegistryException) as raised:
            s3_util.get_dataset_entries_from_s3(session, "bucket_name", "notajsonfile")

    @patch("boto3.session.Session")
    @patch("botocore.client.BaseClient")
    def test_get_dataset_entries_from_s3_bad_keys(self, client, session) -> None:
        """
        Confirm that the S3 util raises an exception on mismatch entry file keys.
        """
        with open(
            "test/unit/resources/test_registry/ingest/entry/invalid_key.json", encoding="UTF-8"
        ) as entry_f:
            mock_s3_response = {"Body": entry_f}
            client.get_object = MagicMock(return_value=mock_s3_response)
            session.client = MagicMock(return_value=client)

            with self.assertRaises(RegistryException) as raised:
                s3_util.get_dataset_entries_from_s3(session, "bucket_name", "placeholder.json")

    @patch("boto3.session.Session")
    @patch("botocore.client.BaseClient")
    def test_get_dataset_entries_from_s3_multiple_bad_keys(self, client, session) -> None:
        """
        Confirm that the S3 util raises an exception on mismatch entry file keys.
        """
        with open(
            "test/unit/resources/test_registry/ingest/entry/invalid_key_multiple.json",
            encoding="UTF-8",
        ) as entry_f:
            mock_s3_response = {"Body": entry_f}
            client.get_object = MagicMock(return_value=mock_s3_response)
            session.client = MagicMock(return_value=client)

            with self.assertRaises(RegistryException) as raised:
                s3_util.get_dataset_entries_from_s3(session, "bucket_name", "placeholder.json")

    @patch("boto3.session.Session")
    @patch("botocore.client.BaseClient")
    def test_get_dataset_entries_from_s3_valid(self, client, session) -> None:
        """
        Confirm that the S3 util raises an exception on mismatch entry file keys.
        """

        with open(
            "test/unit/resources/test_registry/ingest/entry/valid.json", encoding="UTF-8"
        ) as entry_f:
            mock_s3_response = {"Body": entry_f}

            client.get_object = MagicMock(return_value=mock_s3_response)
            session.client = MagicMock(return_value=client)

            dataset_list = s3_util.get_dataset_entries_from_s3(
                session, "bucket_name", "placeholder.json"
            )
            self.assertEqual(len(dataset_list), 1)

            entry_f.seek(0)
            data = json.load(entry_f)

            self.assertEqual(data[0]["id"], dataset_list[0].dataset_id)

    @patch("boto3.session.Session")
    @patch("botocore.client.BaseClient")
    def test_get_dataset_entries_from_s3_valid_multiple(self, client, session) -> None:
        """
        Confirm that the S3 util raises an exception on mismatch entry file keys.
        """

        with open(
            "test/unit/resources/test_registry/ingest/entry/valid_multiple.json", encoding="UTF-8"
        ) as entry_f:
            mock_s3_response = {"Body": entry_f}
            client.get_object = MagicMock(return_value=mock_s3_response)
            session.client = MagicMock(return_value=client)

            dataset_list = s3_util.get_dataset_entries_from_s3(
                session, "bucket_name", "placeholder.json"
            )
            self.assertEqual(len(dataset_list), 2)
            entry_f.seek(0)
            data = json.load(entry_f)

            for i, dataset in enumerate(dataset_list):
                self.assertEqual(data[i]["id"], dataset.dataset_id)
