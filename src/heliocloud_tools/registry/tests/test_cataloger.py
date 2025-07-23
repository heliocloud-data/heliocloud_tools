import json
import unittest
from unittest.mock import patch, MagicMock
from registry.lambdas.app.catalog.cataloger import Cataloger
from registry.lambdas.app.model.dataset import DataSet


class TestCataloger(unittest.TestCase):
    """
    Unit test for the cataloger class
    """

    @patch("registry.lambdas.app.catalog.dataset_repository.DataSetRepository")
    @patch("boto3.Session")
    @patch("botocore.client.BaseClient")
    def test_generate_catalog_json(self, ds_repo, session, client) -> None:
        """
        Confirm catalog.json file is generated correctly.
        :return: none
        """
        # setup dataset repo
        a_dataset = DataSet(dataset_id="Set_a", index="s3://bucket1/set_a", title="Dataset a")
        b_dataset = DataSet(dataset_id="Set_b", index="s3://bucket2/set_b", title="Dataset b")
        c_dataset = DataSet(dataset_id="Set_c", index="s3://bucket2/set_c", title="Dataset c")
        ds_repo.get_all.return_value = [a_dataset, b_dataset, c_dataset]

        # setup boto3 session & client
        client.get_bucket_location = MagicMock(return_value={"LocationConstraint": None})
        client.put_object = MagicMock(return_value=None)
        session.client = MagicMock(return_value=client)

        # run the cataloger
        cataloger = Cataloger(
            dataset_repository=ds_repo, session=session, name="tester", contact="tester@domain.org"
        )
        results = cataloger.execute()

        # Check the results
        self.assertEqual(len(results), 2)
        for result in results:
            if result.endpoint == """s3://bucket1""":
                self.assertEqual(result.num_datasets, 1)
            elif result.endpoint == """s3://bucket2""":
                self.assertEqual(result.num_datasets, 2)
            else:
                self.assertTrue(False, f"Incorrect dataset information: {result.endpoint}")

        # Check the catalog.json created for bucket 2
        args = list(
            filter(
                lambda args: args.kwargs["Bucket"] == "bucket2", client.put_object.call_args_list
            )
        )
        self.assertEqual(len(args), 1)
        body = args[0].kwargs["Body"]
        catalog_json = json.loads(body.decode())
        self.assertIsNotNone(catalog_json, "Catalog.json was not produced correctly for bucket2.")
        self.assertEqual(catalog_json["endpoint"], "s3://bucket2")
        self.assertEqual(catalog_json["name"], "tester")
        self.assertEqual(catalog_json["region"], "us-east-1")
        self.assertEqual(catalog_json["contact"], "tester@domain.org")
        self.assertEqual(len(catalog_json["catalog"]), 2)

        # Check for dataset c in the catalog.json and check its contents
        set_c = list(
            filter(lambda dataset: dataset["dataset_id"] == "Set_c", catalog_json["catalog"])
        )
        self.assertEqual(len(set_c), 1)
        set_c = set_c[0]
        self.assertEqual(c_dataset.title, "Dataset c")
        self.assertEqual(c_dataset.index, "s3://bucket2/set_c")
