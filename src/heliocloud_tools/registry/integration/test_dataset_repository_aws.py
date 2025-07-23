from datetime import datetime
import unittest
import pymongo
import pytest as pytest

from registry.lambdas.app.catalog.dataset_repository import DataSetRepository
from registry.lambdas.app.model.dataset import DataSet, IndexType, FileType


@pytest.mark.skip("Requires a running DocumentDB instance.")
class TestDataSetRepositoryAWS(unittest.TestCase):
    """
    Tests the DataSetRepository implemented as backed by a DocumentDB instance running on AWS.
    Assumes there is a local SSH tunnel to facilitate access to the DocumentDB instance.
    """

    # Connecting to the DocumentDB instance.
    hostname = "localhost"
    port = 27017
    global_pem = "registry/lambdas/app/resources/global-bundle.pem"
    username = "hc_master"
    password = "password"

    # Database name to use for these tests
    db_name = "test_dsrepository"

    def setUp(self) -> None:
        # Get a Db handle
        self.db_client = pymongo.MongoClient(
            TestDataSetRepositoryAWS.hostname,
            port=TestDataSetRepositoryAWS.port,
            username=TestDataSetRepositoryAWS.username,
            password=TestDataSetRepositoryAWS.password,
            tls=True,
            tlsAllowInvalidCertificates=True,
            tlsAllowInvalidHostnames=True,
            tlsCAFile=TestDataSetRepositoryAWS.global_pem,
            directConnection=True,
            retryWrites=False,
        )

        # Clear out the datasets collection
        self.db_client.drop_database(TestDataSetRepositoryAWS.db_name)

    def tearDown(self) -> None:
        self.db_client.drop_database(TestDataSetRepositoryAWS.db_name)
        self.db_client.close()

    def test_single_dataset(self):
        saved_ds = DataSet(
            dataset_id="1234_ABC", index="s3://my.dataset.bucket/MMS", title="MMS_data"
        )
        saved_ds.start = datetime(year=2015, month=1, day=12, hour=9, minute=30, second=30)
        saved_ds.stop = datetime(year=2018, month=12, day=31, hour=12, minute=30, second=15)
        saved_ds.modification = datetime(year=2023, month=5, day=17, hour=15, minute=30, second=15)
        saved_ds.indextype = IndexType.CSV
        saved_ds.filetype = [FileType.CSV, FileType.CDF, FileType.NETCDF3]
        saved_ds.resource = "SPASE 1234567"

        # Save it and find it again
        ds_repo = DataSetRepository(
            db_client=self.db_client, db_name=TestDataSetRepositoryAWS.db_name
        )
        ds_repo.save([saved_ds])
        found_ds = ds_repo.get_by_dataset_id(dataset_id=saved_ds.dataset_id)

        # Validate it
        self.assertEqual(saved_ds.dataset_id, found_ds.dataset_id)
        self.assertEqual(saved_ds.start, found_ds.start)
        self.assertEqual(saved_ds.stop, found_ds.stop)
        self.assertEqual(saved_ds.index, found_ds.index)
        self.assertEqual(saved_ds.filetype, found_ds.filetype)
        self.assertEqual(saved_ds.resource, found_ds.resource)

        # Delete it & validate it doesn't exist anymore
        ds_repo.delete_by_dataset_id(dataset_id=saved_ds.dataset_id)
        result = ds_repo.get_by_dataset_id(dataset_id=saved_ds.dataset_id)
        self.assertIsNone(result)

    def test_multiple_datasets(self):
        ds_repo = DataSetRepository(
            db_client=self.db_client, db_name=TestDataSetRepositoryAWS.db_name
        )

        # Make 3 DataSet instances and save them
        datasets = list[DataSet]()
        for dataset_id in ["SET1", "SET2", "SET3"]:
            dataset = DataSet(
                dataset_id=dataset_id,
                index="s3://my.dataset.bucket/MMS",
                title=f"Data for set {dataset_id}",
            )
            dataset.start = datetime(year=2023, month=5, day=12, hour=9, minute=30, second=6)
            dataset.stop = datetime(year=2023, month=12, day=12, hour=9, minute=30, second=6)
            dataset.modification = datetime.now()
            dataset.indextype = IndexType.CSV_ZIP
            dataset.filetype = [FileType.CDF, FileType.DATAMAP]
            dataset.contact = "Dr. SoNSo"
            datasets.append(dataset)
        count_saved = ds_repo.save(datasets)
        self.assertEqual(count_saved, len(datasets))

        # Get all 3 datasets back, and check a field
        found_datasets = ds_repo.get_all()
        self.assertEqual(len(datasets), len(found_datasets))
        for dataset in datasets:
            if dataset.dataset_id == "SET1":
                for found_dataset in found_datasets:
                    if found_dataset.dataset_id == "SET1":
                        self.assertEqual(dataset.index, found_dataset.index)

        # Delete the datasets and confirm the repository is empty
        count_deleted = ds_repo.delete_all()
        self.assertEqual(count_deleted, 3)
