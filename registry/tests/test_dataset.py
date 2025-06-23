import unittest
from datetime import datetime

from registry.lambdas.app.model.dataset import DataSet, FileType, IndexType


class TestDataSet(unittest.TestCase):
    dataset_id = "ABC_123"
    index = "s3://public_bucket/data_set_dir/"
    title = "My MMS Data"
    start = datetime(year=2015, month=9, day=1, hour=12, minute=11, second=14)
    stop = datetime(year=2015, month=10, day=1, hour=12, minute=30, second=5)
    modification = datetime(year=2023, month=5, day=17, minute=30, second=5)
    filetype = [FileType.CSV, FileType.CDF]
    indextype = IndexType.CSV
    ext_list = [
        ["fits", "fts", "fit"],
        ["csv"],
        ["cdf"],
        ["netcdf3", "ncd", "netcdf", "ncdf"],
        ["hdf5", "h5", "hdf"],
        ["datamap"],
        ["other"],
    ]

    def test_invalid_id(self):
        with self.assertRaises(ValueError) as raised:
            DataSet(dataset_id="12345_&", index=TestDataSet.index, title=TestDataSet.title)

    def test_invalid_index(self):
        with self.assertRaises(ValueError) as raised:
            DataSet(
                dataset_id=TestDataSet.dataset_id, index="ftp://something", title=TestDataSet.title
            )

    def test_invalid_start_stop(self):
        start = datetime(year=2015, month=9, day=1, hour=12, minute=11, second=14)
        stop = datetime(year=2015, month=9, day=1, hour=10, minute=45, second=00)
        with self.assertRaises(ValueError) as raised:
            dataset = DataSet(dataset_id="456778", index=TestDataSet.index, title="")
            dataset.start = start
            dataset.stop = stop

    def test_dataset_serialized_dict(self):
        dataset = DataSet(
            dataset_id=TestDataSet.dataset_id,
            index=TestDataSet.index,
            title=TestDataSet.title,
            start=TestDataSet.start,
            stop=TestDataSet.stop,
            modification=TestDataSet.modification,
            filetype=TestDataSet.filetype,
            indextype=TestDataSet.indextype,
            citation="Cited by XYZ",
        )

        dataset_dict = dataset.to_serializable_dict()
        self.assertEqual(dataset_dict["dataset_id"], dataset.dataset_id)

        restored_dataset = DataSet.from_serialized_dict(dataset_dict)
        self.assertEqual(dataset.dataset_id, restored_dataset.dataset_id)
        self.assertEqual(dataset.filetype, restored_dataset.filetype)
        self.assertEqual(dataset.indextype, restored_dataset.indextype)
        self.assertEqual(dataset.start, restored_dataset.start)

    def test_filetype_normalize(self):
        for exts in self.ext_list:
            # Create a FileType for each extension
            # Convert map to set - removes duplicates
            ext_enum_set = set(map(FileType, exts))

            self.assertEqual(len(ext_enum_set), 1)

            # Set upack
            (ext_enum,) = ext_enum_set

            self.assertEqual(ext_enum.value, exts[0])

    def test_filetype_valid(self):
        for exts in self.ext_list:
            for ext in exts:
                self.assertTrue(FileType.is_valid_file_type(ext))

    def test_filetype_bad_ext(self):
        bad_extension = "badextension"
        with self.assertRaises(ValueError) as raised:
            FileType(bad_extension)

        self.assertFalse(FileType.is_valid_file_type(bad_extension))
