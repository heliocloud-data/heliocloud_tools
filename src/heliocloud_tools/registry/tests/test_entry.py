import datetime
import unittest

from registry.lambdas.app.local_utils.entry import get_entries_from_fs
from registry.lambdas.app.core.exceptions import IngesterException, RegistryException


class TestEntry(unittest.TestCase):
    def test_upload_entry_not_json(self):
        filename = "test/unit/resources/test_registry/ingest/entry/not_json.txt"
        with self.assertRaises(IngesterException) as raised:
            entry = get_entries_from_fs(filename=filename)

    def test_upload_entry_bad_keys(self):
        filename = "test/unit/resources/test_registry/ingest/entry/invalid_key.json"
        with self.assertRaises(RegistryException) as raised:
            entry = get_entries_from_fs(filename=filename)

    def test_upload_entries_valid(self):
        filename = "test/unit/resources/test_registry/ingest/entry/valid.json"
        dataset = get_entries_from_fs(filename=filename)
        self.assertEqual(dataset[0].dataset_id, "MMS")
        self.assertEqual(
            dataset[0].creation,
            datetime.datetime(year=2015, month=9, day=1, hour=0, minute=0, second=0),
        )
        self.assertEqual(dataset[0].resource, "SPASE-1234567")
