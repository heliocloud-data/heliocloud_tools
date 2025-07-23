import unittest

from registry.lambdas.app.core.exceptions import IngesterException
from registry.lambdas.app.ingest.manifest import get_manifest_from_fs


class TestManifest(unittest.TestCase):
    """
    Various tests for correct manifest parsing.
    """

    resource_path = "test/unit/resources/test_registry/ingest/manifest/"

    def test_unrecognized_manifest(self):
        manifest_file = TestManifest.resource_path + "manifest.txt"
        with self.assertRaises(IngesterException) as raised:
            get_manifest_from_fs(manifest_file=manifest_file)

    def test_manifest_missing_header(self):
        manifest_file = TestManifest.resource_path + "missing_header.csv"
        with self.assertRaises(IngesterException) as raised:
            get_manifest_from_fs(manifest_file)

    def test_manifest_bad_data_type(self):
        manifest_file = TestManifest.resource_path + "bad_data.csv"
        with self.assertRaises(IngesterException) as raised:
            get_manifest_from_fs(manifest_file=manifest_file)

    def test_get_valid_manifest(self):
        manifest_file = TestManifest.resource_path + "valid.csv"
        manifest_df = get_manifest_from_fs(manifest_file=manifest_file)
        self.assertEqual(manifest_df.columns[0], "time")
