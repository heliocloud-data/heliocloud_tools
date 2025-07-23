"""
Utility methods for tests that may use the local file system.
"""
import json

from ..model.dataset import DataSet
from ..core.exceptions import IngesterException, RegistryException


def get_entries_from_fs(filename: str) -> list[DataSet]:
    """
    Load the entries.json file from the local file system, returning a Dataset instance.
    :param filename: fully qualified path to entries.json
    :return: a DataSet instance created from the contents of entries.json
    """

    # Check file extension
    if not filename.endswith("json"):
        raise IngesterException(
            "Upload entry file " + filename + " does not have a JSON extension."
        )

    with open(filename, encoding="UTF-8") as entry_f:
        data = json.load(entry_f)

    dataset_list = []
    for dataset in data:
        try:
            dataset_list.append(DataSet.from_serialized_dict(dataset))
        except KeyError as err:
            raise RegistryException from err

    return dataset_list
