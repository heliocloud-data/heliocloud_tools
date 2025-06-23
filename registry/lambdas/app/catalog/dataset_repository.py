"""
Repository implementation for storing a HelioCloud's Registry catalog.
"""
from __future__ import annotations
from typing import Any
from pymongo import MongoClient
from ..model.dataset import DataSet


class DataSetRepository:
    """
    For persisting dataset records in the catalog database.
    """

    def __init__(self, db_client: MongoClient, db_name="catalog", collection="datasets"):
        """
        Initialize a DataSetRepository instance connected to the
        AWS DocumentDB instance in db_client.
        :param db_client: MongoClient instance connected to the AWS DocumentDB instance to use
        :param db_name: name of the database storing the catalog in DocumentDB (default: catalog)
        :param collection: name of the collecting storing the datasets in DocumentDB
        """

        self.__catalog_db = db_client.get_database(db_name)
        self.__datasets_collection = self.__catalog_db.get_collection(collection)

    def save(self, datasets: list[DataSet]) -> int:
        """
        Save a list of Datasets to the repository. Returns number of documents saved successfully.

        Returns: Count of documents saved.
        """
        saved = 0
        for dataset in datasets:
            result = self.__datasets_collection.replace_one(
                filter={"_id": dataset.dataset_id},
                replacement=DataSetRepository.__dataset_to_dict(dataset),
                upsert=True,
            )
            # If an object id came back, it was an insert
            if result.upserted_id is not None:
                saved += 1
            # Otherwise, a document should have been matched and updated
            else:
                saved += result.matched_count
        return saved

    def get_by_dataset_id(self, dataset_id: str) -> DataSet | None:
        """
        Return a single dataset instance found by its dataset_id
        """
        result = self.__datasets_collection.find_one(filter={"_id": dataset_id})
        return None if result is None else DataSetRepository.__dataset_from_dict(result)

    def get_all(self) -> list[DataSet] | None:
        """
        Return all the datasets in the catalog. Returns None if the catalog is empty.
        """
        results = self.__datasets_collection.find()
        return (
            None
            if results is None
            else [DataSetRepository.__dataset_from_dict(result) for result in results]
        )

    def delete_by_dataset_id(self, dataset_id: str) -> bool:
        """
        Delete a dataset document by id.
        Returns True if successful, else False
        """
        delete_result = self.__datasets_collection.delete_one(filter={"_id": dataset_id})
        return delete_result == 1

    def delete_all(self) -> int | None:
        """
        Deletes all Datasets in the repository.
        If documents are deleted, a count is returned, else None.
        """
        delete_result = self.__datasets_collection.delete_many(
            filter={},
        )
        return None if delete_result is None else delete_result.deleted_count

    @staticmethod
    def __dataset_to_dict(dataset: DataSet) -> dict[str, Any]:
        """
        Convert the dataset to a dictionary representation so it can be saved
        in the repository.
        """
        dataset_dict = dataset.to_serializable_dict()

        # Delete any empty fields.  We aren't going to persist them
        for key, value in list(dataset_dict.items()):
            if value is None:
                del dataset_dict[key]

        # Intentionally set the _id field of the document to insert
        # to the dataset_id. This ensures:
        #    - There is only 1 document in the dataset collection per dataset
        #    - A dataset document can quickly be found by its dataset_id thanks to the index on _id
        dataset_dict["_id"] = dataset.dataset_id

        return dataset_dict

    @staticmethod
    def __dataset_from_dict(dataset_dict: dict) -> DataSet:
        """
        Instantiate and return a DataSet instance from the dictionary
        retrieved from DocumentDB
        """
        # Don't need this field from Document DB
        del dataset_dict["_id"]
        return DataSet.from_serialized_dict(dataset_dict)
