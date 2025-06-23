"""
Implements generation of the catalog.json file in a HelioCloud's public S3 buckets.
"""
import json
from collections import OrderedDict
from dataclasses import dataclass, field
import boto3
from ..core import constants
from ..catalog.dataset_repository import DataSetRepository
from ..aws_utils.s3 import get_s3_bucket_name
from ..model.dataset import DataSet


@dataclass
class Result:
    """
    Stores results from Cataloger runs
    """

    # S3 endpoint updated
    endpoint: str  # S3 bucket updated
    num_datasets: int  # of datasets updated in the S3 bucket catalog.json


class Cataloger:  # pylint: disable=too-few-public-methods
    """
    The Cataloger is used to generate catalog.json files placed at the root of the public s3 buckets
    that are part of a HelioCloud instance's Registry module. They are created by traversing a
    DataSetRepository instance for all DataSet entries, grouping them by public s3 bucket name,
    and composing individual catalog.json files for each bucket.

    Currently, implements the CloudMe v.03 specification
    """

    @dataclass
    class Catalog:  # pylint: disable=too-many-instance-attributes
        """
        Internal class to help with generating and validating the head fields of catalog.json file
        """

        Cloudy: str = field(default=0.3, init=False)  # pylint: disable=invalid-name
        endpoint: str  # An accessible S3 (or equivalent) bucket link
        name: str  # Descriptive name for the dataset
        region: str  # Which AWS region hosts it
        contact: str  # Whom to contact for issues with this bucket
        egress: str = "user-pays"  # One of 'no-egress', 'user-pays', 'egress-allowed', 'none'
        status: str = "1200/OK"  # A return code '1200/OK' per spec
        description: str = None  # Optional description of this collection
        citation: str = None  # Optional how to cite, preferably a DOI for the server
        comment: str = None  # A catch-all comment field for data provider and developer use.
        catalog: list[DataSet] = field(default_factory=list)

        def __post_init__(self):
            if self.egress not in ("no-egress", "user-pays", "egress-allowed", "none"):
                raise TypeError("egress must be one of: no-egress, user-pays, egress-allowed, none")

        def to_serializable_dict(self) -> OrderedDict:
            """
            :return: dictionary form of this Catalog instance
            """
            catalog_dict = OrderedDict(self.__dict__)
            catalog_dict["catalog"] = [dataset.to_serializable_dict() for dataset in self.catalog]
            return catalog_dict

    def __init__(
        self,
        session: boto3.session.Session,
        dataset_repository: DataSetRepository,
        name: str,
        contact: str,
    ) -> None:
        """
        Initializes a new Cataloger instance.

        :param session: a boto3.Session instance to use for accessing AWS resources
        :param dataset_repository: an instance of DataSetRepository containing catalog information
        :param name: value to use in the name field of catalog.json
        :param contact: value to use in the contact field of catalog.json
        """
        self.__s3client = session.client("s3")
        self.__dataset_repository = dataset_repository

        # Name and contact information to use when generating the catalog.json
        self.__name = name
        self.__contact = contact

        # List of results to return
        self.__results = list[Result]()

    def __cache_datasets_by_bucket(self) -> dict[str, list[DataSet]]:
        """
        Caches the datasets that must be updated for each bucket
        :return: n/a
        """
        # Internal method to pull all the DataSet instances out of the repository and group them up
        # by s3 bucket name
        datasets = self.__dataset_repository.get_all()
        cache = dict[str, list[DataSet]]()
        for dataset in datasets:
            bucket_name = get_s3_bucket_name(dataset.index)
            if bucket_name not in cache:
                cache[bucket_name] = [dataset]
            else:
                cache[bucket_name].append(dataset)
        return cache

    def __generate_bucket_catalog(self):
        """
        Creates the catalog.json file in each S3 bucket
        :return: n/a
        """

        # Generates a catalog.json for each s3 bucket
        cache = self.__cache_datasets_by_bucket()
        for bucket, datasets in cache.items():
            # Need AWS region for the bucket.
            # Special case: if the constraint is None, its actually a default region
            response = self.__s3client.get_bucket_location(Bucket=bucket)
            if response["LocationConstraint"] is None:
                region = constants.DEFAULT_AWS_REGION
            else:
                region = response["LocationConstraint"]

            # Build the catalog.json
            catalog = self.Catalog(
                endpoint="s3://" + bucket, name=self.__name, region=region, contact=self.__contact
            )
            for dataset in datasets:
                catalog.catalog.append(dataset)

            bucket_catalog_json = json.dumps(catalog.to_serializable_dict(), indent="\t")
            self.__s3client.put_object(
                Bucket=bucket, Body=bytes(bucket_catalog_json, "utf-8"), Key="catalog.json"
            )
            # Store results
            self.__results.append(
                Result(endpoint=catalog.endpoint, num_datasets=len(catalog.catalog))
            )

    def execute(self) -> list[Result]:
        """
        Executes a Cataloger run, resulting in catalog.json files being placed in the root of all
        the AWS s3 buckets found in the DataSets contained in the DataSetRepository instance
        provided.
        :return: list of Result instances, which reach Result containing an endpoint name and the
                 number of datasets updated within it
        """
        self.__generate_bucket_catalog()
        self.__s3client.close()

        return self.__results
