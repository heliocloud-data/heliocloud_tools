"""
Defines a DataSet stored in the HelioCloud Registry.
"""

import enum
import json
import re
from dataclasses import dataclass
from datetime import datetime


class FileType(enum.Enum):
    """
    Valid file formats for data set files stored in public s3 buckets.
    File formats are normalized on creation to first listed value.
    """

    FITS = "fits", "fts", "fit"
    CSV = "csv"
    CDF = "cdf"
    NETCDF3 = "netcdf3", "ncd", "netcdf", "ncdf"
    HDF5 = "hdf5", "h5", "hdf"
    DATAMAP = "datamap"
    OTHER = "other"

    # Allows multi value enum on object creation
    def __new__(cls, *values):
        obj = object.__new__(cls)
        # _value_ sets the enum returned value
        obj._value_ = values[0]
        for other_value in values[1:]:
            # secondary values reference to main obj
            cls._value2member_map_[other_value] = obj
        return obj

    @classmethod
    def is_valid_file_type(cls, ext: str) -> bool:
        """
        Checks if a FileType can be made out of a given extension.
        Used so you don't have to catch ValueErrors to check elsewhere.
        """
        try:
            FileType(ext)
            return True
        except ValueError:
            return False


class IndexType(enum.Enum):
    """
    Valid file extensions for index files
    """

    CSV = "csv"
    CSV_ZIP = "csv_zip"
    PARQUET = "parquet"


class DataSetEncoder(json.JSONEncoder):
    """
    Helps with encoding enums in a DataSet instance serialized as strings in JSON format
    """

    def default(self, o):
        if isinstance(o, IndexType):
            return IndexType(o).value
        if isinstance(o, FileType):
            return FileType(o).value
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


@dataclass
class DataSet:  # pylint: disable=too-many-instance-attributes
    """
    Model object describing a DataSet (a collection of related files & metadata) being stored in a
    HelioCloud instance's registry (public S3 buckets).
    """

    # pylint disable=too-many-instance-attributes
    # This is a dataclass describing a DataSet, so it must contain all the necessary fields

    # Required fields

    # Unique identifier for this data set, meeting naming requirements: alphanumeric, dashes
    # and underscores only. NOTE: Will be serialized as "id".
    dataset_id: str

    # A fully qualified pointer to the object directory containing both the dataset and
    # its file registry. It MUST start with 's3://' or 'https://', and terminate in '/'
    index: str

    # A short descriptive title sufficient to identify the dataset and its utility to users
    title: str

    # Restricted ISO 8601 date/time of the first record of data in the entire dataset
    # OR the word 'static' for items such as model shapes that lack a time field
    start: datetime = None

    # Restricted ISO 8601 date/time of the last record of data in the entire dataset
    # OR the word 'static' for items such as model shapes that lack a time field
    stop: datetime = None

    # Restricted ISO 8601 date/time of the last time this dataset was updated
    modification: datetime = None

    # Defines what format the actual file registry file is (one of csv, csv-zip, or parquet)
    indextype: IndexType = IndexType.CSV

    # The file format of the actual data. Must be one of VALID_FILE_FORMATs
    filetype: list[FileType] = None

    # Optional fields

    # Optional descriptive statement about a data set
    description: str = None

    # Identifier - such as a SPASE ID, dataset description URL, DOI, json link of model
    # parameters or similar ancillary information
    resource: str = None

    # ISO 8601 date/time of the dataset creation
    creation: datetime = None

    # ISO 8601 date/time after which the dataset will be expired, migrated or not-maintained
    expiration: datetime = None

    # ISO 8601 date/time for which the dataset was last tested by verifier programs
    verified: datetime = None

    # How to cite this dataset, DOI or similar
    citation: str = None

    # Contact name
    contact: str = None

    # Website URL for info, team, etc
    about: str = None

    # Used when data items in this data set span multiple years
    multiyear: bool = False

    def __setattr__(self, key, value):
        # Validate all the required fields
        if key == "dataset_id":
            if not re.fullmatch(r"^[a-zA-Z0-9_-]*$", str(value)):
                raise ValueError(
                    f"Dataset ID {value} not valid. Must contain only alphanumeric, underscore "
                    f"and dash characters."
                )

        # Confirm index is one of s3:// or https://
        if key == "index":
            if not (str(value).startswith("s3://") or str(value).startswith("https://")):
                raise ValueError(
                    f"Dataset index {value} invalid. Must start with s3:// or https://."
                )

        # Check that start is before stop
        if key == "start" and (self.stop is not None):
            if value > self.stop:
                raise ValueError(f"Dataset start {value} cannot be after stop {self.stop})")

        # Check that stop is after start
        if key == "stop" and (self.start is not None):
            if value < self.start:
                raise ValueError(f"Dataset stop {value} cannot be before start {self.start}.")

        self.__dict__[key] = value

    def to_json(self) -> str:
        """
        Returns this Dataset instance as JSON string.
        """
        dataset_dict = self.__dict__
        dataset_dict["id"] = dataset_dict["dataset_id"]
        del dataset_dict["dataset_id"]
        return json.dumps(dataset_dict, cls=DataSetEncoder)

    def to_serializable_dict(self) -> dict:
        """
        Return a dictionary representation of this DataSet instance,
        using only simple types (str, int, float) for use in serialization.
        """
        dataset_dict = self.__dict__.copy()

        # Serialize Enum & datetime fields to strings
        for key, value in dataset_dict.items():
            if isinstance(value, IndexType):
                dataset_dict[key] = value.value
            if isinstance(value, list):
                if isinstance(value[0], FileType):
                    dataset_dict[key] = [x.value for x in value]
            if isinstance(value, datetime):
                dataset_dict[key] = value.isoformat()

        return dataset_dict

    @staticmethod
    def from_json(dataset_json: str):
        """
        De-serialize a dataset instance from its JSON string representation.
        """
        dataset_dict = json.loads(dataset_json)
        dataset = DataSet(
            dataset_id=dataset_dict["id"], index=dataset_dict["index"], title=dataset_dict["title"]
        )
        for key, value in dataset_dict.items():
            # Already required to instantiate a DataSet
            if key in ("id", "index", "title"):
                continue
            # Enum field
            if key == "indextype":
                dataset.indextype = IndexType(value)
                continue
            # Enum field
            if key == "filetype":
                filetypes = list[FileType]()
                for types in value:
                    filetypes.append(types)
                dataset.filetype = filetypes
                continue
            # Datetime fields
            if key in ("start", "stop", "modification", "creation", "expiration", "verified"):
                dataset.__dict__[key] = datetime.fromisoformat(value) if value is not None else None
                continue
            # Everything else
            dataset.__dict__[key] = value
        return dataset

    @staticmethod
    def from_serialized_dict(dataset_dict: dict):
        """
        Instantiate and return a DataSet instance from its serialized dictionary
        representation.
        """
        dataset_dict["dataset_id"] = (
            dataset_dict["id"] if "id" in dataset_dict else dataset_dict["dataset_id"]
        )
        # Remove any null fields.  These will just be the default values
        # from dataset instantiation
        for key, value in list(dataset_dict.items()):
            if value is None:
                del dataset_dict[key]

        # Dataset instance to populate
        dataset = DataSet(
            dataset_id=dataset_dict["dataset_id"],
            index=dataset_dict["index"],
            title=dataset_dict["title"],
        )
        for key in ("dataset_id", "index", "title"):
            del dataset_dict[key]

        # Now set any remaining attributes
        for key, value in dataset_dict.items():
            if key == "indextype":
                dataset.indextype = IndexType(value)
                continue
            if key == "filetype":
                dataset.filetype = [FileType(val) for val in value]
                continue
            if key in ("start", "stop", "modification", "creation", "expiration", "verified"):
                setattr(dataset, key, datetime.fromisoformat(value))
                continue
            setattr(dataset, key, value)
        return dataset
