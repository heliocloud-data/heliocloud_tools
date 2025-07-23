"""
Helper methods for creating a manifest dataframe.
"""

import pandas as pd

from ..core.exceptions import IngesterException


def get_manifest_from_fs(manifest_file: str) -> pd.DataFrame:
    """
    Constructs a manifest instance from a file on the local filesystem
    """
    if not manifest_file.endswith(".csv"):
        raise IngesterException(f"Expecting .csv extension for manifest file: {manifest_file}.")

    # Read the manifest in line by line
    manifest_lines = []
    with open(manifest_file, encoding="UTF-8") as manifest:
        for line in manifest:
            manifest_lines.append(line.strip().split(","))

    # Hand it off to build the manifest Dataframe
    return build_manifest_df(manifest_lines)


def build_manifest_df(manifest: list[list[str]]) -> pd.DataFrame:
    """
    Return the manifest as a Panda's DataFrame
    """
    # Split the lines and create a data frame
    columns = [column.replace("#", "").strip() for column in manifest[0]]
    manifest_df = pd.DataFrame(columns=columns, data=manifest[1:])

    # Validate the manifest structure
    required_headers = ["time", "s3key", "filesize"]
    # First, confirm all required headers are present
    if not all(header in manifest_df.columns for header in required_headers):
        raise IngesterException(
            "Manifest file is missing one of the required headers: " + str(required_headers) + "."
        )

    # Check data types of manifest and cast appropriately
    try:
        manifest_df = manifest_df.astype(
            dtype={"time": "datetime64[ns, UTC]", "s3key": "string", "filesize": "int64"}
        )
    except ValueError as ex:
        raise IngesterException(str(ex)) from ex

    return manifest_df
