# Utilities to help with integration tests
import boto3
import os

from app_config import load_configs

# The default HC instance name.
DEFAULT_HC_INSTANCE = "example"

# The maximum number of characters that appear in the lambda name that are
# assigned by the heliocloud deployment.
MAX_LAMBDA_SEGMENT_NAME = 32


def get_region(hc_instance: str, use_env_var_if_available: bool = False) -> str:
    """
    Utility function for fetching the AWS region based on the configuration chain.
    """

    cfg = load_configs(hc_id=hc_instance)

    region = None
    if "env" in cfg and "region" in cfg["env"]:
        region = cfg["env"]["region"]

    if region is None and use_env_var_if_available:
        region = os.getenv("AWS_DEFAULT_REGION")

    return region


def new_boto_session(hc_instance: str) -> boto3.session.Session:
    """
    Creates a new boto3 session for the given heliocloud instance.
    """
    cfg = load_configs(hc_id=hc_instance)

    region = get_region(hc_instance)

    if region is None:
        return boto3.session.Session()
    return boto3.session.Session(region_name=region)


def get_hc_instance() -> str:
    """
    Gets name of the HelioCloud instance currently being worked with
    :return: name of the instance
    """
    ret = os.environ["HC_INSTANCE"]
    if ret is None or len(ret) == 0:
        ret = DEFAULT_HC_INSTANCE

    return ret


def get_ingest_s3_bucket_name(hc_instance: str) -> str:
    """
    Gets the name of the AWS S3 bucket being used for ingest in a HelioCloud's Registry
    :param hc_instance: name of the HelioCloud instance to get the ingest bucket name for
    :return: the ingest bucket name
    """
    cfg = load_configs(hc_id=hc_instance)
    return cfg["registry"]["ingestBucketName"]


def get_registry_s3_buckets(hc_instance: str) -> list[str]:
    """
    Gets the name of the AWS S3 buckets being used in a HelioCloud's Registry for storing and sharing datasets
    :param hc_instance: name of the HelioCloud instance to get the registry bucket name(s) for
    :return: a list of S3 bucket names used by the Registry
    """
    cfg = load_configs(hc_id=hc_instance)
    return cfg["registry"]["datasetBucketNames"]


def remove_file_if_exists(filename: str) -> bool:
    ret = False

    if os.path.exists(filename):
        os.remove(filename)
        ret = True

    return ret


def get_lambda_function_name(session: boto3.Session, hc_instance: str, lambda_name: str) -> str:
    """
    Gets the name of a particular lambda function in AWS based on the HelioCloud instance name and the name
    of the lambda in the codebase
    :param session: a boto3.Session instance to use
    :param hc_instance: name of the HelioCloud instance to lookup the lambda within
    :param lambda_name: name of the lambda to lookup
    :return: function name of the lambda in AWS
    """

    # Derive the starting characters of the function name, which as far as I know,
    # simply drops any hyphen characters from the heliocloud instance name.
    function_name_starts_with = hc_instance.replace("-", "")

    # Additionally, it appears that this segment is a maximum of 32 characters.
    if len(function_name_starts_with) > MAX_LAMBDA_SEGMENT_NAME:
        function_name_starts_with = function_name_starts_with[:MAX_LAMBDA_SEGMENT_NAME]

    client = session.client("lambda")
    response = client.list_functions()
    client.close()

    for function in response["Functions"]:
        function_name = str(function["FunctionName"])

        if (lambda_name in function_name) and function_name.startswith(function_name_starts_with):
            return function_name


def list_lambda_function_names(session: boto3.Session):
    """
    Prints the lambda functions to the console.
    :param session: a boto3.Session instance to use
    """

    client = session.client("lambda")
    response = client.list_functions()
    client.close()

    count = 0
    print("Lambda Function(s):")
    for function in response["Functions"]:
        function_name = str(function["FunctionName"])

        print(f" * {function_name}")
        count = count + 1

    if count == 0:
        print("<EMPTY>")
