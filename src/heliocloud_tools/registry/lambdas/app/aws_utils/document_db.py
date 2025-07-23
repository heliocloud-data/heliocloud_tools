"""
Method(s) for getting access to a HelioCloud's AWS DocumentDB instance
"""
import json
import boto3
import pymongo
from botocore.exceptions import ClientError


def get_documentdb_client(
    session: boto3.session.Session, secret_name: str, tls_ca_file: str, local=False
) -> pymongo.MongoClient:
    """
    Constructs and returns a MongoClient instance connected to this HelioCloud's DocumentDB instance
    :param session: boto3.Session to use
    :param secret_name: name of the AWS Secrets Manager secret containing connection credentials
    :param tls_ca_file: CA file to use in a secure connection
    :param local: if True, use  localhost as the hostname to connect to DocumentDB.
           Defaults to False.
    :return:
    """

    # Get the connection secrets
    sm_client = session.client("secretsmanager")
    try:
        response = sm_client.get_secret_value(SecretId=secret_name)
    except ClientError as error:
        raise error

    secret_string = json.loads(response["SecretString"])
    username = secret_string["username"]
    password = secret_string["password"]
    port = secret_string["port"]
    sm_client.close()

    # Allow localhost override (typically used in development scenarios)
    if local:
        return pymongo.MongoClient(
            host="localhost",
            port=port,
            username=username,
            password=password,
            tls=True,
            tlsInsecure=True,
            tlsCAFile=tls_ca_file,
            retryWrites=False,
        )

    # Otherwise normal connection
    host = secret_string["host"]
    return pymongo.MongoClient(
        host=host,
        port=port,
        username=username,
        password=password,
        tls=True,
        tlsCAFile=tls_ca_file,
        retryWrites=False,
    )
