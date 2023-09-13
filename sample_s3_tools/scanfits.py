"""
Simple fits S3 reader, validator and register tool.
Uses internal FITS checksum to validate header and data.
Also extracts FITS header keywords for populating a database

"""

from astropy.io import fits
import boto3


def main():
    test_fnames = [
        "sdo_aia_h2_20121231T235600_0094_v1.fits",
        "sdo_hmi_h2_20121126T234800_bp_err_v1.fits",
        "sdo_hmi_h2_20121126T234800_bp_v1.fits",
        "sdo_hmi_h2_20121126T234800_br_err_v1.fits",
        "sdo_hmi_h2_20121126T234800_br_v1.fits",
        "sdo_hmi_h2_20121126T234800_bt_err_v1.fits",
        "sdo_hmi_h2_20121126T234800_bt_v1.fits",
        "sdo_aia_bad.fits",
    ]

    for tf in test_fnames:
        sdo_scanfits(tf)


def sdo_maketable():
    # RANGE key is a sort key, HASH key is a primary key
    dynamodb_resource = boto3.resource("dynamodb")
    table = dynamodb_resource.create_table(
        TableName="SDO_MDI",
        KeySchema=[
            {"AttributeName": "Name", "KeyType": "HASH"},
            {"AttributeName": "Email", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "Name", "KeyType": "HASH"},
            {"AttributeName": "Email", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    print(table)


def sdo_dbstore():
    # dynamodb_client = boto3.client('dynamodb')
    dynamodb_resource = boto3.resource("dynamodb")
    table = dynamodb_resource.Table("SDO_MDI")
    response = table.put_item(Item={"Name": "me", "Email": "whatever"})
    print(response)


def sdo_scanfits(fname):
    try:
        hdul = fits.open(fname, use_fsspec=True, checksum=True, fsspec_kwards={"anon": True})
    except Exception as e:
        print("Error: ", e, ": ", fname)
        return

    hdul.info()
    # print(repr(hdul[1].header))

    registry = {}

    keywords = [
        "TELESCOP",
        "INSTRUME",
        "WAVELNTH",
        "CAMERA",
        "CONTENT",
        "T_OBS",
        "QUALITY",
        "CHECKSUM",
        "CRLN_OBS",
        "CRLT_OBS",
        "CAR_ROT",
    ]
    if hdul[1].header["TELESCOP"] == "SDO/AIA":
        keywords += [
            "WAVE_STR",
            "PROC_LEV",
            "IMG_TYPE",
            "EXPTIME",
            "LVL_NUM",
            "PERCENTD",
            "QUALLEV0",
        ]
    elif hdul[1].header["TELESCOP"] == "SDO/HMI":
        keywords += ["CADENCE"]

    for key in keywords:
        if key in hdul[1].header:
            registry[key] = hdul[1].header[key]
            print(key, ":", registry[key])

    hdul.close()


main()
