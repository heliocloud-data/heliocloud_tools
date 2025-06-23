import boto3

buckets = ["helio-public", "gov-nasa-hdrl-data1", "heliodata"]
fnames = [
    "skantunes/psp_wispr.fts",
    "sdo/aia/20221231/0094/sdo_aia_h2_20221231T235600_0094_v1.fits",
    "stereoa/20070426/20070426_014630_n4euA.fts",
]

keynames = ["Kion-HC", "Kion-EUVML", "HC-SMCE"]
keys = ["", "", ""]

secrets = ["", "", ""]

tokens = ["", "", ""]

for i in range(3):
    bucket = buckets[i]
    fname = fnames[i]
    try:
        print("Trying anon on", bucket)
        s3 = boto3.resource("s3")
        s3obj = s3.Object(bucket, fname)
        data = s3obj.get()["Body"].read()
        print("  ", data[0:78])
        # print(data[400:560])
    except Exception as error:
        print("  ", bucket, "failed:", error)

    for j in range(len(keys)):
        keyname = keynames[j]
        key = keys[j]
        secret = secrets[j]
        token = tokens[j]
        try:
            print("Trying key", keyname, "on", bucket)
            s3r = boto3.resource(
                "s3", aws_access_key_id=key, aws_secret_access_key=secret, aws_session_token=token
            )
            s3obj = s3r.Object(bucket, fname)
            data = s3obj.get()["Body"].read()
            print("  ", data[0:78])
            # print(data[400:560])
        except Exception as error:
            print("  ", keyname, bucket, "failed:", error)
