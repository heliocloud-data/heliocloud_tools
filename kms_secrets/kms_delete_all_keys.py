import argparse
import boto3

LIMIT = 50
DAYS = 7


def disable_key(client, key_id: str):
    print(f"Disabling key: {key_id} ...")
    resp = client.disable_key(KeyId=key_id)


def schedule_key_deletion(client, key_id: str, pending_window_in_days: int):
    print(f" Scheduling for deletion in {pending_window_in_days}")
    client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=pending_window_in_days)


def schedule_all_keys_for_deletion(client, days: int):
    next_marker = ""

    key_count = 0
    fail_count = 0
    while next_marker != None:
        if next_marker == "" or next_marker == None:
            resp = client.list_keys(Limit=LIMIT)
        else:
            resp = client.list_keys(Limit=LIMIT, Marker=next_marker)

        for key in resp["Keys"]:
            print(key["KeyId"])
            desc_key = client.describe_key(KeyId=key["KeyId"])
            if "KeyMetadata" not in desc_key or "KeyManager" not in desc_key["KeyMetadata"]:
                print(" Unable to detect key manager")
                continue
            if desc_key["KeyMetadata"]["KeyManager"] == "AWS":
                print(" Skipping over AWS managed key")
                continue

            if (
                "KeyState" in desc_key["KeyMetadata"]
                and desc_key["KeyMetadata"]["KeyState"] == "PendingDeletion"
            ):
                print(
                    f" Already scheduled for deletion on {desc_key['KeyMetadata']['DeletionDate']}"
                )
                continue

            try:
                disable_key(client, key["KeyId"])
                schedule_key_deletion(client, key["KeyId"], days)
                key_count = key_count + 1
            except Exception:
                print(" Failed to delete")
                fail_count = fail_count + 1

        next_marker = None
        if "Truncated" in resp and resp["Truncated"] == True:
            next_marker = resp["NextMarker"]


if __name__ == "__main__":
    # Require an instance name argument
    parser = argparse.ArgumentParser(
        prog="HelioCloud tool for removing ALL user created KMS keys",
        description="",
    )
    parser.add_argument(
        "region",
        type=str,
        help="Name of the AWS region in which the HelioCloud instance resides. This should "
        "match the region value configured in the HelioCloud's instance configuration.",
    )

    args = parser.parse_args()
    if args.region == None:
        print("Using default region...")
        client = boto3.client("kms")
    else:
        print(f"Using region {args.region}")
        client = boto3.client("kms", region_name=args.region)

    schedule_all_keys_for_deletion(client, DAYS)
