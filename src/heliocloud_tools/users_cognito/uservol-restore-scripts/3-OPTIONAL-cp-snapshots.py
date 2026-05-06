import boto3
import csv
import argparse
import shutil
from datetime import datetime


def get_snapshot(ec2, snapshot_id):
    response = ec2.describe_snapshots(SnapshotIds=[snapshot_id])
    return response["Snapshots"][0]


def build_tags(snapshot, extra_tags=None):
    tags = snapshot.get("Tags") or []

    tags.append({
        "Key": "CopiedAt",
        "Value": datetime.utcnow().isoformat()
    })

    if extra_tags:
        tags.extend(extra_tags)

    return tags


def allow_cross_account_copy(ec2_source, snapshot_id, dest_account_id):
    print(f"Sharing snapshot {snapshot_id} with account {dest_account_id}")

    ec2_source.modify_snapshot_attribute(
        SnapshotId=snapshot_id,
        Attribute="createVolumePermission",
        OperationType="add",
        UserIds=[dest_account_id]
    )


def copy_snapshot(ec2_dest, snapshot, source_region):
    response = ec2_dest.copy_snapshot(
        SourceSnapshotId=snapshot["SnapshotId"],
        SourceRegion=source_region,
        Description=snapshot.get("Description", "")
    )
    return response["SnapshotId"]


def tag_snapshot(ec2_dest, snapshot_id, tags):
    ec2_dest.create_tags(
        Resources=[snapshot_id],
        Tags=tags
    )


def process_csv(file_path, source_ec2, dest_ec2, source_region, source_account, dest_account):
    # Backup CSV first
    backup_path = file_path + ".bak.cp"
    shutil.copyfile(file_path, backup_path)
    print(f"Backup created: {backup_path}")

    # Load CSV
    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        fieldnames = reader.fieldnames

    if "snap_id" not in fieldnames:
        raise ValueError("CSV must contain 'snap_id' column")

    same_account = source_account == dest_account

    for row in rows:
        old_snapshot_id = row["snap_id"]

        print(f"\nProcessing snapshot: {old_snapshot_id}")

        try:
            snapshot = get_snapshot(source_ec2, old_snapshot_id)

            # IMPORTANT:
            # Cross-account requires permission BEFORE copy
            if not same_account:
                allow_cross_account_copy(
                    source_ec2,
                    old_snapshot_id,
                    dest_account
                )

            # Copy snapshot (region + account aware via permissions)
            new_snapshot_id = copy_snapshot(
                dest_ec2,
                snapshot,
                source_region
            )

            print(f"Copied -> {new_snapshot_id}")

            # Tag copied snapshot
            tags = build_tags(snapshot)
            tag_snapshot(dest_ec2, new_snapshot_id, tags)

            print(f"Tagged snapshot {new_snapshot_id}")

            # Update CSV in-place
            row["snap_id"] = new_snapshot_id

        except Exception as e:
            print(f"FAILED {old_snapshot_id}: {str(e)}")

    # Write updated CSV back
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nCSV updated in-place: {file_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copy EC2 snapshots across regions and accounts"
    )

    parser.add_argument("-f", "--input-file", required=True)

    parser.add_argument("--src-region", required=True)
    parser.add_argument("--dest-region", required=True)

    parser.add_argument("--src-account", required=True)
    parser.add_argument("--dest-account", required=True)

    args = parser.parse_args()

    session_source = boto3.Session(region_name=args.src_region)
    session_dest = boto3.Session(region_name=args.dest_region)

    ec2_source = session_source.client("ec2")
    ec2_dest = session_dest.client("ec2")

    process_csv(
        args.input_file,
        ec2_source,
        ec2_dest,
        args.src_region,
        args.src_account,
        args.dest_account
    )

    print("\nCopying snapshots completed!")
