#!/usr/bin/env python3
"""
4-restore-vols.py

For each row in the migration CSV (produced by 1-export.sh + 2-snapshot-vols.sh (or 3-),
this script:
  1. Creates an EBS volume from the snapshot in the target AZ
  2. Wait for the volume to become available
  3. Creates a Kubernetes PersistentVolume pointing at the new vol-id
  4. Creates a Kubernetes PersistentVolumeClaim bound to that PV
  5. Bind new PV and PVC to Kubernetes

Input CSV columns:
  pvc_name, pv_name, status, capacity, access_modes, storage_class,
  ebs_volume_id, aws_region, age_days, reclaim_policy, volume_mode,
  user_claim, snap_id

Usage:
  python3 4-restore-vols.py [OPTIONS]

Options:
  -f, --file              FILE    Migration CSV (required)
  -n, --namespace         NS      Target k8s namespace       (default: daskhub)
  -k, --kubeconfig        FILE    Path to new cluster kubeconfig
  -z, --availability-zone AZ      AWS AZ for new volumes     (required unless --dry-run)
  -t, --volume-type       TYPE    EBS volume type            (default: gp3)
  -s, --storageclass      NAME    StorageClass to use        (default: gp2)
      --reclaim-policy    POLICY  Retain|Delete|Recycle      (default: Retain)
      --dry-run                   Print manifests; no AWS or k8s calls
      --skip-existing             Skip PVCs already in the namespace
  -h, --help                      Show this help
"""

from __future__ import annotations
import argparse
import csv
import subprocess
import sys
import time
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError, WaiterError
import yaml

# ── ANSI colours ──────────────────────────────────────────────────────────────

CYAN   = "\033[0;36m"
GREEN  = "\033[0;32m"
YELLOW = "\033[1;33m"
RED    = "\033[0;31m"
BOLD   = "\033[1m"
NC     = "\033[0m"

def log(msg):  print(f"{CYAN}[INFO]{NC}  {msg}")
def ok(msg):   print(f"{GREEN}[OK]{NC}    {msg}")
def warn(msg): print(f"{YELLOW}[WARN]{NC}  {msg}")
def err(msg):  print(f"{RED}[ERROR]{NC} {msg}", file=sys.stderr)
def die(msg):  err(msg); sys.exit(1)

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("-f", "--file",              required=True,       help="Migration CSV")
    p.add_argument("-n", "--namespace",         default="daskhub",   help="Kubernetes namespace")
    p.add_argument("-k", "--kubeconfig",        default=None,        help="Path to kubeconfig")
    p.add_argument("-z", "--availability-zone", default=None,        help="Target AWS AZ (e.g. us-east-2a)")
    p.add_argument("-t", "--volume-type",       default="gp3",       help="EBS volume type")
    p.add_argument("-s", "--storageclass",      default="gp2",       help="Kubernetes StorageClass")
    p.add_argument(      "--reclaim-policy",    default="Retain",    help="PV reclaim policy")
    p.add_argument(      "--dry-run",           action="store_true", help="No AWS/k8s calls")
    p.add_argument(      "--skip-existing",     action="store_true", help="Skip existing PVCs")
    return p.parse_args()

# ── Shell helpers ─────────────────────────────────────────────────────────────

def run(cmd: list[str], capture=True, check=True) -> subprocess.CompletedProcess:
    """Run a command as a proper argument list — no shell, no quoting surprises."""
    return subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        check=check,
    )

def aws(*args) -> dict:
    """Run an AWS CLI command and return parsed JSON output."""
    result = run(["aws", *args, "--output", "json"])
    return json.loads(result.stdout)

def kubectl(args: list[str], kubeconfig: str | None, namespace: str | None = None,
            stdin: str | None = None, check=True) -> subprocess.CompletedProcess:
    cmd = ["kubectl"]
    if kubeconfig:
        cmd += ["--kubeconfig", kubeconfig]
    if namespace:
        cmd += ["-n", namespace]
    cmd += args
    return subprocess.run(
        cmd,
        input=stdin,
        capture_output=True,
        text=True,
        check=check,
    )

# ── boto3 client (initialised once in main, passed around) ───────────────────

def make_ec2_client(az: str) -> "boto3.client":
    """
    Derive the AWS region from the AZ (strip the trailing letter, e.g.
    us-east-2a -> us-east-2) and return a boto3 EC2 client for that region.
    Credentials are picked up from the standard boto3 chain: env vars,
    ~/.aws/credentials, IAM instance profile, etc.
    """
    region = az[:-1] if az else None
    return boto3.client("ec2", region_name=region)

# ── AWS operations (boto3) ────────────────────────────────────────────────────

def create_volume(ec2, snap_id: str, az: str, volume_type: str,
                  pvc_name: str, user_claim: str, timestamp: str) -> str:
    """Create a new EBS volume from a snapshot. Returns the new vol-id."""
    response = ec2.create_volume(
        SnapshotId=snap_id,
        AvailabilityZone=az,
        VolumeType=volume_type,
        TagSpecifications=[{
            "ResourceType": "volume",
            "Tags": [
                {"Key": "Name",           "Value": pvc_name},
                {"Key": "SourceSnapshot", "Value": snap_id},
                {"Key": "SourcePVC",      "Value": pvc_name},
                {"Key": "SourceUser",     "Value": user_claim},
                {"Key": "RestoredAt",     "Value": timestamp},
                {"Key": "ManagedBy",      "Value": "ebs-migration-tool"},
            ],
        }],
    )
    return response["VolumeId"]


def wait_for_volume(ec2, vol_id: str, timeout_s: int = 300) -> bool:
    """
    Use the boto3 built-in EBS waiter, which polls describe_volumes every 15s.
    Falls back to a manual poll on WaiterError so we can report the final state.
    """
    try:
        waiter = ec2.get_waiter("volume_available")
        waiter.wait(
            VolumeIds=[vol_id],
            WaiterConfig={"Delay": 8, "MaxAttempts": timeout_s // 8},
        )
        return True
    except WaiterError:
        # Report whatever state the volume is actually in
        try:
            resp  = ec2.describe_volumes(VolumeIds=[vol_id])
            state = resp["Volumes"][0]["State"]
        except ClientError:
            state = "unknown"
        warn(f"Timed out or error waiting for {vol_id} (final state: {state}).")
        return False

# ── Kubernetes manifest builders ──────────────────────────────────────────────

def pv_manifest(row: dict, new_vol_id: str, namespace: str,
                storageclass: str, reclaim_policy: str) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    access_modes = row["access_modes"].split("|")
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": row["pv_name"],
            "annotations": {
                "pv.kubernetes.io/provisioned-by":  "ebs.csi.aws.com",
                "migration.tool/source-snapshot":   row["snap_id"],
                "migration.tool/new-ebs-volume":    new_vol_id,
                "migration.tool/source-pvc":        row["pvc_name"],
                "migration.tool/restored-at":       now,
            },
            "labels": {
                "migration-restored": "true",
                "source-pvc":         row["pvc_name"],
            },
        },
        "spec": {
            "capacity":                       {"storage": row["capacity"]},
            "volumeMode":                     row["volume_mode"] or "Filesystem",
            "accessModes":                    access_modes,
            "persistentVolumeReclaimPolicy":  row["reclaim_policy"] or reclaim_policy,
            "storageClassName":               row["storage_class"] or storageclass,
            "csi": {
                "driver":       "ebs.csi.aws.com",
                "volumeHandle": new_vol_id,
                "fsType":       "ext4",
            },
            "claimRef": {
                "namespace": namespace,
                "name":      row["pvc_name"],
            },
        },
    }


def pvc_manifest(row: dict, namespace: str, storageclass: str) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    access_modes = row["access_modes"].split("|")
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name":      row["pvc_name"],
            "namespace": namespace,
            "annotations": {
                "migration.tool/bound-pv":     row["pv_name"],
                "migration.tool/restored-at":  now,
            },
            "labels": {
                "migration-restored": "true",
            },
        },
        "spec": {
            "storageClassName": row["storage_class"] or storageclass,
            "volumeMode":       row["volume_mode"] or "Filesystem",
            "volumeName":       row["pv_name"],
            "accessModes":      access_modes,
            "resources": {
                "requests": {"storage": row["capacity"]},
            },
        },
    }

# ── Pre-flight checks ─────────────────────────────────────────────────────────

def preflight(args):
    import shutil

    if not args.dry_run:
        if not args.availability_zone:
            die("Supply the target AWS AZ with -z / --availability-zone (e.g. us-east-2a)")

    if not shutil.which("kubectl"):
        die("'kubectl' not found in PATH.")

    with open(args.file, newline="") as f:
        header = f.readline()
    if "snap_id" not in header:
        die("CSV is missing the 'snap_id' column. "
            "Run 2-snapshot-vols.sh before this script.")


def ensure_namespace(namespace: str, kubeconfig: str | None, dry_run: bool):
    if dry_run:
        return
    result = kubectl(["get", "namespace", namespace], kubeconfig, check=False)
    if result.returncode != 0:
        log(f"Namespace '{namespace}' not found — creating...")
        kubectl(["create", "namespace", namespace], kubeconfig)
        ok(f"Namespace '{namespace}' created.")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    preflight(args)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    volume_map_path  = f"restore_volume_map_{timestamp}.csv"
    applied_log_path = f"restore_applied_{timestamp}.log"

    # Initialise boto3 EC2 client once — region derived from the AZ argument.
    # In dry-run mode there are no AWS calls so we skip client creation.
    ec2 = make_ec2_client(args.availability_zone) if not args.dry_run else None

    ensure_namespace(args.namespace, args.kubeconfig, args.dry_run)

    # Counters
    created_vol = created_pv = created_pvc = skipped = errors = 0

    log(f"Input CSV  : {args.file}")
    log(f"Namespace  : {args.namespace}")
    log(f"Target AZ  : {args.availability_zone or '(dry-run, not required)'}")
    log(f"Volume type: {args.volume_type}")
    if args.dry_run:
        warn("DRY-RUN mode — nothing will be created.")
    print()

    volume_map_rows = []

    with open(args.file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            # csv.DictReader already strips the newline; also strip any stray
            # whitespace or carriage returns from every field just to be safe.
            row = {k: (v.strip() if v else v) for k, v in row.items()}

            pvc_name = row["pvc_name"]
            snap_id  = row["snap_id"]
            ebs_vol  = row["ebs_volume_id"]

            # ── Validate snap_id ───────────────────────────────────────────────
            if not snap_id.startswith("snap-"):
                warn(f"No valid snap_id for '{pvc_name}' (value: '{snap_id}') — skipping.")
                skipped += 1
                continue

            # ── Skip non-EBS volumes ───────────────────────────────────────────
            if ebs_vol in ("EFS_STATIC", "UNKNOWN", ""):
                warn(f"Skipping non-EBS PVC '{pvc_name}' ({ebs_vol}) — handle EFS separately.")
                skipped += 1
                continue

            log(f"Processing : {pvc_name}  |  snapshot: {snap_id}")

            # ── Skip if PVC already exists ─────────────────────────────────────
            if args.skip_existing and not args.dry_run:
                result = kubectl(
                    ["get", "pvc", pvc_name], args.kubeconfig, args.namespace, check=False
                )
                if result.returncode == 0:
                    warn(f"PVC '{pvc_name}' already exists — skipping.")
                    skipped += 1
                    continue

            # ── Dry run: print manifests and move on ───────────────────────────
            if args.dry_run:
                print(f"  [dry-run] Would create {args.volume_type} volume from {snap_id} in <az>")
                pv  = pv_manifest(row, "vol-PLACEHOLDER", args.namespace,
                                  args.storageclass, args.reclaim_policy)
                pvc = pvc_manifest(row, args.namespace, args.storageclass)
                print(yaml.dump(pv,  default_flow_style=False))
                print("---")
                print(yaml.dump(pvc, default_flow_style=False))
                print()
                continue

            # ── STEP 1: Create EBS volume from snapshot ────────────────────────
            log(f"  Creating {args.volume_type} volume from snapshot {snap_id}...")
            try:
                new_vol_id = create_volume(
                    ec2, snap_id, args.availability_zone, args.volume_type,
                    pvc_name, row["user_claim"], timestamp,
                )
            except ClientError as e:
                err(f"Volume creation failed for '{pvc_name}': {e.response['Error']['Message']}")
                errors += 1
                continue

            ok(f"  New volume: {new_vol_id}")
            created_vol += 1
            volume_map_rows.append({
                "pvc_name":          pvc_name,
                "snap_id":           snap_id,
                "new_vol_id":        new_vol_id,
                "availability_zone": args.availability_zone,
                "volume_type":       args.volume_type,
                "created_at":        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            })

            # ── STEP 2: Wait for volume to be ready ────────────────────────────
            log(f"  Waiting for {new_vol_id} to become available...")
            if not wait_for_volume(ec2, new_vol_id):
                err(f"Volume {new_vol_id} not ready — skipping PV/PVC for '{pvc_name}'.")
                errors += 1
                continue
            ok(f"  {new_vol_id} is available.")

            # ── STEP 3: Ensure clean state (PVC → PV deletion order) ───────────

            # Check existence first
            existing_pvc = kubectl(
                ["get", "pvc", pvc_name], args.kubeconfig, args.namespace, check=False
            )
            existing_pv = kubectl(
                ["get", "pv", row["pv_name"]], args.kubeconfig, check=False
            )

            # If either exists, enforce deletion order: PVC → PV
            if existing_pvc.returncode == 0 or existing_pv.returncode == 0:

                # ── Delete PVC first ───────────────────────────────────────────
                if existing_pvc.returncode == 0:
                    warn(f"  PVC '{pvc_name}' exists — deleting first...")

                    kubectl(
                        ["patch", "pvc", pvc_name,
                         "-p", '{"metadata":{"finalizers":[]}}',
                         "--type", "merge"],
                        args.kubeconfig, args.namespace, check=False,
                    )

                    del_pvc = kubectl(
                        ["delete", "pvc", pvc_name, "--wait=true"],
                        args.kubeconfig, args.namespace, check=False,
                    )

                    with open(applied_log_path, "a") as log_f:
                        log_f.write(del_pvc.stdout)
                        if del_pvc.stderr:
                            log_f.write(del_pvc.stderr)

                    if del_pvc.returncode != 0:
                        err(f"Could not delete PVC '{pvc_name}' — see {applied_log_path}")
                        errors += 1
                        continue

                    log(f"  Waiting for PVC '{pvc_name}' to disappear...")
                    for _ in range(30):
                        chk = kubectl(
                            ["get", "pvc", pvc_name], args.kubeconfig, args.namespace, check=False
                        )
                        if chk.returncode != 0:
                            break
                        time.sleep(2)
                    else:
                        err(f"PVC '{pvc_name}' still present after 60 s — skipping.")
                        errors += 1
                        continue

                # ── Delete PV second ───────────────────────────────────────────
                if existing_pv.returncode == 0:
                    warn(f"  PV '{row['pv_name']}' exists — deleting after PVC...")

                    kubectl(
                        ["patch", "pv", row["pv_name"],
                         "-p", '{"metadata":{"finalizers":[]}}',
                         "--type", "merge"],
                        args.kubeconfig, check=False,
                    )

                    del_pv = kubectl(
                        ["delete", "pv", row["pv_name"], "--wait=true"],
                        args.kubeconfig, check=False,
                    )

                    with open(applied_log_path, "a") as log_f:
                        log_f.write(del_pv.stdout)
                        if del_pv.stderr:
                            log_f.write(del_pv.stderr)

                    if del_pv.returncode != 0:
                        if "NotFound" in del_pv.stderr:
                            warn(f"  PV '{row['pv_name']}' already gone.")
                        else:
                            err(f"Could not delete PV '{row['pv_name']}' — see {applied_log_path}")
                            errors += 1
                            continue
                        chk = kubectl(["get", "pv", row["pv_name"]], args.kubeconfig, check=False)
                        
                    log(f"  Waiting for PV '{row['pv_name']}' to disappear...")
                    for _ in range(30):
                        chk = kubectl(["get", "pv", row["pv_name"]], args.kubeconfig, check=False)
                        if chk.returncode != 0:
                            break
                        time.sleep(2)
                    else:
                        err(f"PV '{row['pv_name']}' still present after 60 s — skipping.")
                        errors += 1
                        continue


            # ── STEP 3: Apply PV ───────────────────────────────────────────────

            pv_yaml = yaml.dump(
                pv_manifest(row, new_vol_id, args.namespace,
                            args.storageclass, args.reclaim_policy),
                default_flow_style=False,
            )
            result = kubectl(["apply", "-f", "-"], args.kubeconfig,
                             stdin=pv_yaml, check=False)

            with open(applied_log_path, "a") as log_f:
                log_f.write(result.stdout)
                if result.stderr:
                    log_f.write(result.stderr)

            if result.returncode != 0:
                err(f"Failed to create PV '{row['pv_name']}' — see {applied_log_path}")
                errors += 1
                continue

            ok(f"  PV created : {row['pv_name']}")
            created_pv += 1


            # ── STEP 4: Apply PVC ──────────────────────────────────────────────

            pvc_yaml = yaml.dump(
                pvc_manifest(row, args.namespace, args.storageclass),
                default_flow_style=False,
            )
            result = kubectl(["apply", "-f", "-"], args.kubeconfig, args.namespace,
                             stdin=pvc_yaml, check=False)

            with open(applied_log_path, "a") as log_f:
                log_f.write(result.stdout)
                if result.stderr:
                    log_f.write(result.stderr)

            if result.returncode != 0:
                err(f"Failed to create PVC '{pvc_name}' — see {applied_log_path}")
                errors += 1
            else:
                ok(f"  PVC created: {pvc_name}")
                created_pvc += 1

            print()

    # ── Write volume map ───────────────────────────────────────────────────────
    if volume_map_rows:
        with open(volume_map_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=volume_map_rows[0].keys())
            w.writeheader()
            w.writerows(volume_map_rows)

    # ── Post-apply binding check ───────────────────────────────────────────────
    if not args.dry_run and created_pvc > 0:
        log("Waiting 10s for PVC bindings to settle...")
        time.sleep(10)
        print()
        log(f"PVC status in namespace '{args.namespace}':")
        result = kubectl(["get", "pvc"], args.kubeconfig, args.namespace, check=False)
        print(result.stdout)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{BOLD}========================================={NC}")
    print(f"{BOLD} Restore Summary{NC}")
    print(f"{BOLD}========================================={NC}")
    print(f"  Namespace        : {args.namespace}")
    print(f"  EBS vols created : {GREEN}{created_vol}{NC}")
    print(f"  PVs created      : {GREEN}{created_pv}{NC}")
    print(f"  PVCs created     : {GREEN}{created_pvc}{NC}")
    print(f"  Skipped          : {YELLOW}{skipped}{NC}")
    print(f"  Errors           : {RED}{errors}{NC}")
    if not args.dry_run:
        print(f"  Volume map       : {BOLD}{volume_map_path}{NC}")
        print(f"  Apply log        : {BOLD}{applied_log_path}{NC}")
    print(f"{BOLD}========================================={NC}")

    if errors:
        print()
        warn(f"{errors} error(s) occurred. Review {applied_log_path} and re-run.")
        sys.exit(1)


if __name__ == "__main__":
    main()
