import csv
import os
from pathlib import Path

# Constants
namespace = "daskhub"
storage_class = "gp2"
region = "us-east-1"
zone = "us-east-1a"
storage = "100Gi" 
output_dir = "output_yamls"

Path(output_dir).mkdir(parents=True, exist_ok=True)

with open("volumes.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        username = row["userName"]
        volume_id = row["volumeID"]
        pvc_name = f"claim-{username}"
        pv_name = f"pv-{username}"

        # PV YAML
        pv_yaml = f"""apiVersion: v1
kind: PersistentVolume
metadata:
  name: {pv_name}
  labels:
    topology.kubernetes.io/region: {region}
    topology.kubernetes.io/zone: {zone}
spec:
  capacity:
    storage: {storage}
  volumeMode: Filesystem
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: {storage_class}
  awsElasticBlockStore:
    volumeID: {volume_id}
    fsType: ext4
  claimRef:
    namespace: {namespace}
    name: {pvc_name}
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: topology.kubernetes.io/zone
          operator: In
          values:
            - {zone}
        - key: topology.kubernetes.io/region
          operator: In
          values:
            - {region}
"""

        # PVC YAML
        pvc_yaml = f"""apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {pvc_name}
  namespace: {namespace}
  annotations:
    hub.jupyter.org/username: {username}
  labels:
    app: jupyterhub
    chart: jupyterhub-3.2.1
    component: singleuser-storage
    heritage: jupyterhub
    hub.jupyter.org/username: {username}
    release: daskhub
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: {storage}
  storageClassName: {storage_class}
  volumeName: {pv_name}
  volumeMode: Filesystem
"""

        # Write to files
        with open(os.path.join(output_dir, f"{username}_pv.yaml"), "w") as f:
            f.write(pv_yaml)
        with open(os.path.join(output_dir, f"{username}_pvc.yaml"), "w") as f:
            f.write(pvc_yaml)

print(f"YAMLs written to ./{output_dir}/")

