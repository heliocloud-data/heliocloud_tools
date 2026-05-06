
# User volume restore scripts

## About
These 4 scripts are created to ease the update and migration process of HelioCloud deployments.

These scripts allow admins to copy over all the user volumes from a deployment and transplant them in a fresh environment, including the shared working space.

The scripts are intended to be run in order as they complete they automate the whole process on the admin-instance associated with the HelioCloud deployment, often named: `<deployment-name>/Daskhub/DaskhubInstance`.

## Operation
### The python scripts require the following packages to run:
- PyYAML
- boto3
#### To create a virtual environment: `$ python -m venv vol-restore`
#### Activation: `$ source vol-restore/bin/activate`
#### pip install command: `pip install boto3 PyYAML`
---
- ### [1-export.sh](https://github.com/heliocloud-data/heliocloud_tools/src/heliocloud_tools/users_cognito/uservol-restore-scripts/1-export.sh "1-export.sh"):
	This script is responsible for creating a CSV representation of the current cluster's configuration that is vital in recreating the user volumes, Persistent Volume configuration and Persistent Volume Claims necessary for the new volumes to be detected by a new cluster.
	#### usage: `$ bash ./1-export.sh`
	#### output: `user-vols-<timestamp>.csv`
---
- ### [2-snapshot-vols.sh](https://github.com/heliocloud-data/heliocloud_tools/src/heliocloud_tools/users_cognito/uservol-restore-scripts/2-snapshot-vols.sh "2-snapshot-vols.sh")	:
	This script is responsible for creating snapshots (backups) of the user-volumes for purposes of recreation or transfer from one AWS account/region to another.
	#### usage: `$ bash ./2-snapshot-vols.sh -f user-vols<timestamp>.csv`
	#### output: `None, snapshot ids are embedded in input CSV file, backup is created.`
---
- ### User-volumes CSV (generated):
	At this point, the first script has generated the user volumes CSV, and the second script has modified it.

	This CSV contains the necessary information to construct the user volumes in the new cluster and must be transferred off of the admin HelioCloud instance of the old deployment to the new admin HelioCloud instance of the new deployment.
---
- ### [3-OPTIONAL-cp-snapshots.py](https://github.com/heliocloud-data/heliocloud_tools/src/heliocloud_tools/users_cognito/uservol-restore-scripts/3-OPTIONAL-cp-snapshots.py "3-OPTIONAL-cp-snapshots.py"):
	This script is optional, it is used to transfer the backed up user volumes to a target AWS region or account where the new HelioCloud deployment lives such that the volumes are created in the same account and region as the new Kubernetes cluster.
	#### usage: 
	```
	$ python3 3-OPTIONAL-cp-snapshots.py -f user-vols<timestamp>.csv \
		-src-region SRC_REGION \
		-dest-region DEST_REGION \
		-src-profile SRC_PROFILE \
		-dest-profile DEST_PROFILE \
	```
	#### output: `None, the CSV is modified again to include the new snapshot IDs after the copy, backup is created.`
---
- ### [4-restore-vols.py](https://github.com/heliocloud-data/heliocloud_tools/src/heliocloud_tools/users_cognito/uservol-restore-scripts/4-restore-vols.py "4-restore-vols.py"):
	This script is responsible for creating volumes from the snasphots and then reintegrating the fresh user-volumes to the Kubernetes cluster and handles all PV/PVC creation and deletion automatically.
	#### usage: `$ python3 ./4-restole-vols.py -f user-vols<timestamp>.csv -z availability-zone-#a/b/c`
	#### output: `Logs and reports are generated.`
	- ⚠️ **NOTE**: This script will **overwrite** existing user data and delete existing PV/PVCs if they are found as the volume-id attribute is `read-only` after creation and therefore we cannot simply update in-place the target/new volume-id, this behavior can be mitigated by using the `--skip-existing` argument.
		- *This however should not be a problem as it is assumed the new HelioCloud deployment is still untouched by users.*
---
