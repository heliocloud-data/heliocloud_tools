import csv
import json
import subprocess

def generate_aws_cli_command(UserName, VolumeID, SnapID, NewPVC):
    region = 'us-east-1'
    availability_zone = 'us-east-1a'

    tags = [
        {"Key": "CSIVolumeName", "Value": NewPVC},
        {"Key": "ebs.csi.aws.com/cluster", "Value": "true"},
        {"Key": "kubernetes.io/cluster/hsdcloud-eks-helio", "Value": "owned"},
        {"Key": "kubernetes.io/created-for/pv/name", "Value": NewPVC},
        {"Key": "kubernetes.io/created-for/pvc/name", "Value": f"claim-{UserName}"},
        {"Key": "kubernetes.io/created-for/pvc/namespace", "Value": "daskhub"},
        {"Key": "KubernetesCluster", "Value": "hsdcloud-eks-helio"},
        {"Key": "Name", "Value": f"hsdcloud-eks-helio-dynamic-{NewPVC}"}
    ]

    # Build the tag string in required AWS CLI format
    formatted_tags = ",".join(
        [f"{{Key={tag['Key']},Value={tag['Value']}}}" for tag in tags]
    )
    tag_spec = f"ResourceType=volume,Tags=[{formatted_tags}]"

    # Construct the AWS CLI command for creating the volume
    command = (
        f"aws ec2 create-volume --region {region} --availability-zone {availability_zone} "
        f"--snapshot-id {SnapID} --volume-type gp2 "
        f"--tag-specifications '{tag_spec}'"
    )

    return command

def process_csv(file_path, dry_run=False):
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            UserName = row['UserName']
            VolumeID = row['VolumeID']
            SnapID = row['SnapID']
            OldPVC = row['OldPVC']
            NewPVC = row['NewPVC']

            command = generate_aws_cli_command(UserName, VolumeID, SnapID, NewPVC)

            # If dry_run is enabled, print the command; otherwise, execute it
            if dry_run:
                print(f"Dry Run: {command}")
            else:
                print(f"Generating volume for: {UserName}")
                result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if result.returncode == 0:
                    try:
                        response = json.loads(result.stdout)
                        created_volume = response["VolumeId"]
                        print(f"{UserName},{created_volume}")
                    except json.JSONDecodeError:
                        print("Failed to parse JSON output.")
                else:
                    print(f"Error creating volume for: {UserName}")
                    print(result.stderr)

if __name__ == "__main__":
    file_path = "UserData.csv"
    dry_run = False  # CHECK THIS FIRST, False = run!!
    process_csv(file_path, dry_run)
    

