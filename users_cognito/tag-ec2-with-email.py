import boto3

REGION = 'us-east-1'
USER_POOL_ID = 'us-east-1_p09cMJeEj'

ec2 = boto3.client('ec2', region_name=REGION)
cognito = boto3.client('cognito-idp', region_name=REGION)

def get_user_email_from_cognito(username):
    try:
        response = cognito.admin_get_user(
            UserPoolId=USER_POOL_ID,
            Username=username
        )
        for attr in response['UserAttributes']:
            if attr['Name'] == 'email':
                return attr['Value']
        print(f"No email found for Cognito user: {username}")
    except cognito.exceptions.UserNotFoundException:
        print(f"User {username} not found in Cognito.")
    return None

def get_all_instances():
    paginator = ec2.get_paginator('describe_instances')
    instances = []
    for page in paginator.paginate():
        for reservation in page['Reservations']:
            for instance in reservation['Instances']:
                instances.append(instance)
    return instances

def tag_instance_with_email(instance_id, email):
    ec2.create_tags(Resources=[instance_id], Tags=[{'Key': 'Email', 'Value': email}])
    print(f"Tagged instance {instance_id} with Email: {email}")

def process_instance(instance):
    instance_id = instance['InstanceId']
    tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}

    if 'Owner' not in tags or tags['Owner'].lower() == 'admin':
        return

    if 'Email' in tags:
        print(f"Instance {instance_id} already has an Email tag. Skipping.")
        return

    username = tags['Owner']
    email = get_user_email_from_cognito(username)

    if email:
        tag_instance_with_email(instance_id, email)

def main(test_instance_id=None):
    if test_instance_id:
        print("Running in test mode...")
        try:
            instance = ec2.describe_instances(InstanceIds=[test_instance_id])['Reservations'][0]['Instances'][0]
            process_instance(instance)
        except Exception as e:
            print(f"Error processing test instance: {e}")
    else:
        print("Processing all instances in region...")
        all_instances = get_all_instances()
        for instance in all_instances:
            process_instance(instance)

if __name__ == '__main__':
    import sys
    if len(sys.argv) == 2:
        main(test_instance_id=sys.argv[1])
    else:
        confirm = input("Run on ALL instances? 'yes' to confirm: ")
        if confirm.lower() == 'yes':
            main()
        else:
            print("Aborted.")
