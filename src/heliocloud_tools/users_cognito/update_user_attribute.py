import boto3
import os

"""
User attribute helper script for AWS Cognito User Pool.
This script lists all users in a specified Cognito User Pool and optionally updates them to add a 'preferred_username' attribute if it is missing.
Legacy HelioCloud UserPools do not have 'preferred_username' set, so this script can be used to add it.
Make sure to set the environment variables USER_POOL_ID and AWS_REGION before running this script.
Usage:
1. Set environment variables:
    export USER_POOL_ID='your_user_pool_id'  # e.g., 'us-west-2_123456789'
    export AWS_REGION='your_aws_region'      # e.g., 'us-west-2'
    
2. Set up AWS credentials in your environment or use an IAM role with appropriate permissions.
3. Run the script:
    python update_user_attribute.py

"""

USER_POOL_ID = os.getenv('USER_POOL_ID')
AWS_REGION = os.getenv('AWS_REGION')

client = boto3.client('cognito-idp', region_name=AWS_REGION)

def list_all_users(user_pool_id):
    """
    Function for getting all users in a Cognito User Pool.
    """
    users = []
    paginator = client.get_paginator('list_users')
    for page in paginator.paginate(UserPoolId=user_pool_id):
        users.extend(page['Users'])
    return users

def print_user_attributes(users):
    """
    Function for printing user attributes in a readable format.
    """
    for idx, user in enumerate(users, 1):
        print(f"\n=== User #{idx}: {user.get('Username')} ===")
        for attr in user.get('Attributes', []):
            print(f"  {attr['Name']}: {attr['Value']}")
        print(f"  Status: {user.get('UserStatus')}")
        print('-' * 40)

def add_preferred_username(user_pool_id, users):
    """
    Function for adding 'preferred_username' attribute to users if it is missing.
    """
    for user in users:
        username = user['Username']
        attrs = {attr['Name']: attr['Value'] for attr in user['Attributes']}
        if 'preferred_username' not in attrs:
            try:
                client.admin_update_user_attributes(
                    UserPoolId=user_pool_id,
                    Username=username,
                    UserAttributes=[
                        {
                            'Name': 'preferred_username',
                            'Value': username
                        }
                    ]
                )
                print(f"Set preferred_username for '{username}' to '{username}'")
            except Exception as e:
                print(f"Error updating user '{username}': {e}")
        else:
            print(f"User '{username}' already has preferred_username set to '{attrs['preferred_username']}'")

if __name__ == "__main__":
    print(f"Fetching users from User Pool: {USER_POOL_ID}...\n")
    users = list_all_users(USER_POOL_ID)
    if not users:
        print("No users found in the user pool.")
    else:
        print_user_attributes(users)
        print("\nUpdating users to add 'preferred_username' where missing...\n")
        add_preferred_username(USER_POOL_ID, users)
