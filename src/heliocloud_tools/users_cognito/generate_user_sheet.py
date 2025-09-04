import boto3
import csv

if __name__ == "__main__":
    # Fill the following information SMDC/Science Cloud friendly
    # just get an access key and scroll all the way down, copy and paste the data in here
    
    user_pool_id = "PoolID here"
    region = "us-east-1"
    
    # These are in the same order as you get from the smdc ID, Secret, Session
    access_key = "FILL-HERE"
    secret_key = "FILL-HERE"
    session_token = "FILL-HERE"

    client = boto3.client(
        "cognito-idp",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )
    
    # This is the best way to go through the list as it handles any amount of users in the pool
    paginator = client.get_paginator("list_users")

    with open("userList.csv", mode="w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["Name", "Username", "Email", "CreatedTime"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for page in paginator.paginate(UserPoolId=user_pool_id): 
            for user in page["Users"]:
                username = user.get("Username")
                created_time = user.get("UserCreateDate")

                # Format date as MM/DD/YYYY (cutting off the unix-like timestamp at the end)
                created_date_str = created_time.strftime("%m/%d/%Y") if created_time else ""

                # We have to dig for the rest as they are in a field called "attributes"
                attributes = {attr["Name"]: attr["Value"] for attr in user.get("Attributes", [])}
                name = attributes.get("name", "")
                email = attributes.get("email", "")

                writer.writerow({
                    "Name": name,
                    "Username": username,
                    "Email": email,
                    "CreatedTime": created_date_str
                })

    print("Script finished executing, file saved.")
