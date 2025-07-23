# HelioCloud tool for listing the users in the Cognito pool(s) created
# Replace user-pool-id with the correct ID from your deployment

aws cognito-idp list-users --user-pool-id us-east-1_p09cMJeEj | grep Username | sed s/\"//g | sed s/Username// | sed s/,// | sed s/:// | sed s/\ //g | sort
