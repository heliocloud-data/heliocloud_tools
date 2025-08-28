#!/bin/bash

# This uses MFA devices to get temporary (eg 12 hour) credentials.  Requires
# a TTY for user input.
#
# GPL 2 or higher

#### THIS LINE ASSUMES YOU HAVE MORE THAN ONE AWS PROFILE AND SET A PROFILE WHEN YOU CONFIGURED AWS CLI
#### IF YOU ONLY HAVE ONE PROFILE AND DIDN"T SET THINGS UP FOR MULTIPLE PROFILES, 
#### THEN TAKE OUT PROFILE BIT ANY ANY LINE WITH IT EXCEPT THE ONE THAT HAS USERPROFILE VERSION COMMENTED OUT. 
#### YOU"LL WANT TO UNCOMMENT THAT LINE AND USE IT INSTEAD!
#### when you run this bash script, add your profile name at the end like `bash aws_cli_bash_starter_for_mfa_assumesMultipleProfiles.sh curiousgeorge`
name_of_your_aws_profile_set_in_aws_cli=$1
AWS_PROFILE=$name_of_your_aws_profile_set_in_aws_cli

if [ ! -t 0 ]
then
  echo Must be on a tty >&2
  exit 255
fi

if [ -n "$AWS_SESSION_TOKEN" ]
then
 echo "Session token found.  This can not be used to generate a new token. unset AWS_SESSION_TOKEN AWS_SECRET_ACCESS_KEY AWS_ACCESS_KEY_ID and then ensure you have a profile with the normal access key credentials or set the variables to the normal keys.">&2
  exit 255
fi

identity=$(aws sts get-caller-identity --profile $AWS_PROFILE --output json)
username=$(echo -- "$identity" | sed -n 's!.*"arn:aws:iam::.*:user/\(.*\)".*!\1!p')
if [ -z "$username" ]
then
  echo "Can not identify who you are.  Looking for a line like arn:aws:iam::.....:user/FOO_BAR but did not find one in the output of aws sts get-caller-identity $identity" >&2
  exit 255
fi

echo You are: $username >&2

#### UNCOMMENT THIS LINE AND USED IT INSTEAD OF THE ONE BELOW! IF YOU DID NOT SET UP A MULTIPLE PROFILES IN AWS CLI
# mfa=$(aws iam list-mfa-devices --user-name "$username" --output json)
mfa=$(aws iam list-mfa-devices --profile $AWS_PROFILE --output json)
device=$(echo -- "$mfa" | sed -n 's!.*"SerialNumber": "\(.*\)".*!\1!p')
if [ -z "$device" ]
then
  echo "Can not find any MFA device for you.  Looking for a SerialNumber but did not find one in the output of aws iam list-mfa-devices --username \"$username\" $mfa" >&2
  exit 255
fi

echo Your MFA device is: $device >&2

echo -n "Enter your MFA code now: " >&2
read code

tokens=$(aws sts get-session-token --serial-number "$device" --token-code $code --output json --profile $AWS_PROFILE)

secret=$(echo -- "$tokens" | sed -n 's!.*"SecretAccessKey": "\(.*\)".*!\1!p')
session=$(echo -- "$tokens" | sed -n 's!.*"SessionToken": "\(.*\)".*!\1!p')
access=$(echo -- "$tokens" | sed -n 's!.*"AccessKeyId": "\(.*\)".*!\1!p')
expire=$(echo -- "$tokens" | sed -n 's!.*"Expiration": "\(.*\)".*!\1!p')

if [ -z "$secret" -o -z "$session" -o -z "$access" ]
then
  echo "Unable to get temporary credentials.  Could not find secret/access/session entries

$tokens" >&2
  exit 255
fi

export AWS_SESSION_TOKEN=$session
export AWS_SECRET_ACCESS_KEY=$secret
export AWS_ACCESS_KEY_ID=$access

echo export AWS_SESSION_TOKEN=$session
echo export AWS_SECRET_ACCESS_KEY=$secret
echo export AWS_ACCESS_KEY_ID=$access
echo ""
echo Keys valid until $expire >&2
