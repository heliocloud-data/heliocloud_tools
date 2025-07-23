#!/bin/sh

# Set up new user with a directory and sftp account for the helio-dh-data S3 bucket

# sftp server id
SERVER_ID="s-e21dbb55598e452b9"

# name of our S3 bucket which is backed by an sftp server
# which we are adding a user account for
BUCKET="helio-data-staging"

# the RW transfer user role
ROLE="arn:aws:iam::424120911029:role/helio-data-staging-rw-transfer-role"

# use the AWS user id
USER_ID=$1

#home directory
HOME_DIR=/$BUCKET

# SSH KEY NAME
SSH_KEY_NAME=$USER_ID"_rsa"

# ------- MAIN BODY -------
echo "Adding sftp account for $USER_ID for $BUCKET"

# generate new ssh key
#
echo "Generated SSH user key into $SSH_KEY_NAME"
ssh-keygen -P "" -m PEM -f $SSH_KEY_NAME
SSH_PUB_KEYNAME=$SSH_KEY_NAME".pub"
SSH_PUB_BLOCK=`cat $SSH_PUB_KEYNAME`

# quick set of region, just in case
#
#aws configure set region us-east-1

# create sftp user with home dir same name as user ID
#
# echo aws transfer create-user --server-id \"$SERVER_ID\" --user-name $USER_ID --role $ROLE --ssh-public-key-body \"$SSH_PUB_BLOCK\" --home-directory-mappings \"[ { \\\"Entry\\\": \\\"/\\\", \\\"Target\\\": \\\"$HOME_DIR\\\" } ]\" --home-directory-type LOGICAL 

aws transfer create-user --server-id "$SERVER_ID" --user-name $USER_ID --role $ROLE --ssh-public-key-body "$SSH_PUB_BLOCK" --home-directory-mappings "[ { \"Entry\": \"/\", \"Target\": \"$HOME_DIR\" } ]" --home-directory-type LOGICAL 

