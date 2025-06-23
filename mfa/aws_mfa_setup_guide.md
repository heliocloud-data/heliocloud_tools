# AWS-MFA Setup Guide

### What is this?
These are instructions for setting up client-based multi-factor-authentication in a way that doesn't require typing in any keys manually or exporting environment variables by hand, allowing frictionless use relying only on 3 commands/inputs:
`DEV`, `mfaX` and `AWS_EXP` such as this for example:
```
oms9@oms9-Dev:~/Desktop$ DEV
Here are your MFA aliases:
mfa1 - smce-helio
mfa2 - smce-helio2
mfa3 - smce-tops
Dev environment activated, aliases created, and directory changed.

(.venv) oms9@oms9-Dev:~/Desktop/repo$ mfa3
INFO - Validating credentials for profile: smce-tops 
INFO - Your credentials have expired, renewing.
Enter AWS MFA code for device [arn:aws:iam::6149XXXXXXXX:mfa/Desktop] (renewing for 43200 seconds):126906
INFO - Fetching Credentials - Profile: smce-tops, Duration: 43200
INFO - Success! Your credentials will expire in 43200 seconds at: 2024-11-16 02:16:46+00:00

(.venv) oms9@oms9-Dev:~/Desktop/repo$ AWS_EXP
1) smce-helio
2) smce-helio2
3) smce-tops
Select a profile: 3
You selected: smce-tops
AWS credentials for profile 'smce-tops' have been exported.
```
To confirm the setup was successful:
```
(.venv) oms9@oms9-Dev:~/Desktop/repo$ printenv | grep '^AWS_'
AWS_SECRET_ACCESS_KEY=qWLsI5HCtf+2fqZxMVXXXXXXXXXXX
AWS_ACCESS_KEY_ID=ASIAY6LFCXXXXXXXX
AWS_SESSION_TOKEN=FwoGZXIvYXdzEJD////////// truncated for demonstration  ///
```

### Installation:
- Run `pip install aws-mfa` inside your virtual environment.
- Edit your credentials file, typically located at `~/.aws/credentials` as follows:
  - change every `[profile-name]` to be `[profile-name-long-term]`
  - For example: `[smce-helio]` becomes `[smce-helio-long-term]`
  - Add a line named `aws_mfa_device` and paste the arn of your device
  - The line should look like: `aws_mfa_device = arn:aws:iam::424XXXXXXXXX:mfa/DevName`
  - An example profile should therefore look like: 
    ```
    [smce-helio-long-term] 
    aws_mfa_device = arn:aws:iam::424XXXXXXXXX:mfa/DevName
    aws_access_key_id = AKIAWFXXXXXXXXXXXXXX
    aws_secret_access_key = lwIvJqJ6F48Pz99WXXXXXXXXXXXXXXXXXXXXXXXX
    ```
    
- Next, locate your `.bashrc` file, typically found at: `~/.bashrc` and append the following functions to the end of the file, pay attention to the last function as you will need to modify it to include your repo's path.
    ```
    AWS_EXP() {
        # This parses through the creds file to print available profiles.
        profiles=$(grep -oP '^\[\K[^\]]+' ~/.aws/credentials | grep -v "long-term")

        if [ -z "$profiles" ]; then
            echo "No profiles found."
            return 1
        fi

        # Print the profiles out as a numbered list so users can select which profile to export
        PS3="Select a profile: "
        select profile in $profiles; do
            if [ -n "$profile" ]; then
                echo "You selected: $profile"
                break
            else
                echo "Invalid selection, try again."
            fi
        done

        # When the profile is selected, find that line/header and then retrieve the keys by skipping down from that line
        access_key=$(awk -v profile="[$profile]" '$0 == profile {getline; getline; print $3}' ~/.aws/credentials)
        secret_key=$(awk -v profile="[$profile]" '$0 == profile {getline; getline; getline; print $3}' ~/.aws/credentials)
        session_token=$(awk -v profile="[$profile]" '$0 == profile {getline; getline; getline; getline; print $3}' ~/.aws/credentials)

        # Export the profile as a set of environment variables
        export AWS_ACCESS_KEY_ID=$access_key
        export AWS_SECRET_ACCESS_KEY=$secret_key
        export AWS_SESSION_TOKEN=$session_token

        echo "AWS credentials for profile '$profile' have been exported."
        echo ""
    }
    ```
    ```
    function CREATE_ALIASES() {
        # Capture the list of profiles from the aws configure and exclude '-long-term' and 'default'
        local profiles=$(aws configure list-profiles | grep -v '\-long\-term' | grep -v '^default$')
        
        local count=1

        # Clear any previous aliases to avoid mishaps
        unalias mfa* 2>/dev/null

        echo "Here are your MFA aliases:"

        # Loop through each profile and create an alias using the count var
        while IFS= read -r profile; do
            if [ -n "$profile" ]; then
                alias_name="mfa${count}"
                alias_command="alias ${alias_name}='aws-mfa --profile ${profile}'"
                eval $alias_command
                echo "${alias_name} - ${profile}"
                count=$((count + 1))
            fi
        done <<< "$profiles"
    }
    ```
    ```
    function DEV() {
        # Create the aliases first since it doesnt require being in the venv
        CREATE_ALIASES

        # (OPTIONAL) cd to the repo
        # cd ~/INSERT REPO PATH HERE
        
        echo "Dev environment activated, aliases created, and directory changed."
        echo ""
        
        # (OPTIONAL) but ensure that aws-mfa is accessible in your terminal if you wish to disable activating your venv 
        source INSERT REPO PATH HERE/.venv/bin/activate
    }
    ```
