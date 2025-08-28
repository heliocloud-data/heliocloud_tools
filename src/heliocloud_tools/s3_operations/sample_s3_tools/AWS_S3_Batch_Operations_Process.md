# AWS S3 Batch Operations Process

## Preparing a Manifest
For both tasks (copying and then moving objects), you will need to prepare a manifest file that lists the S3 objects on which the operations will be performed. This manifest can be in CSV or JSON format and should be uploaded to an S3 bucket.

1. **Create a CSV or JSON manifest file** that lists the objects in your source bucket (Example s3://gov-nasa-hdrl-data1/sdo/` for the first task).
2. **Upload the manifest file** to a bucket where you have permissions to read and write.

## Creating a Copy Job
To replicate the syncing behavior by copying objects:

1. **Go to the S3 Console** and select **Batch Operations** from the navigation pane.
2. **Choose Create job** and select the source bucket where your manifest file is located.
3. **Select the manifest file** you prepared.
4. **For Operation**, choose **Copy**.
5. **Specify the destination** as `Example s3://gov-nasa-hdrl-data1/sdac/sdo/`.
6. **Set additional options** as needed (e.g., S3 Storage Class, ACLs).
7. **Review and create the job**.
8. **Validate the copy 
## Creating a Delete Job
After validating the copy operation:

1. **Prepare a new manifest file** if necessary, listing the objects to delete.
2. **Create a new Batch Operations job**, selecting **Delete** as the operation.
3. **Follow the job creation process**, selecting your manifest file for deletion.
4. **Review and create the job** to delete the objects.

## Moving Objects
To move objects:

1. **Prepare a manifest file** listing the objects in `Example s3://gov-nasa-hdrl-data1/cdaweb/`.
2. **Create a Batch Operations job** for copying these objects to 'Example s3://gov-nasa-hdrl-data1/spdf/`.
3. After ensuring the copy was successful, **prepare a manifest for deletion**.
4. **Create a deletion job** using the new manifest to remove the original objects.

## Monitoring and Validation
- **Monitor Job Progress** in the **Batch Operations** dashboard.
- **Validation:** Check the destination buckets post-operation.

## Considerations
- **IAM Permissions:** Ensure necessary permissions for batch operations.
- **Testing:** Test your operation with a smaller subset before full application.
- **Backup:** Have backups of critical data before deletion. Do not delete stagging until validation is complete.
