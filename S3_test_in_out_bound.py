import os, sys
import shutil
import boto3
from botocore.exceptions import ClientError

# upload file to S3 bucket
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# upload file from src location to S3
def upload_s3(src, dest):
    """
    Copy each file from src dir to dest dir, including sub-directories.
    """
    s3 = boto3.client('s3')
    
    for item in os.listdir(src):
        file_path = os.path.join(src, item)
        
        # if item is a file, copy it to s3
        if os.path.isfile(file_path):
            with open(file_path, "rb") as f:                
                s3.upload_fileobj(f, dest, item)    
    return True

            
# download file from S3 to dest location
def download_s3(src, dest):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(src)

    for s3_file in bucket.objects.all():
        print(s3_file.key) # prints the contents of bucket
        s3 = boto3.client ('s3')
        dest_file = str(s3_file.key)
        dest_dir = dest + dest_file
        #print(dest_file, ' ', dest_dir)
        s3.download_file(src, dest_file, dest_dir)
    return True

    
# copy only - no delete or move

#local folders
fromdir="C:\\temp\\inbound\\"  
todir="C:\\temp\\outbound\\"

#s3 buckets
bucket_in="inbound"
bucket_out="outbound"

#uploading to s3 bucket
upload_s3(fromdir, bucket_in)

#downloading from s3 bucket
download_s3(bucket_out,todir)
