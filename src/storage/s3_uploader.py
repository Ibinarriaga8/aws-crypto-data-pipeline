import boto3

def upload_file(file_path, bucket, key):
    """
    Uploads a file to an S3 bucket.
    """
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket, key)