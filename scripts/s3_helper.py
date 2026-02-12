"""
S3/MinIO helper.
Uses boto3 with MinIO locally. To deploy to AWS, change only MINIO_ENDPOINT to
the AWS S3 endpoint — all other code stays the same.
"""
import os
import io
import boto3
from botocore.client import Config


def get_s3_client():
    """Create S3 client pointing at MinIO."""
    return boto3.client(
        's3',
        endpoint_url=os.environ['MINIO_ENDPOINT'],
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
    )


def ensure_bucket(bucket_name):
    """Create bucket if it doesn't exist."""
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        s3.create_bucket(Bucket=bucket_name)


def upload_file(bucket, key, filepath):
    """Upload a local file to S3/MinIO."""
    s3 = get_s3_client()
    ensure_bucket(bucket)
    s3.upload_file(filepath, bucket, key)
    print(f"Uploaded {filepath} -> s3://{bucket}/{key}")


def download_file(bucket, key, filepath):
    """Download a file from S3/MinIO to local path."""
    s3 = get_s3_client()
    s3.download_file(bucket, key, filepath)
    print(f"Downloaded s3://{bucket}/{key} -> {filepath}")


def read_csv_from_s3(bucket, key):
    """Read a CSV directly from S3 into a pandas DataFrame."""
    import pandas as pd
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), low_memory=False)


def list_files(bucket, prefix=''):
    """List all files in a bucket with optional prefix filter."""
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', [])]
    except Exception:
        return []
