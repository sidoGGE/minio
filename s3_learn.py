import boto3 
import requests 
from botocore.exceptions import ClientError 
from botocore.client import Config

MINIO_ENDPOINT = 'http://localhost:9000' 
ACCESS_KEY = 'admin'
SECRET_KEY = 'password'
BUCKET_NAME = 'demo-buckets'# ----backet_name

def get_s3_client():# create s3 taliking with minIO/\/this def it's (s3_client in next def)
    return boto3.client('s3',
                        endpoint_url = MINIO_ENDPOINT,
                        aws_access_key_id = ACCESS_KEY,
                        aws_secret_access_key = SECRET_KEY,
                        region_name = 'us-east-1',
                        config=Config(signature_version="s3v4", s3={'addressing_style': 'path'})
                        )

def ensure_bucket(s3_client, bucket_name):# s3_clean is(def get_s3_cleane)
    #create bucket if it not exist
    try:#findout if th bucket exist
        s3_client.head_bucket(Bucket=bucket_name)#----find out
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get('Code', '')
        if error_code in {"404", "NoSuchBucket"}:
            print(f"Bucket '{bucket_name}' not found. Creating it...")
            s3_client.create_bucket(Bucket=bucket_name)#----create
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"An unexpected error occurred: {e}")
            raise

def download_upload(file_url, bucket_name, object_name=None):
    print(f'Downloading {file_url}...')
    response = requests.get(file_url, stream=True, timeout=30)
    response.raise_for_status()

    if object_name is None:
        object_name = file_url.split('/')[-1] or 'downloaded-file'# give as name from the last [-1] part of url after '/' or name it first-file if not exist the last part
    
    s3_client = get_s3_client()
    ensure_bucket(s3_client, bucket_name)

    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_name,
        Body=response.content,
        ContentType=response.headers.get("content-type", "application/octet-stream")
    )

    print(f'Upload successful to MinIO: {bucket_name}/{object_name}')


if __name__ == "__main__":
    # Example URL from your other script to make this runnable
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    object_name = "yellow_tripdata_2023-01.parquet"
    download_upload(url, BUCKET_NAME, object_name=object_name)
