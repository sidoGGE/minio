import boto3
import requests
from botocore.exceptions import ClientError
from botocore.client import Config
# MinIO works as a local S3 server
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password"
BUCKET_NAME = "demo-bucket"
def get_s3_client():
    # طباعة للتأكد من المفاتيح التي يستخدمها السكربت فعلياً
    print(f"DEBUG: Connecting to {MINIO_ENDPOINT}")
    print(f"DEBUG: Using Access Key: {ACCESS_KEY}")  
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={'addressing_style': 'path'})
    )

def ensure_bucket(s3_client, bucket_name):
    """Create the bucket if it does not exist."""

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
       
        if error_code in {"404", "NoSuchBucket"}:
            print(f"Bucket '{bucket_name}' not found. Creating it...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"An unexpected error occurred: {e}")
            raise

def download_and_upload(file_url, bucket_name, object_name=None):
    """Download a file from the internet and upload it to MinIO."""
    print(f"Downloading {file_url}...")
    
    try:
        response = requests.get(file_url, stream=True, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to download the file: {e}")
        return
    if object_name is None:
        object_name = file_url.split("/")[-1] or "downloaded-file"
    s3_client = get_s3_client()
    ensure_bucket(s3_client, bucket_name)
    try:
        s3_client.upload_fileobj(
            response.raw,
            bucket_name,
            object_name,
            ExtraArgs={
                "ContentType": response.headers.get("content-type", "application/octet-stream")
            },
        )
        print(f"Upload successful to MinIO: {bucket_name}/{object_name}")
    except ClientError as e:
        print(f"Upload failed: {e}")

if __name__ == "__main__":
# ======= الجزء السفلي من الكود بعد التعديل =======
    years = ["2022", "2023"] 
    months = [f"{i:02d}" for i in range(1, 13)] 

    for year in years:
        for month in months:
            file_name = f"yellow_tripdata_{year}-{month}.parquet"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

            print(f"\n--- جاري سحب ورفع بيانات {year}-{month} ---")
            
            # نمرر المسار الجديد كـ object_name
            download_and_upload(url, BUCKET_NAME, object_name=file_name)
