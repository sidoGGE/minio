import json
import boto3
import pandas as pd
import os
import sys
from io import BytesIO
from botocore.exceptions import ClientError
from confluent_kafka import Producer

# إعداد للتبديل بين تشغيل الكود داخل Docker (minio:9000) أو محلياً (localhost:9000)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = "admin"
SECRET_KEY = "password"


def ensure_bucket(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket"}:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def produce_taxi_data(year, month):
    print(f"--- بدء سحب بيانات التاكسي لـ {year}-{month} ---")

    # 1. الاتصال بـ MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name='us-east-1'
    )

    bucket_name = "demo-bucket"
    ensure_bucket(s3_client, bucket_name)

    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    # المسار الذي طابق ما ظهر في الـ Terminal لديك
    object_name = f"yellow_taxi/year={year}/month={month}/{file_name}"

    print(f"جاري جلب الملف: {object_name}")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        parquet_bytes = response['Body'].read()
        df = pd.read_parquet(BytesIO(parquet_bytes))
        print(f"تمت قراءة {len(df)} سجل.")

        # 2. إرسال إلى كافكا
        # ملاحظة: داخل Docker نستخدم 'kafka:9092'، محلياً نستخدم 'localhost:9092'
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
        producer = Producer({'bootstrap.servers': kafka_bootstrap})
        topic_name = "taxi_orders"

        records = df.to_dict(orient='records')
        print("بدء عملية الضخ إلى Kafka...")

        for index, record in enumerate(records):
            producer.produce(
                topic=topic_name,
                value=json.dumps(record, default=str).encode('utf-8'),
                callback=delivery_report
            )
            # تخفيف الضغط على الذاكرة والشبكة
            if index % 1000 == 0:
                producer.poll(0)

        producer.flush(timeout=30)
        print("--- اكتمل الإرسال بنجاح ---")

    except Exception as e:
        print(f"خطأ أثناء المعالجة: {e}")
        raise

if __name__ == "__main__":
    # هذا البلوك مخصص للتشغيل المحلي المباشر (خارج Docker)
    # قبل التشغيل، قم بتعيين متغيرات البيئة في الـ Terminal:
    # export MINIO_ENDPOINT="http://localhost:9000"
    # export KAFKA_BOOTSTRAP="localhost:9092"
    # ثم قم بتشغيل السكربت: python producer.py 2022 01
    year = sys.argv[1] if len(sys.argv) > 1 else '2022'
    month = sys.argv[2] if len(sys.argv) > 2 else '01'

    produce_taxi_data(year, month)