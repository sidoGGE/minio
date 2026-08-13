FROM apache/airflow:2.9.2
RUN pip install --no-cache-dir confluent-kafka boto3 pandas pyarrow