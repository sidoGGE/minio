from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 1. إعداد الجلسة (الأساسيات للاتصال بـ MinIO)
spark = SparkSession.builder \
    .appName("KafkaToMinioSilver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# تقليل رسائل النظام المزعجة للتركيز على الأخطاء الحقيقية إن وُجدت
spark.sparkContext.setLogLevel("WARN")
print("--- تم تشغيل Spark بنجاح، جاري الاتصال بـ Kafka ---")

# 2. تعريف Schema التقريبية لبيانات التاكسي 
# (ملاحظة: تأكد من إضافة باقي الأعمدة إذا كنت تحتاجها كلها)
schema = StructType() \
    .add("VendorID", StringType()) \
    .add("tpep_pickup_datetime", StringType()) \
    .add("tpep_dropoff_datetime", StringType()) \
    .add("passenger_count", DoubleType()) \
    .add("trip_distance", DoubleType()) \
    .add("total_amount", DoubleType())

# 3. قراءة البيانات من Topic كافكا
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "taxi_orders") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. تحويل البيانات من صيغة Bytes (التي يرسلها كافكا) إلى أعمدة مقروءة
df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

print("--- جاري سحب البيانات وكتابتها في silver-bucket ---")

# 5. الكتابة إلى MinIO في الباكيت الجديد
query = df_parsed.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://silver-bucket/taxi_data/") \
    .option("checkpointLocation", "s3a://silver-bucket/checkpoints/") \
    .start()

query.awaitTermination()