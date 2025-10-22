from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, expr
from delta.tables import DeltaTable

# Paths
BRONZE_PATH = "s3a://orders/bronze/"

# Initialize Spark (if not already)
# Paths to JARs - Ensure these paths are correct
delta_jar = "/home/mage_code/mage_demo/spark-config/delta-core_2.12-2.4.0.jar"
delta_s_jar = "/home/mage_code/mage_demo/spark-config/delta-storage-2.4.0.jar"
hadoop_aws_jar = "/home/mage_code/mage_demo/spark-config/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/mage_code/mage_demo/spark-config/aws-java-sdk-bundle-1.12.262.jar"
avro_sdk_jar = "/home/mage_code/mage_demo/spark-config/spark-avro_2.12-3.4.0.jar"

# Initialize Spark Session with Delta and MinIO configurations

spark = SparkSession.builder \
    .appName("DeltaExample1") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://orders/silver/") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", f"{delta_jar},{delta_s_jar},{hadoop_aws_jar},{aws_sdk_jar},{avro_sdk_jar}") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
    .getOrCreate()


# Flattened DataFrame example
# bronze_df should have columns: order_id, customer_id, order_amount, order_date, ingestion_timestamp, source_file, ingestion_layer
# Convert order_date from integer to proper date

bronze_df = spark.read.format("avro").load("s3a://orders/topics/streaming.public.order/*")


bronze_df = bronze_df.withColumn(
    "order_date",
    expr("date_add('1970-01-01', after.order_date)")
)

# Add metadata
bronze_df = bronze_df.withColumn("ingestion_timestamp", current_timestamp()) \
                     .withColumn("source_file", input_file_name()) \
                     .withColumn("ingestion_layer", lit("bronze"))

# Overwrite Delta table
bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(BRONZE_PATH)
print("✅ Bronze Delta overwritten with flattened data")
"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""


df = spark.read.format("delta").load(BRONZE_PATH)
df.printSchema()
df.show(5)
