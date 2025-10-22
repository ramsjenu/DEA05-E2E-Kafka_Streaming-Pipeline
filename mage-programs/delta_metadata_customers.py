from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, udf
from delta.tables import DeltaTable

# Paths to JARs - Ensure these paths are correct
delta_jar = "/home/mage_code/mage_demo/spark-config/delta-core_2.12-2.4.0.jar"
delta_s_jar = "/home/mage_code/mage_demo/spark-config/delta-storage-2.4.0.jar"
hadoop_aws_jar = "/home/mage_code/mage_demo/spark-config/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/mage_code/mage_demo/spark-config/aws-java-sdk-bundle-1.12.262.jar"
avro_sdk_jar = "/home/mage_code/mage_demo/spark-config/spark-avro_2.12-3.4.0.jar"

# --------------------------------------------
# 1️⃣ Initialize Spark Session with Delta support
# --------------------------------------------
spark = SparkSession.builder \
    .appName("DeltaExample1") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://customers/topics/streaming.public.customers/") \
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

spark.sparkContext.setLogLevel("INFO")

print(spark.version)


# --------------------------------------------
# 2️⃣ Define input/output paths
# --------------------------------------------
RAW_PATH = "s3a://customers/topics/streaming.public.customers/"          # Replace with your bucket
BRONZE_PATH = "s3a://customers/bronze/"  # Destination path

# --------------------------------------------
# 3️⃣ Read raw data (CSV example)
# --------------------------------------------
#raw_df = spark.read.avro(RAW_PATH)

raw_df = spark.read.format("avro").load("s3a://customers/topics/streaming.public.customers/*")
raw_df.show(5)


print(f"✅ Read {raw_df.count()} records from {RAW_PATH}")


# --------------------------------------------
# 4️⃣ Add ingestion metadata
# --------------------------------------------
bronze_df = (
    raw_df.withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_layer", lit("bronze"))
)


# --------------------------------------------
# 5️⃣ Write as Delta table (Bronze layer)
# --------------------------------------------
(
    bronze_df.write.format("delta")
    .mode("overwrite")  # or "append" for incremental
    .save(BRONZE_PATH)
)

print(f"✅ Written Delta Bronze table to {BRONZE_PATH}")

# --------------------------------------------
# 6️⃣ (Optional) Register table in the Metastore
# --------------------------------------------
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS bronze;
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.source_data_bronze
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print("✅ Registered Delta table: bronze.source_data_bronze")

# --------------------------------------------
# 7️⃣ Verify Metadata & History
# --------------------------------------------
delta_table = DeltaTable.forPath(spark, BRONZE_PATH)
print("\n📜 Delta Table History:")
delta_table.history().show(truncate=False)

# --------------------------------------------
# 8️⃣ Stop Spark
# --------------------------------------------
spark.stop()
"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""
