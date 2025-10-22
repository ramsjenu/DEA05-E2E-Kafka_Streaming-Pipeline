from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, datediff, min as spark_min, max as spark_max

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, date_format, current_timestamp

# Paths to JARs - Ensure these paths are correct
delta_jar = "/home/mage_code/mage_demo/spark-config/delta-core_2.12-2.4.0.jar"
delta_s_jar = "/home/mage_code/mage_demo/spark-config/delta-storage-2.4.0.jar"
hadoop_aws_jar = "/home/mage_code/mage_demo/spark-config/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/mage_code/mage_demo/spark-config/aws-java-sdk-bundle-1.12.262.jar"

# Initialize Spark Session with Delta and MinIO configurations

spark = SparkSession.builder \
    .appName("GoldLayerTransformations") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://orders/silver/") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", f"{delta_jar},{delta_s_jar},{hadoop_aws_jar},{aws_sdk_jar}") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
    .getOrCreate()

orders_df = spark.read.parquet("s3a://orders/silver/")
customers_df = spark.read.parquet("s3a://customers/silver")

orders_df = orders_df.withColumn(
    "order_date_only",
    expr("date_add('1970-01-01', order_date)")
)

    
# Calculate CLV metrics
customer_metrics = orders_df \
    .groupBy('customer_id') \
    .agg(
        count('order_id').alias('total_orders'),
        sum('order_amount').alias('lifetime_value'),
        avg('order_amount').alias('avg_order_value'),
        spark_min('order_date_only').alias('first_order_date'),
        spark_max('order_date_only').alias('last_order_date')
    ) \
    .withColumn(
        'customer_tenure_days',
        datediff(col('last_order_date'), col('first_order_date'))
    ) \
    .withColumn(
        'avg_days_between_orders',
        col('customer_tenure_days') / col('total_orders')
    )

# Join with customer demographic

clv_enriched = customer_metrics.alias("m") \
    .join(customers_df.alias("c"), on="customer_id", how="inner") \
    .select(
        col("m.customer_id"),
        col("c.name"),
        col("c.email"),
        col("c.region"),
        col("m.total_orders"),
        col("m.lifetime_value"),
        col("m.avg_order_value"),
        col("c.customer_tenure_days"),        # clearly from customers_df
        col("m.avg_days_between_orders"),
        col("m.first_order_date"),
        col("m.last_order_date"),
        current_timestamp().alias("processing_timestamp")
    )


# Write to Gold layer
s3_clv_path = 's3a://orders/gold/customer_lifetime_value/'

clv_enriched.write.mode("overwrite").format("delta").partitionBy('region').save(s3_clv_path)

print("✅ Customer lifetime value calculation completed")

"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""
