from pyspark.sql.functions import col, count, sum as spark_sum, avg, current_timestamp, to_date
from pyspark.sql import SparkSession

# Convert timestamp to date for partitioning
delta_jar = "/home/mage_code/mage_demo/spark-config/delta-core_2.12-2.4.0.jar"
delta_s_jar = "/home/mage_code/mage_demo/spark-config/delta-storage-2.4.0.jar"
hadoop_aws_jar = "/home/mage_code/mage_demo/spark-config/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/mage_code/mage_demo/spark-config/aws-java-sdk-bundle-1.12.262.jar"

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
    .config("spark.jars", f"{delta_jar},{delta_s_jar},{hadoop_aws_jar},{aws_sdk_jar}") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
    .getOrCreate()


orders_df = spark.read.parquet("s3a://orders/silver/*.parquet")
customers_df = spark.read.parquet("s3a://customers/silver/*.parquet")

# orders_df = orders_df.withColumn("order_date_only", to_date(col("order_date")))

# orders_df.printSchema()
from pyspark.sql.functions import expr

# Suppose orders_df.after.order_date is integer days
orders_df = orders_df.withColumn(
    "order_date_only",
    expr("date_add('1970-01-01', order_date)")
)


daily_sales = orders_df \
    .join(customers_df, on='customer_id', how='inner') \
    .groupBy(
        col('order_date_only').alias('date'),
        col('region')
    ) \
    .agg(
        count('order_id').alias('total_orders'),
        spark_sum('order_amount').alias('total_revenue'),
        avg('order_amount').alias('avg_order_value'),
        count('customer_id').alias('unique_customers')
    ) \
    .withColumn('processing_timestamp', current_timestamp())

# Write to Gold layer with partitioning
s3_path = "s3a://orders/gold/sales_daily/"
daily_sales.write.mode('overwrite').format("delta").partitionBy('date','region').save(s3_path)
