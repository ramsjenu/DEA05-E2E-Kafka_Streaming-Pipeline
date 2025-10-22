from pyspark.sql import SparkSession
from datetime import datetime
from datetime import date
from pyspark.sql.functions import col, count, avg, date_format, current_timestamp, sum as spark_sum , current_date
from pyspark.sql.functions import col, from_unixtime, to_timestamp, to_date

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

import boto3
import json

from botocore.client import Config

# Paths to JARs - Ensure these paths are correct
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

# Initialize GlueContext

class DataQualityValidator:
    def __init__(self, spark):
        self.spark = spark
        self.dq_results = []

    def check_completeness(self, df, table_name, required_columns):
        total_records = df.count()
        for column in required_columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = (null_count / total_records) * 100
            result = {
                'table': table_name,
                'check_type': 'completeness',
                'column': column,
                'total_records': total_records,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2),
                'status': 'PASS' if null_percentage < 5 else 'FAIL',
                'timestamp': datetime.now().isoformat()
            }
            self.dq_results.append(result)
            print(f"[Completeness] {column}: {null_count}/{total_records} Nulls ({null_percentage:.2f}%) -> {result['status']}")
        return self

    def check_accuracy(self, df, table_name, validation_rules):
        total_records = df.count()
        for rule_name, rule_condition in validation_rules.items():
            invalid_count = df.filter(~rule_condition).count()
            invalid_percentage = (invalid_count / total_records) * 100
            result = {
                'table': table_name,
                'check_type': 'accuracy',
                'rule': rule_name,
                'total_records': total_records,
                'invalid_count': invalid_count,
                'invalid_percentage': round(invalid_percentage, 2),
                'status': 'PASS' if invalid_percentage < 1 else 'FAIL',
                'timestamp': datetime.now().isoformat()
            }
            self.dq_results.append(result)
            print(f"[Accuracy] {rule_name}: {invalid_count}/{total_records} Invalid ({invalid_percentage:.2f}%) -> {result['status']}")
        return self

    def check_consistency(self, df1, df2, join_key, table1_name, table2_name):
        orphaned_records = df1.join(df2, on=join_key, how='left_anti')
        orphaned_count = orphaned_records.count()
        total_records = df1.count()
        orphaned_percentage = (orphaned_count / total_records) * 100
        result = {
            'table': f"{table1_name}_vs_{table2_name}",
            'check_type': 'consistency',
            'join_key': join_key,
            'total_records': total_records,
            'orphaned_count': orphaned_count,
            'orphaned_percentage': round(orphaned_percentage, 2),
            'status': 'PASS' if orphaned_percentage < 1 else 'FAIL',
            'timestamp': datetime.now().isoformat()
        }
        self.dq_results.append(result)
        print(f"[Consistency] Orphaned: {orphaned_count}/{total_records} ({orphaned_percentage:.2f}%) -> {result['status']}")
        return self

    def check_volume(self, df, table_name, expected_min, expected_max):
        actual_count = df.count()
        result = {
            'table': table_name,
            'check_type': 'volume',
            'actual_count': actual_count,
            'expected_min': expected_min,
            'expected_max': expected_max,
            'status': 'PASS' if expected_min <= actual_count <= expected_max else 'FAIL',
            'timestamp': datetime.now().isoformat()
        }
        self.dq_results.append(result)
        print(f"[Volume] {actual_count} records (Expected: {expected_min}-{expected_max}) -> {result['status']}")
        return self

    def generate_report(self, s3_bucket, s3_key):
        import builtins  # ensure we use Python's sum

    # Build report
        report = {
            'summary': {
                'total_checks': len(self.dq_results),
                'passed': builtins.sum(1 for r in self.dq_results if r['status'] == 'PASS'),
                'failed': builtins.sum(1 for r in self.dq_results if r['status'] == 'FAIL'),
                'timestamp': datetime.now().isoformat()
            },
            'details': self.dq_results
        }

        # Create S3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',  # MinIO server URL
            aws_access_key_id='admin',
            aws_secret_access_key='password',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # Upload JSON report
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json.dumps(report, indent=2),
            ContentType='application/json'
        )

        print(f"✅ DQ Report saved to MinIO: s3://{s3_bucket}/{s3_key}")
        return report



# ---- Glue Job Logic ----

# Read Parquet from S3
orders_df = spark.read.parquet("s3a://orders/bronze/*.parquet")
customers_df = spark.read.parquet("s3a://customers/bronze/*.parquet")


from pyspark.sql.functions import col


# Flatten the Debezium-style structure if necessary
if "after" in orders_df.columns:
    orders_df = orders_df.select(
        col("after.order_id").alias("order_id"),
        col("after.customer_id").alias("customer_id"),
        col("after.order_amount").alias("order_amount"),
        col("after.order_date").alias("order_date"),
        col("ingestion_timestamp")
    )


# Flatten CDC structure for customers
if "after" in customers_df.columns:
    customers_df = customers_df.selectExpr(
        "after.customer_id as customer_id",
        "after.name as name",
        "after.email as email",
        "after.region as region",
        "after.customer_tenure_days as customer_tenure_days",
        "ingestion_timestamp",
        "ingestion_layer"
    )


validator = DataQualityValidator(spark)

# Completeness
validator.check_completeness(
    orders_df,
    "orders",
    ["order_id", "customer_id", "order_amount", "order_date"]
)
orders_df = orders_df.withColumn(
    "order_date_only",
    expr("date_add('1970-01-01', order_date)")
)
# Accuracy
validation_rules = {
    'order_date_valid': col('order_date_only') <= current_date(),
    'order_amount_positive': col('order_amount') > 0
}
#validator.check_accuracy(orders_df, "orders", validation_rules)
validator.check_accuracy(orders_df, "orders", validation_rules)

# Consistency
validator.check_consistency(
    orders_df,
    customers_df,
    "customer_id",
    "order",
    "customers"
)

# Volume
# validator.check_volume(orders_df, "orders", expected_min=10000, expected_max=50000)

# Generate DQ report in S3
report = validator.generate_report(
    s3_bucket="orders",
    s3_key="dq_reports/orders_dq_report.json"
)

s3_orders_path = "s3a://orders/silver/"
s3_customers_path = "s3a://customers/silver/"
# Write clean data if all checks passed
if all(r['status'] == 'PASS' for r in validator.dq_results):
    orders_df.write.mode("overwrite").format("delta").save(s3_orders_path)
    customers_df.write.mode("overwrite").format("delta").save(s3_customers_path)
    print("✅ Data written to Curated bucket")
else:
    orders_df.write.mode("overwrite").json("s3a://orders/quarentine/")
    print("❌ Data failed DQ checks. Quarantined.")
"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""
