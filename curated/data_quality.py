import boto3
import json
from datetime import datetime
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType , IntegerType

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("ingest_time", StringType(), True)  # Added by Kafka consumer
])


customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("region", StringType(), True),
    StructField("customer_tenure_days", IntegerType(), True)
])

# Initialize GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

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
        report = {
            'summary': {
                'total_checks': len(self.dq_results),
                'passed': sum(1 for r in self.dq_results if r['status'] == 'PASS'),
                'failed': sum(1 for r in self.dq_results if r['status'] == 'FAIL'),
                'timestamp': datetime.now().isoformat()
            },
            'details': self.dq_results
        }
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json.dumps(report, indent=2),
            ContentType='application/json'
        )
        print(f"✅ DQ Report saved: s3://{s3_bucket}/{s3_key}")
        return report

# ---- Glue Job Logic ----

# Read Parquet from S3
orders_df = spark.read.json("s3://vrams-data-lake-inbound/inbound/topic_orders/date=2025-10-19/*.json")
customers_df = spark.read.json("s3://vrams-data-lake-inbound/inbound/topic_customers/date=2025-10-19/*.json")

validator = DataQualityValidator(spark)

# Completeness
validator.check_completeness(
    orders_df,
    "orders",
    ["order_id", "customer_id", "order_amount", "order_date"]
)

# Accuracy
validation_rules = {
    'order_amount_positive': col('order_amount') > 0,
    'order_date_valid': col('order_date') <= datetime.now()
}
validator.check_accuracy(orders_df, "orders", validation_rules)

# Consistency
validator.check_consistency(
    orders_df,
    customers_df,
    "customer_id",
    "orders",
    "customers"
)

# Volume
# validator.check_volume(orders_df, "orders", expected_min=10000, expected_max=50000)

# Generate DQ report in S3
report = validator.generate_report(
    s3_bucket="vrams-data-lake-curated",
    s3_key="dq_reports/orders_2025-10-19.json"
)

# Write clean data if all checks passed
if all(r['status'] == 'PASS' for r in validator.dq_results):
    orders_df.write.mode("overwrite").partitionBy("order_date").parquet("s3://vrams-data-lake-curated/orders/")
    customers_df.write.mode("overwrite").parquet("s3://vrams-data-lake-curated/customers/")
    print("✅ Data written to Curated bucket")
else:
    orders_df.write.mode("overwrite").json("s3://vrams-data-lake-quarantine/orders/date=2025-10-19/")
    print("❌ Data failed DQ checks. Quarantined.")
