from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, date_format, current_timestamp

spark = SparkSession.builder \
    .appName("GoldLayerTransformations") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Read from Curated layer
orders_df = spark.read.parquet("s3://vrams-data-lake-curated/orders/")
customers_df = spark.read.parquet("s3://vrams-data-lake-curated/customers/")

# Transformation 1: Daily Sales by Region
daily_sales = orders_df \
    .join(customers_df, on='customer_id', how='inner') \
    .groupBy(
        date_format(col('order_date'), 'yyyy-MM-dd').alias('date'),
        col('region')
    ) \
    .agg(
        count('order_id').alias('total_orders'),
        sum('order_amount').alias('total_revenue'),
        avg('order_amount').alias('avg_order_value'),
        count('customer_id').alias('unique_customers')
    ) \
    .withColumn('processing_timestamp', current_timestamp())

# Write to Gold layer
daily_sales.write \
    .mode('overwrite') \
    .partitionBy('date', 'region') \
    .parquet('s3://vrams-data-lake-gold/sales_daily/')

print("✅ Daily sales aggregation completed")