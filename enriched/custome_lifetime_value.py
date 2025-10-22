from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, datediff, min as spark_min, max as spark_max

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, date_format, current_timestamp

spark = SparkSession.builder \
    .appName("GoldLayerTransformations") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

orders_df = spark.read.parquet("s3://vrams-data-lake-curated/orders/")
customers_df = spark.read.parquet("s3://vrams-data-lake-curated/customers/")


    
# Calculate CLV metrics
customer_metrics = orders_df \
    .groupBy('customer_id') \
    .agg(
        count('order_id').alias('total_orders'),
        sum('order_amount').alias('lifetime_value'),
        avg('order_amount').alias('avg_order_value'),
        spark_min('order_date').alias('first_order_date'),
        spark_max('order_date').alias('last_order_date')
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
clv_enriched.write \
    .mode('overwrite') \
    .partitionBy('region') \
    .parquet('s3://vrams-data-lake-gold/customer_lifetime_value/')

print("✅ Customer lifetime value calculation completed")