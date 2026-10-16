from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

@dp.table(
name="finradar.silver.customers"
,comment="Parses and cleans the customers data"
)

@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_first_name", "first_name IS NOT NULL")
@dp.expect("valid_country", "country IS NOT NULL")
@dp.expect("valid_city", "city IS NOT NULL")

def customers_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finradar.bronze.customers")

    transformed_df = bronze_df.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("gender"),
        F.col("age"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("annual_income"),
        F.col("customer_segment"),
        F.to_date(F.col("account_open_date"),"yyyy-MM-dd").alias("account_open_date"),
        F.col("risk_score"),
        F.col("preferred_spending_min"),
        F.col("preferred_spending_max"),
        F.col("preferred_city"),
        F.col("preferred_country"),
        F.col("trusted_device_id"),
        F.col("card_number"),
        F.col("card_type"),
        F.col("email"),
        F.col("transaction_limit"),
        F.col("update_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )

    return transformed_df
