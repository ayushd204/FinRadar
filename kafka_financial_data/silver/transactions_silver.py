# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import json
from pyspark.sql.types import *

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("card_number", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), False),
])

@dp.table(
name="finradar.silver.transactions"
,comment="Parses and cleans the transaction data"
)
# the dp.expect set the data quality rules ; 
@dp.expect_or_drop("valid_transaction_id","transaction_id is NOT NULL",)
# if the rule is not respected, the data is dropped , (rule_name,actual rule)
@dp.expect_or_drop("valid_customer_id","customer_id is NOT NULL",)
@dp.expect_or_drop("valid_card_number","card_number is NOT NULL",)
@dp.expect_or_drop("valid_merchant_id","merchant_id is NOT NULL",)
@dp.expect("valid_amount","amount > 0")


def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finradar.bronze.transactions")

    transformed_df = bronze_df.select(
        F.from_json(col("value"), schema).alias("data")
        ,F.col("topic").alias("kafka_topic")
        ,F.col("partition").alias("kafka_partition")
        ,F.col("offset").alias("kafka_offset")
        ,F.col("timestamp").alias("kafka_timestamp")
        ,F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
        ).select(
            F.col("data.*")
            ,F.col("kafka_topic")
            ,F.col("kafka_partition")
            ,F.col("kafka_offset")
            ,F.col("kafka_timestamp")
            ,F.col("bronze_ingestion_timestamp")
            ,F.current_timestamp().alias("silver_ingestion_timestamp")
            )
    return transformed_df

#this code took data from the bronze table and then refined it 1 layer above to the silver layer ; 



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finradar.gold.alert_email_log