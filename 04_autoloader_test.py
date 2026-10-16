# Databricks notebook source

source_path = "/Volumes/finradar/source/fraud_watchlist/source_data/"

# COMMAND ----------

dbutils.fs.ls(source_path)

# COMMAND ----------

input_stream = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json") \
                .option("cloudFiles.schemaLocation", "/Volumes/finradar/source/fraud_watchlist/schema/") \
                .option("cloudFiles.inferColumnTypes","true")
                .load(source_path)
)
# use of cloudFiles means using AUTOLOADER ; autoloader does not support just spark.read ; we have to use spark.readStream ; 

# COMMAND ----------

from pyspark.sql import functions as F

transformed_df = input_stream.select(
    "*",
    F.col("_metadata.file_path").alias("file_path"),
    F.current_timestamp().alias("ingestion_timestamp")
)

# COMMAND ----------

streaming_query = (transformed_df.writeStream.format("delta")
.outputMode("Append")
.option("checkpointLocation","/Volumes/finradar/source/fraud_watchlist/checkpoint/")
.trigger(availableNow=True)
.toTable("finradar.bronze.fraud_watchlist_batch_test")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finradar.bronze.fraud_watchlist_batch_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finradar.silver.fraud_watchlist

# COMMAND ----------

