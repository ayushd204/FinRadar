# Databricks notebook source
import json

kafka_connection_json = dbutils.secrets.get(scope="finradar_scope",key="kafka_connection_details")
kafka_config = json.loads(kafka_connection_json)
bootstrap_servers = kafka_config["bootstrap_servers"]
topic = kafka_config["topic"]
api_key = kafka_config["api_key"]
api_secret = kafka_config["api_secret"]

# COMMAND ----------

jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";' 
# java auth and authorization service

# COMMAND ----------

# first read the data from the kafka in terms in "BATCH" and see what changes to read it in terms of streaming ;

sample_batch = (spark.read.format("kafka")
                .option("kafka.bootstrap.servers",bootstrap_servers)
                .option("subscribe",topic)
                # all of the options below are for general auth used by kafka
                .option("kafka.security.protocol","SASL_SSL")
                .option("kafka.sasl.mechanism","PLAIN")
                .option("kafka.sasl.jaas.config",jaas_config)
                .option("startingOffsets","earliest")
                .load()
)

# COMMAND ----------

# sample_batch.count()
sample_batch.display()

# COMMAND ----------

from pyspark.sql.functions import col

parsed_batch = sample_batch.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)
#casting "key" column to string & "value" column to string cause they were in binaries earlier ; 

# COMMAND ----------

display(parsed_batch)

# COMMAND ----------

parsed_batch.write.saveAsTable("finradar.bronze.transactions_batch_test2")

# COMMAND ----------

# MAGIC %md
# MAGIC The code starting from the cell below performs reading and writing stream data e2e ; 

# COMMAND ----------

streaming_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers",bootstrap_servers)
                .option("subscribe",topic)
                # all of the options below are for general auth used by kafka
                .option("kafka.security.protocol","SASL_SSL")
                .option("kafka.sasl.mechanism","PLAIN")
                .option("kafka.sasl.jaas.config",jaas_config)
                .option("startingOffsets","earliest")
                .load()
)
#the difference in reading streaming data vs batch is simply the "READSTREAM" function;
#also display.streaming_df does not work in streaming data cause there has to be conttinuous read and write ;

# COMMAND ----------

from pyspark.sql.functions import col

parsed_streaming = streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

streaming_query = (parsed_streaming.writeStream.format("delta")
.outputMode("Append")
.option("checkpointLocation","/Volumes/finradar/source/transactions/checkpoint/")
.trigger(availableNow=True)
.toTable("finradar.bronze.transactions_streaming_test")
)
#this code says to load all the data that is availableNow ; it checks at the checkpoint and sees where it left off 
#Note: this is pyspark being used ; "spark streaming" which means whichever platforms like mircrosoft synapse analytics or fabric is used or even open source spark is used, spark streaming can be used ; 

"""
parsed_batch.write.saveAsTable("finradar.bronze.transactions_batch_test") for batch vs 
multiple options in terms of write stream because 
There is no streaming checkpoint or streaming trigger because Spark is not managing an ongoing stream.
"""

# COMMAND ----------

