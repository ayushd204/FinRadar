from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="finradar.gold.transaction_count_by_minute_sliding_window"      #this is a target table
    ,comment="Find the transaction counts in the streaming data "
)
def transaction_count_by_minute()->DataFrame:

    transactions = spark.readStream.table("finradar.silver.transactions")       #streaming table


    transactions_with_watermark = transactions.withWatermark("transaction_timestamp", "5 minutes")
    
    transaction_count_df = transactions_with_watermark.groupBy(
        F.window("transaction_timestamp", "5 minute","1 minute")    
        # this is sliding window, the window size being 5 minute and the slide interval being 1 minute
    ).agg(
        F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )

    return transaction_count_df
    