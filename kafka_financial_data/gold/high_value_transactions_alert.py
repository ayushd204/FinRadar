from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="finradar.gold.high_value_transactions_alert"      #this is a target table
    ,comment="Alert transactions higher than the limit for a customer"
)
def high_value_transactions_alert()->DataFrame:

    transactions = spark.readStream.table("finradar.silver.transactions")       #streaming table
    customers = spark.read.table("finradar.silver.customers")       #static table

    joined_df = (transactions.join(customers, on="customer_id", how="left")
                .filter(F.col("amount") > F.col("transaction_limit"))
                .select(
                    F.concat_ws("-",F.lit("ALERT"),F.col("transaction_id")).alias("alert_id"),
                    F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
                    F.current_timestamp().alias("alert_timestamp"),

                    transactions.transaction_id,
                    transactions.customer_id,
                    customers.email.alias("customer_email"),
                    F.concat_ws("-",F.col("first_name"),F.col("last_name")).alias("customer_name"),
                    transactions.amount.alias("transaction_amount"),
                    customers.transaction_limit,
                    transactions.currency,
                    transactions.merchant_name,
                    transactions.merchant_category,
                    transactions.transaction_type,
                    transactions.payment_channel,
                    transactions.city,
                    transactions.country,
                    transactions.is_international,
                    transactions.transaction_timestamp,
                    transactions.status,
                    )
                )
    
    return joined_df

    #this join is what detects the fraud ; 
    #inner join is not applicable as the transactions table may have data which is not there in the customers table;
    #left join is used to get all the transactions and the corresponding customer details
    # this returned df goes and sits down to the gold catalog ; 

