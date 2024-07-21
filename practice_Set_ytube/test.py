from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType

spark = SparkSession.builder.appName("cuatomer_update").getOrCreate()

df = spark.read.csv("../data/tran.csv",header=True,inferSchema=True)
# df.show()

def calculate_final_sum (transaction_type,transaction_amt):
    total = 0
    if transaction_type == 'credit':
        total+=transaction_amt
    if transaction_type == 'debit':
        total -= transaction_amt
    return total
calculate_final_sum_udf = udf(calculate_final_sum, IntegerType())

df_total = df.groupby(df["customer_id"],df["transaction_type"]).agg(sum(col('transaction_amoun')).alias('sum')).select(['customer_id','sum',"transaction_type"])


df_final = df_total.withColumn("final_sum",calculate_final_sum_udf(df_total["transaction_type"],df_total["sum"]))
df_final.groupby(df["customer_id"]).agg(sum(df_final['final_sum'])).show()

# list_col=['col1','col2','col3']
# list_final = list-list[:1:-1]
#"total",calculate(df['transaction_type'],df['transaction_amoun'])).show()

