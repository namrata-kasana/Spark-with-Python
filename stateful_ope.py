from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev, count

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Read streaming data
transactions = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "transactions").load()

# Create a windowed view
windowed_transactions = transactions.withWatermark("timestamp", "10 minutes").groupBy(window("timestamp", "5 minutes"),\
                                                                                      "user_id").agg(avg("amount").alias("avg_amount"), stddev("amount").\
                                                                                                     alias("stddev_amount"), count("*").alias("transaction_count"))

# Stateful operations using mapWithState or flatMapGroupsWithState (simplified)
fraud_detection_output = windowed_transactions.mapWithState(...)  # Implement stateful logic

# Write output to a sink
fraud_detection_output.writeStream.format("console").outputMode("append").start()
