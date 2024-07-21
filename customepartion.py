from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import StructType, StringType

# Step 1: Set up the Spark session
spark = SparkSession.builder.appName("CustomPartitioningExample").getOrCreate()

# Step 2: Define the schema and read the streaming data
schema = StructType().add("value", StringType())
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Step 3: Split the lines into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Step 4: Define a custom partitioner
class CustomPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions

    def getPartition(self, key):
        # Custom logic for partitioning
        return hash(key) % self.num_partitions

# Step 5: Apply the custom partitioner
num_partitions = 4  # Define the number of partitions
partitioner = CustomPartitioner(num_partitions)

def partition_key(row):
    return row['word']

partitioned_words = words.rdd.map(lambda row: (partition_key(row), row)).partitionBy(num_partitions, partitioner.getPartition).toDF()

# Step 6: Process the partitioned data
word_counts = partitioned_words.groupBy("word").count()

# Step 7: Write the output to the console
query = word_counts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
