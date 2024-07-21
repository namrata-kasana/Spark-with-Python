from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col

spark = SparkSession.builder.appName("uniquewords").getOrCreate()

df = spark.read.text("data/Moby-Dick.txt")

df_words = df.select(explode(split(df.value," ")).alias("words"))

df_count = df_words.select(col("words")).distinct().count()

print(df_count)