from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, first, last, sha2, udf, spark_partition_id, count
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession.builder.appName("RDDtoDF").getOrCreate()
rdd = spark.sparkContext.parallelize([(1, "mango","naman"), (1, "mango","mridula"), (2, "cherry","namrata")])
df = spark.createDataFrame(rdd, ["id", "fruit","name"])

# df2= df.drop_duplicates(['id','fruit'])
# window = Window.partitionBy('id','fruit').orderBy('id')
# df_with_row = df.withColumn("row_number",row_number().over(window))


# df_duplicated= df_with_row.filter(col('row_number') == 1)
# df_duplicated.show()

# df_duplicates=df.groupBy(df.columns).agg(last("id"))
# df_duplicates.show()

# df2=df.withColumn("hash_value",sha2(col("name"),256))
# df_duplicates = df.drop_duplicates(['id'])
# df_duplicates.show()

def square(number):
    return number+number

square_udf = udf(square,IntegerType())
df_udf = df.withColumn("id_double",square(col('id')))
df_udf2=df_udf.repartition(3)
df_udf2.withColumn("partition_id",spark_partition_id()).groupby("partition_id").agg(count("partition_id")).show()



# df3= df.groupBy(['id','fruit']).count().filter(col('count')>1)
# df.show()
# df3.show()

# Don't forget to stop the Spark session
spark.stop()