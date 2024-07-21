# Here's a Spark example demonstrating how to use salting to address skewed data distribution:
#
# Scenario:
#
# Imagine you have a DataFrame containing user website activity data.
# The user_id column might be skewed, with a few users having a disproportionately high number of activities compared to others.
# This skewness can lead to uneven workload distribution during joins or aggregations.
#
# Salting Approach:
#
# Identify the Skewed Column: Analyze your DataFrame to identify the column with skewed data (e.g., user_id in this case).
#
# Add a Random Salt:
#
# Generate a random number for each partition in your DataFrame.
# Create a new column named "salt" containing these random numbers.
# Combine Salt with Skewed Key:
#
# Create a new column (e.g., "salted_user_id") by concatenating the original skewed column (user_id) with the "salt" column.
# Repartition Data:
#
# Repartition the DataFrame based on the "salted_user_id" column. This ensures a more balanced distribution of data across partitions, mitigating the skewness issue.
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import rand, col, concat
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, concat, col, lit

spark = SparkSession.builder.getOrCreate()

# Sample DataFrame with skewed user activity data
data = [
    (1, "item1"), (1, "item2"), (1, "item3"),  # User 1 has many activities
    (2, "item4"), (3, "item5")
]

df = spark.createDataFrame(data, ["user_id", "activity"])

# Identify skewed column (assuming user_id is skewed)
skewed_col = "user_id"

# Generate random salt for each partition
salt = rand(seed=10) * 100  # Adjust multiplier for salting range
print(salt)

# Create salted user ID
salted_user_id = concat(col(skewed_col), lit("_"), salt.cast("int"))

# Repartition data using salted user ID (adjust numPartitions as needed)
repartitioned_df = df.repartition(4, salted_user_id)

# You can now perform joins or aggregations on repartitioned_df

spark.stop()