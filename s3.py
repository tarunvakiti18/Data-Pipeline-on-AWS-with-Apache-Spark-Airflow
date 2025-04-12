from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.types import IntegerType
import sys

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("first") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("===Hello====")

# Read data from S3
df = spark.read.load("s3://b40/src")

# Group by username and sum the amount
aggdf = df.groupBy("username").agg(sum(df["amount"].cast(IntegerType())).alias("total"))

# Show the result
aggdf.show()

# Read the destination folder from command-line argument
path = sys.argv[1]

# Write output to the given S3 path
aggdf.write.mode("overwrite").save(f"s3://b40/{path}")
