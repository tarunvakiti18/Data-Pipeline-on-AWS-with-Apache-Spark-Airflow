from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Optional: If you're using Hadoop on Windows, set the Hadoop home directory
import os
os.environ["HADOOP_HOME"] = "D:\\hadoop"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Spark PySpark DataFrame Example") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("===Hello====")

# Load data from S3
customerdata = spark.read.load("s3://b40/dest/customer_api")
sitcount = spark.read.load("s3://b40/dest/sitecount")
total = spark.read.load("s3://b40/dest/total_amount_data")

# Display usernames
customerdata.select("username").show()

# Remove digits from usernames
rm = customerdata.withColumn("username", regexp_replace("username", r"([0-9])", ""))

# Perform joins
joindf = rm.join(sitcount, on="username", how="left") \
           .join(total, on="username", how="left")

# Write final output to S3
joindf.write.format("parquet").mode("overwrite").save("s3://b40/dest/finalcustomer")
