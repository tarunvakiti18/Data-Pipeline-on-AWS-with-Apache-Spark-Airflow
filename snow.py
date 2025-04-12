from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("first") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("===Hello====")

# Snowflake connection options
import os

snowflake_options = {
    "sfURL": os.getenv("SF_URL"),
    "sfAccount": os.getenv("SF_ACCOUNT"),
    "sfUser": os.getenv("SF_USER"),
    "sfPassword": os.getenv("SF_PASSWORD"),
    "sfDatabase": os.getenv("SF_DATABASE"),
    "sfSchema": os.getenv("SF_SCHEMA"),
    "sfRole": os.getenv("SF_ROLE"),
    "sfWarehouse": os.getenv("SF_WAREHOUSE"),
    "dbtable": os.getenv("SF_DBTABLE")
}

# Read data from Snowflake
snowdf = spark.read \
    .format("snowflake") \
    .options(**snowflake_options) \
    .load()

# Group by username and count sites
sitecount = snowdf.groupBy("username").agg(count("site").alias("site_cnt"))

# Write output to Parquet (S3 path)
sitecount.write.format("parquet").mode("overwrite").save("s3://b40/dest/sitecount")
