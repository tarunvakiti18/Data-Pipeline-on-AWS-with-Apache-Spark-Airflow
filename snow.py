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
snowflake_options = {
    "sfURL": "https://roazjrj-sk70838.snowflakecomputing.com",
    "sfAccount": "roazjrj",
    "sfUser": "sivavasusaia",
    "sfPassword": "Aditya908",
    "sfDatabase": "zeyodb",
    "sfSchema": "zeyoschema",
    "sfRole": "ACCOUNTADMIN",
    "sfWarehouse": "COMPUTE_WH",
    "dbtable": "srctab"
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
