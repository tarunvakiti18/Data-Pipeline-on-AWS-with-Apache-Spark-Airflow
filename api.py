from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, expr
import requests

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("first") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Fetch data from API
url = "https://randomuser.me/api/0.8/?results=1000"
urldata = requests.get(url).text

# Convert JSON string into DataFrame
df = spark.read.json(spark.sparkContext.parallelize([urldata]))

# Explode the 'results' array
exploded_df = df.withColumn("results", expr("explode(results)"))

# Show data and schema
exploded_df.show()
exploded_df.printSchema()

# Flatten nested structure
flatten_df = exploded_df.select(
    "nationality",
    "results.user.cell",
    "results.user.dob",
    "results.user.email",
    "results.user.gender",
    "results.user.location.city",
    "results.user.location.state",
    "results.user.location.street",
    "results.user.location.zip",
    "results.user.md5",
    "results.user.name.first",
    "results.user.name.last",
    "results.user.name.title",
    "results.user.password",
    "results.user.phone",
    "results.user.picture.large",
    "results.user.picture.medium",
    "results.user.picture.thumbnail",
    "results.user.registered",
    "results.user.salt",
    "results.user.sha1",
    "results.user.sha256",
    "results.user.username",
    "seed",
    "version"
)

# Show flattened data and schema
flatten_df.show()
flatten_df.printSchema()

# Write to Parquet (S3 path; replace with a local path if testing locally)
flatten_df.write.format("parquet").mode("overwrite").save("s3://b40/dest/customer_api")
