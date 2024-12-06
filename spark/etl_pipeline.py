from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ETL Pipeline for Customer Analytics") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Paths for input and output data in S3
INPUT_PATH = "s3a://customer-analytics-wifi-logs"
OUTPUT_PATH = "s3a://customer-analytics-transformed/wifi-logs"

# Step 1: Read Raw Data from S3
print("Reading raw data from S3...")
raw_df = spark.read.json(INPUT_PATH)

# Step 2: Clean and Transform Data
print("Cleaning and transforming data...")
cleaned_df = raw_df \
    .withColumn("timestamp", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .filter(col("session_duration") > 0)  # Remove invalid sessions

# Step 3: Write Transformed Data Back to S3 in Parquet Format
print("Writing transformed data to S3...")
cleaned_df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("ETL Pipeline Completed Successfully.")
spark.stop()
