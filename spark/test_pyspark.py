from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

# Test SparkSession creation
spark = SparkSession.builder.appName("Test").getOrCreate()
print("PySpark is working correctly!")
