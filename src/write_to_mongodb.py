from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create SparkSession
SPARK_MASTER = "spark://spark:7077"

# MongoDB connection
MONGODB_URI = "mongodb://root:example@mongo:27017"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("test_read") \
    .getOrCreate()

# Read data from Hadoop
df = spark.read.parquet("hdfs://namenode:9000/data/parquet")

# Write to MongoDB
df.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append") \
    .option("uri", MONGODB_URI) \
    .option("database", "infracao") \
    .option("collection", "infracao") \
    .save()

# Command to run
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /src/write_to_mongodb.py
