from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# Create SparkSession
SPARK_MASTER = "spark://spark:7077"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("test_read") \
    .getOrCreate()


# Read data from Hadoop
df = spark.read.csv("hdfs://namenode:9000/data/dados-abertos--jan2022.csv")

df.show()

## Run the script
# spark-submit src/test_read.py
