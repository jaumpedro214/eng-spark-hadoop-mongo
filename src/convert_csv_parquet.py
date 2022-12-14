from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create SparkSession
SPARK_MASTER = "spark://spark:7077"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("test_read") \
    .getOrCreate()

# Read data from Hadoop
df = spark.read.csv(
    "hdfs://namenode:9000/data/*.csv", 
    header=True, 
    inferSchema=True, 
    sep=";"
)

# Show data
df.show()

# Convert to Parquet
df.write\
    .mode("overwrite")\
    .parquet("hdfs://namenode:9000/data/infracoes.parquet")
