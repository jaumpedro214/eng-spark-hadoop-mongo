from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create SparkSession
SPARK_MASTER = "spark://spark:7077"

# MongoDB connection
MONGODB_URI = "mongodb://root:example@mongo:27017"
MONGODB_DATABASE = "infracao"
MONGODB_COLLECTION = "contagem_rodovia"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("test_read") \
    .getOrCreate()

# Read data from Hadoop
df_infracoes = spark.read.parquet(
    "hdfs://namenode:9000/data/infracoes.parquet", 
)

# Contagem de acidentes por rodovia
df_infracoes = df_infracoes.withColumn(
    "rodovia",
    # Regex pattern to extract the highway name AA-XXX
    F.regexp_extract(F.col("auinf_local_rodovia"), r"([A-Z]{2}-\d{3})", 1)
).groupBy("rodovia").count()


# Write to MongoDB
df_infracoes.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DATABASE) \
    .option("collection", MONGODB_COLLECTION) \
    .save()

# Command to run
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /src/infractions_highway.py