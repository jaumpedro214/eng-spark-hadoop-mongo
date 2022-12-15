from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create SparkSession
SPARK_MASTER = "spark://spark:7077"

# MongoDB connection
MONGODB_URI = "mongodb://root:example@mongo:27017"
MONGODB_DATABASE = "infracao"
MONGODB_COLLECTION = "contagem_ano"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("test_read") \
    .getOrCreate()

# Read data from Hadoop
df_infracoes = spark.read.parquet(
    "hdfs://namenode:9000/data/infracoes.parquet", 
)

# Contagem de acidentes por ano
df_infracoes = df_infracoes.withColumn(
    "ano",
    # Regex pattern to extract the year
    F.regexp_extract(F.col("cometimento"), r"\d{2}\/\d{2}\/(\d{4})", 1)
).groupBy("ano").count()


# Write to MongoDB
df_infracoes.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DATABASE) \
    .option("collection", MONGODB_COLLECTION) \
    .save()

# Command to run
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /src/infractions_by_year.py