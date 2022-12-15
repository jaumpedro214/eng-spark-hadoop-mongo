from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create SparkSession
SPARK_MASTER = "spark://spark:7077"

# MongoDB connection
MONGODB_URI = "mongodb://root:example@mongo:27017"
MONGODB_DATABASE = "infracao"
MONGODB_COLLECTION = "gravidade_mes"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("test_read") \
    .getOrCreate()

# Read data from Hadoop
df_infracoes = spark.read.parquet(
    "hdfs://namenode:9000/data/infracoes.parquet", 
)

# grav_tipo   -> Gravidade da infração
# cometimento -> Data da infração

# Contar o número de infrações de cada gravidade por mês e ano
df_infracoes = (
    df_infracoes
    .select(
        # convert to date
        F.to_date(F.col("cometimento"), "dd/MM/yyyy").alias("data"),
        F.col("grav_tipo")
    )
    .withColumn(
        "ano-mes",
        F.concat(
            F.year(F.col("data")),
            F.lit("-"),
            F.month(F.col("data"))
        )
    )
    .groupBy(
        "ano-mes",
        "grav_tipo"
    )
    .agg(
        F.count("*").alias("count")
    )
)

# Show data
df_infracoes.show()

# Write to MongoDB
df_infracoes.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DATABASE) \
    .option("collection", MONGODB_COLLECTION) \
    .save()

# Command to run
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /src/infration_type_percentual.py