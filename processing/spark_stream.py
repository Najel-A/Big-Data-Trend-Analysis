from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType
from dotenv import load_dotenv
import os

load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.kafka.log.level", "DEBUG") \
    .getOrCreate()

schema = StructType() \
    .add("text", StringType()) \
    .add("created_at", TimestampType()) \
    .add("sentiment", StringType()) \
    .add("entities", ArrayType(StructType([
        
    ])))

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitter_sentiment") \
    .option("startingOffsets", "earliest") \
    .load() 

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.text"),
        col("data.created_at").cast(TimestampType()),
        col("data.sentiment"),
        col("data.entities")
    )

def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
        .option("dbtable", 'twitter_sentiment') \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()

query = df_json.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()