from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, lit, exp
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType
from dotenv import load_dotenv
import os
from datetime import datetime
import math

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
    .add("entities", ArrayType(StructType([])))

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitter_sentiment") \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.text"),
        col("data.created_at").cast(TimestampType()),
        col("data.sentiment"),
        to_json(col("data.entities")).alias("entities")
    )

# Exponential decay parameters
DECAY_RATE = 0.1  # The higher the faster the decay rate
HALF_LIFE = 60    # Time in seconds for weight to halve

def write_to_postgres_with_decay(batch_df, batch_id):
    # Timestamp in seconds
    current_time = datetime.now().timestamp()
    
    # Calculate time difference in seconds and apply exponential decay
    decayed_df = batch_df.withColumn(
        "time_diff_sec", 
        (lit(current_time) - col("created_at").cast("double"))
    ).withColumn(
        "weight", 
        exp(-lit(DECAY_RATE) * col("time_diff_sec") / lit(HALF_LIFE))
    )

    # Log the first 5 rows (for debugging)
    print(f"Batch {batch_id} - Sample Data:")
    decayed_df.show(5, truncate=False)
    
    # Optional: Apply weights to sentiment scores (example)
    weighted_df = decayed_df.withColumn(
        "weighted_sentiment",
        col("weight") * \
        when(col("sentiment") == "positive", 1)
         .when(col("sentiment") == "negative", -1)
         .otherwise(0)
    )
    
    # Add processing metadata
    result_df = weighted_df.withColumn("processing_time", lit(current_time)) \
                          .withColumn("batch_id", lit(batch_id))
    
    # Write to PostgreSQL
    result_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
        .option("dbtable", 'twitter_sentiment_edw') \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()

query = df_json.writeStream \
    .foreachBatch(write_to_postgres_with_decay) \
    .outputMode("append") \
    .start()

query.awaitTermination()
