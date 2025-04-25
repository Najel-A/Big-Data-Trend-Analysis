from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, lit, exp, when
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType
from dotenv import load_dotenv
import os
from datetime import datetime
import math

# Make this table; Fix this to match
# CREATE TABLE twitter_sentiment_edw (
#     text TEXT,
#     created_at TIMESTAMP,
#     sentiment TEXT,
#     entities JSONB,
#     weight DOUBLE PRECISION,
#     weighted_sentiment DOUBLE PRECISION,
#     processing_time TIMESTAMP,
#     batch_id BIGINT
# );

# CREATE TABLE processing_errors (
#     batch_id BIGINT,
#     error TEXT,
#     error_time TIMESTAMP
# );

load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Initialize Spark with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("TwitterSentimentEDW") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
    .config("spark.kafka.log.level", "DEBUG") \
    .getOrCreate()

# Define schema for incoming tweets, modify this as needed to match table schema
schema = StructType() \
    .add("text", StringType()) \
    .add("created_at", TimestampType()) \
    .add("sentiment", StringType()) \
    .add("entities", ArrayType(StructType([])))

# Kafka stream setup
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitter_sentiment") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data
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
    try:
        if batch_df.isEmpty():
            print(f"Batch {batch_id} is empty - skipping")
            return

        # Get current timestamp in seconds
        current_time = datetime.now().timestamp()
        
        # Calculate time difference and apply exponential decay
        decayed_df = batch_df.withColumn(
            "time_diff_sec", 
            (lit(current_time) - col("created_at").cast("double"))
        ).withColumn(
            "weight", 
            exp(-lit(DECAY_RATE) * col("time_diff_sec") / lit(HALF_LIFE))
        )

        # Debug logging
        print(f"\n=== Processing Batch {batch_id} ===")
        print("Sample of decayed data:")
        decayed_df.show(5, truncate=False)

        # Convert sentiment to numerical score with weights
        weighted_df = decayed_df.withColumn(
            "sentiment_score",
            when(col("sentiment") == "positive", 1.0)
             .when(col("sentiment") == "negative", -1.0)
             .otherwise(0.0)
        ).withColumn(
            "weighted_sentiment",
            col("weight") * col("sentiment_score")
        )

        # Add processing metadata
        result_df = weighted_df.withColumn("processing_time", lit(datetime.now())) \
                              .withColumn("batch_id", lit(batch_id)) \
                              .drop("time_diff_sec", "sentiment_score")

        # Debug final output
        print("Final DataFrame to be written:")
        result_df.show(5, truncate=False)

        # Write to PostgreSQL
        print(f"Writing batch {batch_id} to PostgreSQL...")
        result_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
            .option("dbtable", 'twitter_sentiment_edw') \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
        
        print(f"Successfully wrote batch {batch_id} to EDW")

    except Exception as e:
        print(f"ERROR processing batch {batch_id}: {str(e)}")
        # Optionally write errors to a separate table
        error_df = spark.createDataFrame([(batch_id, str(e), datetime.now())], 
                                       ["batch_id", "error", "error_time"])
        error_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
            .option("dbtable", 'processing_errors') \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()

# Start the streaming query
query = df_json.writeStream \
    .foreachBatch(write_to_postgres_with_decay) \
    .outputMode("append") \
    .start()

query.awaitTermination()