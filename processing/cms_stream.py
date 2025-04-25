from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType
from pyspark.sql import Row
from datetime import datetime
from dotenv import load_dotenv
import os
import hashlib
import numpy as np

# CREATE TABLE cms_estimates (
#     batch_id INTEGER,
#     timestamp TIMESTAMP,
#     keyword TEXT,
#     estimated_count INTEGER
# );

# CMS (Count-Min Sketch) algorithm
class CountMinSketch:
    def __init__(self, width, depth, seed=0):
        self.width = width  # Number of columns in the array (size of hash table)
        self.depth = depth  # Number of rows (number of hash functions)
        self.seed = seed
        self.table = np.zeros((self.depth, self.width), dtype=int)

    def _hash(self, item, i):
        """
        A simple hash function to get a position in the table.
        We use the hash of the item and then mod it to the table's width.
        """
        return (hashlib.md5((str(item) + str(i)).encode('utf-8')).hexdigest(), i)

    def _hash_index(self, item, i):
        # Hashing function for each row
        return int(self._hash(item, i)[0], 16) % self.width

    def add(self, item):
        for i in range(self.depth):
            index = self._hash_index(item, i)
            self.table[i][index] += 1

    def count(self, item):
        min_count = float('inf')
        for i in range(self.depth):
            index = self._hash_index(item, i)
            min_count = min(min_count, self.table[i][index])
        return min_count

load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysisWithCMS") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
    .config("spark.kafka.log.level", "DEBUG") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType() \
    .add("text", StringType()) \
    .add("created_at", TimestampType()) \
    .add("sentiment", StringType()) \
    .add("entities", ArrayType(StructType([])))  # Schema inside struct can be expanded later

# Define a simple Count-Min Sketch setup
cms = CountMinSketch(width=1000, depth=10)  # Adjust width and depth based on expected data

# Kafka stream setup
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitter_sentiment") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data and select relevant fields
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.text"),
        col("data.created_at").cast(TimestampType()),
        col("data.sentiment"),
        to_json(col("data.entities")).alias("entities")  # <- Serialize as JSON
    )

def write_to_postgres_with_cms(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    # Update CMS with keywords from this batch
    for row in batch_df.collect():
        keywords = row['text'].split()
        for keyword in keywords:
            cms.add(keyword)
    
    # Save original batch to main table
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
        .option("dbtable", 'twitter_sentiment') \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
    
    # Save CMS metrics for some example keywords (you can modify this)
    example_keywords = ['sports', 'politics', 'tech', 'music', 'news']
    cms_data = [Row(
        batch_id=batch_id,
        timestamp=datetime.utcnow(),
        keyword=keyword,
        estimated_count=cms.count(keyword)
    ) for keyword in example_keywords]
    
    if cms_data:
        cms_metrics_df = spark.createDataFrame(cms_data)
        cms_metrics_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
            .option("dbtable", 'cms_estimates') \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
    
    print(f"Processed batch {batch_id}")  # Debugging

# Set up the stream to write to PostgreSQL
query = df_json.writeStream \
    .foreachBatch(write_to_postgres_with_cms) \
    .outputMode("append") \
    .start()

query.awaitTermination()