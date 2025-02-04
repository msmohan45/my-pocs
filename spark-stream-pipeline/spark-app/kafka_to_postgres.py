from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType


KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "my_topic"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/mydatabase"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin"
POSTGRES_TABLE = "events"

# Define Schema for Parsing Kafka Messages
schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("Datetime", LongType(), True),  # Milliseconds timestamp
            StructField("Start_Time", LongType(), True),  # Microseconds timestamp
            StructField("End_Time", LongType(), True),  # Microseconds timestamp
            StructField("Batch_No", FloatType(), True),
            StructField("Lead_Oxide", IntegerType(), True),
            StructField("Density", FloatType(), True),
            StructField("Pentration", FloatType(), True),
            StructField("Rp_Type_Mixer", StringType(), True)
        ]), True),
        StructField("source", StructType([
            StructField("ts_ms", LongType(), True),  # Event timestamp in milliseconds
            StructField("db", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True)
        ]), True),
        StructField("op", StringType(), True),  # Operation type (c, u, d, r)
        StructField("ts_ms", LongType(), True)  # Kafka event timestamp
    ]), True)
])

# schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("value", StringType(), True),
#     StructField("timestamp", StringType(), True)
# ])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON messages
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.payload.*")

# Extract Desired Fields
extracted_df = parsed_df.select(
    col("after.Datetime").alias("event_timestamp"),
    col("after.Start_Time").alias("start_time"),
    col("after.End_Time").alias("end_time"),
    col("after.Batch_No").alias("batch_no"),
    col("after.Lead_Oxide").alias("lead_oxide"),
    col("after.Density").alias("density"),
    col("after.Pentration").alias("pentration"),
    col("after.Rp_Type_Mixer").alias("mixer_type"),
    col("source.ts_ms").alias("source_event_time"),
    col("source.db").alias("database"),
    col("source.schema").alias("schema"),
    col("source.table").alias("table_name"),
    col("op").alias("operation")
)


# Convert timestamp from StringType to TimestampType
# parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
extracted_df = extracted_df.withColumn("event_timestamp", to_timestamp(from_unixtime(col("event_timestamp") / 1000)))

print("=================================================")
query_debug = extracted_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
print("=================================================")

# Write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", POSTGRES_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = extracted_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
query_debug.awaitTermination()
