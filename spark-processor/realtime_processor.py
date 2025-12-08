from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import logging
import os

# --- 1. Configuration (Reads from Docker Compose Environment Variables) ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092") # e.g., kafka:29092
KAFKA_TOPIC = "raw_activity_logs" 
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost") # e.g., postgres
POSTGRES_DB = os.getenv("POSTGRES_DB", "proact_log_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "proact_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "proact_password")
POSTGRES_TABLE = "processed_activity_metrics"

# Construct the JDBC URL using the service name and port
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}" 

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- 2. Spark Session Initialization ---
# Spark packages for Kafka and the PostgreSQL JDBC driver are provided via 
# the Dockerfile and the spark-submit command in docker-compose.yml.
spark = SparkSession.builder \
    .appName("ProActLogKafkaProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
logger.info("Spark Session Initialized and configured for Kafka and PostgreSQL.")


# --- 3. Complex Event Processing Logic (Categorization UDF) ---

def categorize_activity(app_name, window_title):
    """Categorizes the activity based on application name and window title."""
    app_lower = app_name.lower() if app_name else ""
    title_lower = window_title.lower() if window_title else ""
    
    # Phase 2 categorization logic
    if "code" in app_lower or "terminal" in app_lower or "github" in title_lower:
        return "Development"
    if "slack" in app_lower or "zoom" in app_lower:
        return "Communication"
    if "youtube" in title_lower:
        return "Browsing (YouTube)"
    if "chrome" in app_lower or "firefox" in app_lower:
        return "Browsing"
    return "Uncategorized"

# Register the UDF
categorize_udf = udf(categorize_activity, StringType())


# --- 4. Data Ingestion (Kafka Consumer) ---

# Define the schema for the JSON value coming from Kafka
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("app_name", StringType(), True),
    StructField("window_title", StringType(), True),
])

# Read stream from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize the value, apply categorization, and cast timestamp
raw_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) # Convert ISO string to TimestampType

# Apply the categorization UDF
processed_df = raw_df.withColumn(
    "category", 
    categorize_udf(col("app_name"), col("window_title"))
)

# Select the final columns to be written to the database
final_df = processed_df.select("timestamp", "app_name", "window_title", "category")


# --- 5. Output to PostgreSQL Data Store (Sink) ---

def write_to_postgres(current_df, batch_id):
    """Writes the micro-batch data to the PostgreSQL reporting table."""
    # current_df.count() is an action, so we only call it once for efficiency
    count = current_df.count() 
    if count > 0:
        logger.info(f"Writing Batch {batch_id} with {count} records to PostgreSQL...")
        
        # Write to PostgreSQL using JDBC connector
        current_df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"Batch {batch_id} successfully saved to PostgreSQL.")


# Start the streaming query
query = final_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/app/spark_checkpoint/proactlog_processor") \
    .start()

query.awaitTermination()