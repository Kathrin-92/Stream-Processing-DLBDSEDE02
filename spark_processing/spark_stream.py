# ----------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ----------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, monotonically_increasing_id, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import logging
import os

# ----------------------------------------------------------------------------------------------------------------------
# SET UP CONTAINER-SPECIFIC LOGGING
# ----------------------------------------------------------------------------------------------------------------------

# creating log file
log_path = '/usr/src/spark_processing'
log_filename = os.path.join(log_path, 'spark_processing_logs.log')

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# ----------------------------------------------------------------------------------------------------------------------
# SETUP SPARK SESSION
# ----------------------------------------------------------------------------------------------------------------------

# create a local SparkSession, the starting point of all functionalities related to Spark
spark = SparkSession.builder \
        .appName("AirQualityData") \
        .config("spark.streaming.stopGracefullyonShutdown", True) \
        .config("spark.sql.sources.default", "json") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()
logger.info(f"Step (1): Spark Streaming Session launched.")

# test the connection and let spark read the db schema from the airquality_raw table
# https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_db:5432/airquality_sensor_data") \
    .option("dbtable", "airquality_raw") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

schema_str = str(df.schema)
logger.info(f"print schema: {schema_str}")
logger.info(f"Connection to PostgreSQL successfull! Spark read table schema from airquality_raw.")

# ----------------------------------------------------------------------------------------------------------------------
# READ METADATA
# ----------------------------------------------------------------------------------------------------------------------

airquality_metadata = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_db:5432/airquality_sensor_data") \
    .option("dbtable", "airquality_metadata") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

logger.info(f"Connection to PostgreSQL successfull! Spark read meatadata info on airquality data.")

# ----------------------------------------------------------------------------------------------------------------------
# HISTORICAL RECORD
# ----------------------------------------------------------------------------------------------------------------------

# define json data schema for the incoming streaming data
json_schema = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("datetime_from", IntegerType(), True),
    StructField("datetime_to", IntegerType(), True),
    StructField("component_id", IntegerType(), True),
    StructField("value", FloatType(), True),
    StructField("timestamp_str", StringType(), True)
])

logger.info(f"Define json schema for streaming data.")

stream_df_base = spark.readStream.schema(json_schema).json("/api_service/sensor_data/stream_data")
logger.info(f"Load json file.")

cast_date_from_timestamp = stream_df_base.withColumn('datetime_from', from_unixtime(col("datetime_from")).cast(TimestampType()))
cast_date_to_timestamp = stream_df_base.withColumn('datetime_to', from_unixtime(col("datetime_to")).cast(TimestampType()))
logger.info(f"Cast date to timestamp.")

with_id = cast_date_to_timestamp.withColumn("record_id", monotonically_increasing_id())
with_ingestion_ts = with_id.withColumn("ingestion_timestamp", current_timestamp())
logger.info(f"Add new columns.")

with_metadata = with_ingestion_ts.join(airquality_metadata, with_ingestion_ts.component_id == airquality_metadata.id, "left")
logger.info(f"Join metadata.")

with_renamed_columns = with_metadata.withColumnsRenamed({
    "component_id": "pollutant_id",
    "name": "pollutant_name",
    "datetime_from": "timestamp_start",
    "datetime_to": "timestamp_end"
})
logger.info(f"Rename columns.")

df_airquality_raw = with_renamed_columns.select (
    col("record_id"),
    col("station_id"),
    col("pollutant_id"),
    col("pollutant_name"),
    col("unit"),
    col("value"),
    col("timestamp_start"),
    col("timestamp_end"),
    col("ingestion_timestamp")
)
logger.info(f"Final select for df.")


# perform aggregations on incoming data points continuously and update airquality_aggregated table everytime (min, max, avg)

# if time: create threshold exceedance alerts and/or moving averages