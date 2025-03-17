# ----------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ----------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_unixtime, hash, max, min, avg, window
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, TimestampType
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
logger.info(f"Step (2): Testing connection to PostgreSQL database...")
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

logger.info(f"Step (3): Creating Spark Stream for incoming raw streaming data. Start processing...")

# define json data schema for the incoming streaming data
json_schema = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("datetime_from", IntegerType(), True),
    StructField("datetime_to", IntegerType(), True),
    StructField("component_id", IntegerType(), True),
    StructField("value", FloatType(), True),
    StructField("timestamp_str", StringType(), True)
])
logger.info(f"... successfully defined json schema for streaming data.")

stream_df_base = spark.readStream.schema(json_schema).json("/api_service/sensor_data/stream_data")
logger.info(f"... successfully loaded raw json file.")

logger.info(f"... start preprocessing data.")
cast_date_from_timestamp = stream_df_base.withColumn('datetime_from', from_unixtime(col("datetime_from")).cast(TimestampType()))
cast_date_to_timestamp = cast_date_from_timestamp.withColumn('datetime_to', from_unixtime(col("datetime_to")).cast(TimestampType()))
with_ingestion_ts = cast_date_to_timestamp.withColumn("ingestion_timestamp", current_timestamp())
with_metadata = with_ingestion_ts.join(airquality_metadata, with_ingestion_ts.component_id == airquality_metadata.id, "left")
with_renamed_columns = with_metadata.withColumnsRenamed({
    "component_id": "pollutant_id",
    "name": "pollutant_name",
    "symbol": "pollutant_symbol",
    "datetime_from": "timestamp_start",
    "datetime_to": "timestamp_end"
})
with_id = with_renamed_columns.withColumn("record_id", hash("station_id", "pollutant_id", "value", "timestamp_start", "ingestion_timestamp"))
logger.info(f"... successfully preprocessed data. Converted data types, added new columns, and joined metadata.")

df_airquality_raw = with_id.select (
    col("record_id"),
    col("station_id"),
    col("pollutant_id"),
    col("pollutant_name"),
    col("pollutant_symbol"),
    col("unit"),
    col("value"),
    col("timestamp_start"),
    col("timestamp_end"),
    col("ingestion_timestamp")
)
logger.info(f"... selected necessary data columns to write to PostgreSQL.")

query_raw = df_airquality_raw.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_db:5432/airquality_sensor_data") \
        .option("dbtable", "airquality_raw") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()) \
    .start()
logger.info(f"Step (4): Writing streaming data to airquality_raw PostgreSQL table...")

# ----------------------------------------------------------------------------------------------------------------------
# AGGREGATED DATA
# ----------------------------------------------------------------------------------------------------------------------

logger.info(f"Step (5): Start aggregating data...")
aggregated_data = (df_airquality_raw
    .withWatermark("ingestion_timestamp", "10 minutes")
    .groupBy("pollutant_id", "pollutant_name", "pollutant_symbol", "unit", window(col("ingestion_timestamp"), "5 minutes", "1 minute"))
    .agg(
        max(col("value")).alias("max_value"),
        min(col("value")).alias("min_value"),
        avg(col("value")).alias("avg_value"),
))
with_last_updated = aggregated_data.withColumn("last_updated", current_timestamp())
logger.info(f"... aggregated data successfully.")

df_airquality_aggregated = with_last_updated.select (
    col("pollutant_id"),
    col("pollutant_name"),
    col("pollutant_symbol"),
    col("unit"),
    col("max_value"),
    col("min_value"),
    col("avg_value"),
    col("last_updated")
)
logger.info(f"... selected necessary data columns to write to PostgreSQL.")
logger.info("test new 18:05")

# to do: this does not work, always 0 rows in the table
# query_agg = df_airquality_aggregated.writeStream \
#     .foreachBatch(lambda batch_df, batch_id: batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://postgres_db:5432/airquality_sensor_data") \
#         .option("dbtable", "airquality_aggregated") \
#         .option("user", "postgres") \
#         .option("password", "password") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("overwrite") \
#         .save()) \
#     .start()
# logger.info(f"Step (6): Writing aggregated streaming data to airquality_aggregated PostgreSQL table...")

spark.streams.awaitAnyTermination()
