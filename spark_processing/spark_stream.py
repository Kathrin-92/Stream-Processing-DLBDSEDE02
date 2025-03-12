# ----------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ----------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
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

# https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres_db:5432/airquality_sensor_data") \
    .option("dbtable", "airquality_sensor_data") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
schema_str = str(df.schema)
logger.info(f"print schema: {schema_str}")
logger.info(f"Spark read postgres schema.")

# ----------------------------------------------------------------------------------------------------------------------
# HISTORICAL RECORD
# ----------------------------------------------------------------------------------------------------------------------

# write all new data points into airquality_raw table to keep historical record

# # define schema for the incoming streaming data
# json_schema = StructType([
#     StructField("station_id", IntegerType(), True),
#     StructField("component_id", IntegerType(), True),
#     StructField("datetime_from", TimestampType(), True),
#     StructField("datetime_to", TimestampType(), True),
#     StructField("value", StringType(), True)  # Adjust type based on your data
# ])
#
# df = spark.readStream \
#     .schema(json_schema) \
#     .json("api_service/sensor_data/stream_data")  # Directory where new JSON files arrive
#
# perform aggregations on incoming data points continuously and update airquality_aggregated table everytime (min, max, avg)

# if time: create threshold exceedance alerts and/or moving averages