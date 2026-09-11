"""
Spark Structured Streaming Module.
Processes streaming IoT telemetry landed in iot_landing/ with PySpark Structured Streaming.
"""

import os
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql.functions import col, from_unixtime, to_timestamp, window, avg, max, min

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LANDING_DIR = os.path.join(ROOT_DIR, "iot_landing")
CHECKPOINT_DIR = os.path.join(ROOT_DIR, "checkpoints", "spark_streaming")


def get_telemetry_schema() -> Any:
    """Returns the expected schema of the landed IoT JSON events."""
    return StructType([
        StructField("room_temperature", DoubleType(), True),
        StructField("room_humidity", DoubleType(), True),
        StructField("outdoor_temperature", DoubleType(), True),
        StructField("outdoor_humidity", DoubleType(), True),
        StructField("location_lattitude", DoubleType(), True),
        StructField("location_longitude", DoubleType(), True),
        StructField("AQI", DoubleType(), True),
        StructField("AirCondition_Status", IntegerType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("_topic", StringType(), True),
        StructField("_landed_at", DoubleType(), True),
    ])


def create_spark_session(app_name: str = "CPE371_IoT_SparkStreaming") -> Any:
    """Creates a local SparkSession configured for streaming processing."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def process_stream(landing_dir: str = LANDING_DIR, checkpoint_dir: str = CHECKPOINT_DIR):
    """Initializes and runs the structured streaming job."""
    spark = create_spark_session()
    schema = get_telemetry_schema()

    print(f"[*] Reading streaming JSON files from: {landing_dir}")
    raw_stream = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 5)
        .json(landing_dir)
    )

    # Convert epoch timestamp to SQL Timestamp for windowing
    stream_df = raw_stream.withColumn(
        "event_time", to_timestamp(from_unixtime(col("timestamp")))
    )

    # Example Windowed Aggregation (Watermarked 10-minute window)
    windowed_metrics = (
        stream_df.withWatermark("event_time", "1 minute")
        .groupBy(
            window(col("event_time"), "30 seconds", "15 seconds"),
            col("_topic"),
        )
        .agg(
            avg("room_temperature").alias("avg_room_temp"),
            avg("room_humidity").alias("avg_room_hum"),
            avg("AQI").alias("avg_aqi"),
            max("outdoor_temperature").alias("max_outdoor_temp"),
        )
    )

    query = (
        windowed_metrics.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    print("[*] Streaming query active. Awaiting termination...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[!] Streaming query stopped by user.")
        query.stop()


if __name__ == "__main__":
    process_stream()