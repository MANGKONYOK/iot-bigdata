"""
Spark Structured Streaming Module.
Processes streaming IoT telemetry landed in iot_landing/ with PySpark Structured Streaming.
Computes windowed aggregations for Indoor and Outdoor spatial zones.
"""

import argparse
import os
import sys
from typing import Any, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql.functions import (
    col,
    to_timestamp,
    window,
    avg,
    max,
    min,
    count,
    round as spark_round,
    when,
    coalesce,
    lit,
)

# Spatial Categorization Constants
INDOOR_ROOMS: List[str] = [
    "living",
    "kitchen",
    "laundry",
    "wine_cellar",
    "gym",
    "guest_bedroom",
    "dining",
    "bath",
    "guest_bath",
    "hall",
]

OUTDOOR_ROOMS: List[str] = [
    "terrace",
    "patio",
    "garage",
]

# Default Paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LANDING_DIR = os.path.join(ROOT_DIR, "iot_landing")
DEFAULT_CHECKPOINT_DIR = os.path.join(ROOT_DIR, "checkpoints", "spark_iot_metrics")


def get_telemetry_schema() -> StructType:
    """
    Defines explicit StructType schema matching publisher & ingestion bridge payload format.
    """
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("room", StringType(), True),
        StructField("location_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("ac_status", IntegerType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("_topic", StringType(), True),
        StructField("_qos", IntegerType(), True),
        StructField("_received_at", StringType(), True),
        StructField("_landed_at", DoubleType(), True),
    ])


def create_spark_session(app_name: str = "CPE371_IoT_SparkStreaming") -> SparkSession:
    """
    Creates and configures a local SparkSession optimized for micro-batch streaming.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def apply_spatial_categorization(df: DataFrame) -> DataFrame:
    """
    Categorizes sensor records into Indoor and Outdoor spatial zones
    based on the room name or explicit location_type metadata.
    """
    return df.withColumn(
        "spatial_zone",
        when(col("room").isin(INDOOR_ROOMS), lit("Indoor"))
        .when(col("room").isin(OUTDOOR_ROOMS), lit("Outdoor"))
        .otherwise(
            when(col("location_type").isNotNull(), col("location_type")).otherwise(lit("Unspecified"))
        ),
    )


def compute_windowed_aggregations(
    stream_df: DataFrame,
    window_duration: str = "10 seconds",
    slide_duration: str = "10 seconds",
) -> DataFrame:
    """
    Computes 10-second tumbling/sliding windowed averages and metrics
    grouped by spatial zone (Indoor / Outdoor) and room.
    """
    # 1. Parse event_time string to TimestampType
    parsed_df = stream_df.withColumn(
        "timestamp",
        coalesce(
            to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("_received_at"), "yyyy-MM-dd HH:mm:ss"),
        ),
    )

    # 2. Add spatial categorization
    categorized_df = apply_spatial_categorization(parsed_df)

    # 3. Compute 10-second windowed aggregations
    windowed_df = (
        categorized_df.groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("spatial_zone"),
            col("room"),
        )
        .agg(
            spark_round(avg("temperature"), 2).alias("avg_temperature"),
            spark_round(avg("humidity"), 2).alias("avg_humidity"),
            spark_round(avg("aqi"), 1).alias("avg_aqi"),
            spark_round(max("temperature"), 2).alias("max_temperature"),
            spark_round(min("temperature"), 2).alias("min_temperature"),
            count("event_id").alias("event_count"),
        )
    )

    return windowed_df


def process_stream(
    landing_dir: str = DEFAULT_LANDING_DIR,
    checkpoint_dir: str = DEFAULT_CHECKPOINT_DIR,
    window_duration: str = "10 seconds",
    slide_duration: str = "10 seconds",
    trigger_interval: str = "5 seconds",
    output_mode: str = "update",
):
    """
    Initializes and runs the structured streaming pipeline from landing directory to console sink.
    """
    os.makedirs(landing_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)

    spark = create_spark_session()
    schema = get_telemetry_schema()

    print("=" * 70)
    print(" CPE371 Big Data IoT - PySpark Structured Streaming Pipeline")
    print("=" * 70)
    print(f"[*] Landing Directory   : {landing_dir}")
    print(f"[*] Checkpoint Directory: {checkpoint_dir}")
    print(f"[*] Window Duration     : {window_duration}")
    print(f"[*] Slide Duration      : {slide_duration}")
    print(f"[*] Trigger Interval    : {trigger_interval}")
    print(f"[*] Output Mode         : {output_mode}")
    print("=" * 70)

    # Ingest continuous JSON micro-batches using spark.readStream
    raw_stream = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 10)
        .json(landing_dir)
    )

    # Apply spatial categorization and 10s windowed aggregations
    aggregated_metrics = compute_windowed_aggregations(
        raw_stream,
        window_duration=window_duration,
        slide_duration=slide_duration,
    )

    # Write streaming updates to Console Sink
    query = (
        aggregated_metrics.writeStream.outputMode(output_mode)
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    print("[*] Streaming query active. Monitoring for incoming micro-batches...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[!] Streaming query stopped by user (KeyboardInterrupt).")
        query.stop()
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="PySpark Structured Streaming Analytics for IoT Telemetry"
    )
    parser.add_argument(
        "--landing-dir",
        default=DEFAULT_LANDING_DIR,
        help=f"Directory where JSON files land (default: {DEFAULT_LANDING_DIR})",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=DEFAULT_CHECKPOINT_DIR,
        help=f"Directory for streaming checkpoints (default: {DEFAULT_CHECKPOINT_DIR})",
    )
    parser.add_argument(
        "--window",
        default="10 seconds",
        help="Aggregation window duration (default: '10 seconds')",
    )
    parser.add_argument(
        "--slide",
        default="10 seconds",
        help="Aggregation slide duration (default: '10 seconds')",
    )
    parser.add_argument(
        "--trigger",
        default="5 seconds",
        help="Micro-batch trigger interval (default: '5 seconds')",
    )
    parser.add_argument(
        "--output-mode",
        choices=["update", "complete", "append"],
        default="update",
        help="Streaming output mode (default: update)",
    )

    args = parser.parse_args()

    process_stream(
        landing_dir=args.landing_dir,
        checkpoint_dir=args.checkpoint_dir,
        window_duration=args.window,
        slide_duration=args.slide,
        trigger_interval=args.trigger,
        output_mode=args.output_mode,
    )


if __name__ == "__main__":
    main()