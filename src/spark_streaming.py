"""
Spark Structured Streaming Module.
Processes streaming IoT telemetry landed in iot_landing/ with PySpark Structured Streaming.
Computes windowed aggregations with event-time watermarking and threshold-based anomaly alerts.
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
)
from pyspark.sql.functions import (
    col,
    try_to_timestamp,
    window,
    avg,
    max as spark_max,
    min as spark_min,
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
    "bedroom",
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

# Default Paths & Thresholds
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LANDING_DIR = os.path.join(ROOT_DIR, "iot_landing")
DEFAULT_CHECKPOINT_DIR = os.path.join(ROOT_DIR, "checkpoints", "spark_iot_metrics")
DEFAULT_TEMP_ALERT_THRESHOLD = 35.0
DEFAULT_HUM_ALERT_THRESHOLD = 70.0


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
    watermark_delay: str = "30 seconds",
    temp_alert_threshold: float = DEFAULT_TEMP_ALERT_THRESHOLD,
    hum_alert_threshold: float = DEFAULT_HUM_ALERT_THRESHOLD,
    group_by_room: bool = False,
) -> DataFrame:
    """
    Computes 10-second tumbling/sliding windowed averages and metrics with watermarking
    and anomaly detection status classification.
    """
    # 1. Parse event_time string to TimestampType.
    # try_to_timestamp (not to_timestamp) is required: Spark 4 enables ANSI mode by
    # default, where to_timestamp RAISES on malformed input instead of returning NULL.
    # That made the coalesce fallback below unreachable and let a single malformed
    # event_time abort the whole streaming query.
    parsed_df = stream_df.withColumn(
        "timestamp",
        coalesce(
            try_to_timestamp(col("event_time"), lit("yyyy-MM-dd HH:mm:ss")),
            try_to_timestamp(col("_received_at"), lit("yyyy-MM-dd HH:mm:ss")),
        ),
    )

    # 2. Add spatial categorization
    categorized_df = apply_spatial_categorization(parsed_df)

    # 3. Apply Watermarking on event timestamp (Issue #5: Evicts late/expired state)
    watermarked_df = categorized_df.withWatermark("timestamp", watermark_delay)

    # 4. Compute 10-second windowed aggregations
    # Activity step 4 asks for four figures: Indoor/Outdoor temperature & humidity.
    # Zone-level grouping produces exactly those; --by-room adds per-room breakdown.
    group_keys = [
        window(col("timestamp"), window_duration, slide_duration),
        col("spatial_zone"),
    ]
    if group_by_room:
        group_keys.append(col("room"))

    windowed_df = (
        watermarked_df.groupBy(*group_keys)
        .agg(
            spark_round(avg("temperature"), 2).alias("avg_temperature"),
            spark_round(avg("humidity"), 2).alias("avg_humidity"),
            spark_round(avg("aqi"), 1).alias("avg_aqi"),
            spark_round(spark_max("temperature"), 2).alias("max_temperature"),
            spark_round(spark_min("temperature"), 2).alias("min_temperature"),
            count("event_id").alias("event_count"),
        )
    )

    # 5. Add conditional status column for Anomaly/Alert classification (Issue #5)
    # ALERT when avg_temperature > 35.0 OR avg_humidity > 70.0, else OK
    metrics_with_status = windowed_df.withColumn(
        "status",
        when(
            (col("avg_temperature") > temp_alert_threshold)
            | (col("avg_humidity") > hum_alert_threshold),
            lit("ALERT"),
        ).otherwise(lit("OK")),
    )

    return metrics_with_status


def process_stream(
    landing_dir: str = DEFAULT_LANDING_DIR,
    checkpoint_dir: str = DEFAULT_CHECKPOINT_DIR,
    window_duration: str = "10 seconds",
    slide_duration: str = "10 seconds",
    watermark_delay: str = "30 seconds",
    trigger_interval: str = "5 seconds",
    output_mode: str = "update",
    temp_alert_threshold: float = DEFAULT_TEMP_ALERT_THRESHOLD,
    hum_alert_threshold: float = DEFAULT_HUM_ALERT_THRESHOLD,
    group_by_room: bool = False,
):
    """
    Initializes and runs the structured streaming pipeline from landing directory to console sink.
    """
    os.makedirs(landing_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)

    spark = create_spark_session()
    schema = get_telemetry_schema()

    print("=" * 75)
    print(" CPE371 Big Data IoT - PySpark Structured Streaming & Alert Engine")
    print("=" * 75)
    print(f"[*] Landing Directory      : {landing_dir}")
    print(f"[*] Checkpoint Directory   : {checkpoint_dir}")
    print(f"[*] Window Duration        : {window_duration}")
    print(f"[*] Slide Duration         : {slide_duration}")
    print(f"[*] Watermark Delay        : {watermark_delay}")
    print(f"[*] Trigger Interval       : {trigger_interval}")
    print(f"[*] Output Mode            : {output_mode}")
    print(f"[*] Alert Thresholds       : Temp > {temp_alert_threshold}°C | Hum > {hum_alert_threshold}%")
    print(f"[*] Grouping               : {'window + zone + room' if group_by_room else 'window + zone'}")
    print("=" * 75)

    # Ingest continuous JSON micro-batches using spark.readStream
    # mqtt_bridge.py writes one pretty-printed (indented) JSON object per file.
    # Without multiLine=true Spark treats each physical line as a record and every
    # field parses as NULL. Verified: 238 all-NULL rows vs 14 correct rows.
    raw_stream = (
        spark.readStream.schema(schema)
        .option("multiLine", "true")
        .option("maxFilesPerTrigger", 10)
        .json(landing_dir)
    )

    # Apply spatial categorization, 10s windowed aggregations, watermarking & alert logic
    aggregated_metrics = compute_windowed_aggregations(
        raw_stream,
        window_duration=window_duration,
        slide_duration=slide_duration,
        watermark_delay=watermark_delay,
        temp_alert_threshold=temp_alert_threshold,
        hum_alert_threshold=hum_alert_threshold,
        group_by_room=group_by_room,
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
        description="PySpark Structured Streaming Analytics & Alert Engine for IoT Telemetry"
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
        "--watermark",
        default="30 seconds",
        help="Watermark delay for state eviction (default: '30 seconds')",
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
    parser.add_argument(
        "--temp-alert",
        type=float,
        default=DEFAULT_TEMP_ALERT_THRESHOLD,
        help=f"Temperature threshold for ALERT status (default: {DEFAULT_TEMP_ALERT_THRESHOLD})",
    )
    parser.add_argument(
        "--hum-alert",
        type=float,
        default=DEFAULT_HUM_ALERT_THRESHOLD,
        help=f"Humidity threshold for ALERT status (default: {DEFAULT_HUM_ALERT_THRESHOLD})",
    )

    parser.add_argument(
        "--by-room",
        action="store_true",
        help="Additionally break windowed metrics down per room (default: zone-level only)",
    )

    args = parser.parse_args()

    process_stream(
        landing_dir=args.landing_dir,
        checkpoint_dir=args.checkpoint_dir,
        window_duration=args.window,
        slide_duration=args.slide,
        watermark_delay=args.watermark,
        trigger_interval=args.trigger,
        output_mode=args.output_mode,
        temp_alert_threshold=args.temp_alert,
        hum_alert_threshold=args.hum_alert,
        group_by_room=args.by_room,
    )


if __name__ == "__main__":
    main()