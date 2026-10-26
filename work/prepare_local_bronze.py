#!/usr/bin/env python3
"""Prepare generated JSONL readings as local Bronze-shaped Parquet for a streaming smoke test.

Expected input examples under --input-dir (search is recursive):
  meter_readings_YYYYMMDD.jsonl
  transformer_readings_YYYYMMDD.jsonl
  meter_readings.jsonl
  transformer_readings.jsonl

This adapter does NOT contact Kafka or S3. It wraps each raw JSON line in the
same columns produced by the project's Kafka-to-Bronze Parquet sink.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from pyspark.sql import SparkSession, DataFrame, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", default="output", help="Root containing generated JSONL files")
    parser.add_argument(
        "--output-dir", default="work/out/bronze_local_test",
        help="Local output directory for Bronze-shaped Parquet files",
    )
    parser.add_argument(
    "--max-rows-per-topic", type=int, default=0,
    help="Maximum rows per topic; use 0 for all rows",
)
    return parser.parse_args()


def wrap_topic(spark: SparkSession, files: list[Path], topic: str, id_field: str, limit: int) -> DataFrame:
    if not files:
        raise FileNotFoundError(f"No JSONL files found for topic {topic}.")
    raw = spark.read.text([str(p.resolve()) for p in files]).withColumnRenamed("value", "raw_value")
    if limit > 0:
        raw = raw.limit(limit)
    return raw.select(
        F.lit(topic).cast("string").alias("topic"),
        F.lit(0).cast("int").alias("partition"),
        F.monotonically_increasing_id().cast("long").alias("offset"),
        F.current_timestamp().cast("timestamp").alias("kafka_timestamp"),
        F.lit(0).cast("int").alias("kafka_timestamp_type"),
        F.get_json_object(F.col("raw_value"), f"$.{id_field}").cast("string").alias("message_key"),
        F.col("raw_value").cast("string").alias("raw_value"),
    )


def main() -> int:
    args = parse_args()
    if args.max_rows_per_topic < 0:
        print("--max-rows-per-topic must be >= 0 (0 means all rows).", file=sys.stderr)
        return 2

    project_root = Path.cwd().resolve()
    input_dir = Path(args.input_dir).expanduser()
    if not input_dir.is_absolute():
        input_dir = project_root / input_dir
    input_dir = input_dir.resolve()
    output_dir = Path(args.output_dir).expanduser()
    if not output_dir.is_absolute():
        output_dir = project_root / output_dir
    output_dir = output_dir.resolve()

    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    meter_files = sorted({
        path
        for pattern in ("meter_readings.jsonl", "meter_readings_*.jsonl")
        for path in input_dir.rglob(pattern)
    })
    transformer_files = sorted({
        path
        for pattern in ("transformer_readings.jsonl", "transformer_readings_*.jsonl")
        for path in input_dir.rglob(pattern)
    })
    if not meter_files or not transformer_files:
        print(f"Input root: {input_dir}")
        print(f"Meter JSONL files found: {len(meter_files)}")
        print(f"Transformer JSONL files found: {len(transformer_files)}")
        print("Expected both meter_readings_YYYYMMDD.jsonl and transformer_readings_YYYYMMDD.jsonl.")
        print("If output/ already contains Bronze Parquet, skip this adapter and pass that Parquet directory directly to the streaming job.")
        return 2

    print(f"Input root: {input_dir}")
    print(f"Meter source files: {len(meter_files)}")
    print(f"Transformer source files: {len(transformer_files)}")
    print(f"Output Bronze Parquet: {output_dir}")
    print(f"Rows per topic limit: {args.max_rows_per_topic or 'all'}")

    spark = (
        SparkSession.builder
        .appName("prepare_local_bronze_smoke_test")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        meter = wrap_topic(spark, meter_files, "meter.readings", "meter_id", args.max_rows_per_topic)
        transformer = wrap_topic(spark, transformer_files, "transformer.readings", "transformer_id", args.max_rows_per_topic)
        bronze = meter.unionByName(transformer)
        # This is a dedicated test source path; overwriting it does not change output/ originals.
        bronze.write.mode("overwrite").parquet(str(output_dir))

        check = spark.read.parquet(str(output_dir))
        check.groupBy("topic").count().orderBy("topic").show(truncate=False)
        print("Bronze-compatible schema:")
        check.printSchema()
        print("Sample raw messages:")
        check.select("topic", "raw_value", "partition", "offset", "kafka_timestamp").show(5, truncate=False)
        print("Local Bronze adapter completed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
