#!/usr/bin/env python3
"""Incremental Bronze -> Silver cleaner for the SIC Smart Grid project.

Reads immutable Parquet files written by the Kafka-to-Bronze sink, cleans each
micro-batch with the validation/transformation rules from silver(1).ipynb, and
terminates after all currently available files have been processed. Schedule this
script with Airflow (e.g. every 15 minutes).

Local mode is the default. Example:
  spark-submit bronze_to_silver_streaming.py --mode local \
    --bronze-path /path/to/SIC_Graduation/lake/bronze \
    --grid-path /path/to/SIC_Graduation/lake/grid

S3 mode (only after local tests pass):
  spark-submit bronze_to_silver_streaming.py --mode s3

Assumptions:
- Bronze is a Parquet file-source directory with topic/raw_value/Kafka metadata.
- New Bronze files are immutable and written as new filenames.
- Daily grid CSVs are named meters_YYYYMMDD.csv.
- The job uses Delta MERGE for business-key idempotency across micro-batches.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


STREAM_JOB_ID = "bronze_to_silver_v1"
IGNORE_GRID_DAYS = {"2026-08-03"}  # Same development-snapshot exclusion as the batch notebook.
MAX_FILES_PER_TRIGGER = 100

BRONZE_SCHEMA = StructType([
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("kafka_timestamp_type", IntegerType(), True),
    StructField("message_key", StringType(), True),
    StructField("raw_value", StringType(), True),
])

METER_COLUMNS = ["meter_id", "ts", "kw", "kvar", "voltage", "kwh"]
TRANSFORMER_COLUMNS = ["transformer_id", "ts", "kw", "voltage", "kwh"]
JSON_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}


def find_project_root() -> Path:
    """Find the folder containing generator.py, starting from this file/cwd."""
    starts = [Path(__file__).resolve().parent, Path.cwd().resolve()]
    for start in starts:
        for candidate in [start, *start.parents]:
            if (candidate / "generator.py").exists():
                return candidate
    # Expected layout is project_root/work/this_script.py.
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent if script_dir.name == "work" else script_dir


def has_parquet_files(path: Path) -> bool:
    if not path.is_dir():
        return False
    try:
        return next(path.glob("*.parquet"), None) is not None
    except OSError:
        return False


def has_meter_grid_files(path: Path) -> bool:
    return path.is_dir() and next(path.glob("meters_*.csv"), None) is not None


def resolve_local_path(override: str | None, candidates: Iterable[Path], label: str, checker) -> str:
    if override:
        path = Path(override).expanduser().resolve()
        if not checker(path):
            raise FileNotFoundError(
                f"{label} path was supplied but expected files were not found: {path}"
            )
        return str(path)
    for path in candidates:
        if checker(path):
            return str(path.resolve())
    choices = "\n  - ".join(str(p) for p in candidates)
    raise FileNotFoundError(
        f"Could not auto-find local {label}. Pass --{label.replace(' ', '-').lower()}-path. "
        f"Checked:\n  - {choices}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["local", "s3"], default="local")
    parser.add_argument("--bronze-path", default=None, help="Local Bronze folder override")
    parser.add_argument("--grid-path", default=None, help="Local grid CSV folder override")
    parser.add_argument("--output-root", default=None, help="Override output root")
    parser.add_argument("--checkpoint-path", default=None, help="Override streaming checkpoint path")
    parser.add_argument("--max-files-per-trigger", type=int, default=MAX_FILES_PER_TRIGGER)
    parser.add_argument(
    "--trigger-mode",
    choices=["available-now", "continuous"],
    default="available-now",
    help=(
        "available-now processes the current backlog then exits; "
        "continuous stays running and watches for new files"
    ),
)
    parser.add_argument(
    "--trigger-interval",
    default="30 seconds",
    help='Processing interval for continuous mode, e.g. "15 seconds" or "1 minute"',
)
    return parser.parse_args()


def build_paths(args: argparse.Namespace, project_root: Path) -> dict[str, str]:
    if args.max_files_per_trigger < 1:
        raise ValueError("--max-files-per-trigger must be >= 1")

    if args.mode == "local":
        bronze_candidates = [
            project_root / "lake" / "bronze",
            project_root / "output" / "bronze",
            project_root / "work" / "out" / "bronze",
            project_root / "work" / "bronze",
            project_root / "bronze",
        ]
        grid_candidates = [
            project_root / "lake" / "grid",
            project_root / "output" / "grid",
            project_root / "work" / "out" / "grid",
            project_root / "grid",
        ]
        bronze_path = resolve_local_path(args.bronze_path, bronze_candidates, "bronze", has_parquet_files)
        grid_path = resolve_local_path(args.grid_path, grid_candidates, "grid", has_meter_grid_files)
        local_root = Path(args.output_root).expanduser().resolve() if args.output_root else (
            project_root / "work" / "out" / "streaming" / STREAM_JOB_ID
        )
        checkpoint = Path(args.checkpoint_path).expanduser().resolve() if args.checkpoint_path else (
            project_root / "work" / "checkpoints" / STREAM_JOB_ID
        )
        return {
            "bronze": bronze_path,
            "grid": grid_path,
            "output_root": str(local_root),
            "checkpoint": str(checkpoint),
        }

    # S3 mode requires credentials in the environment/.env and an S3_BUCKET value.
    load_dotenv(project_root / ".env")
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise EnvironmentError("S3 mode requires S3_BUCKET in the environment or project .env file.")
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        raise EnvironmentError("S3 mode requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
    base = f"s3a://{bucket}".rstrip("/")
    return {
        "bronze": args.bronze_path or f"{base}/bronze",
        "grid": args.grid_path or f"{base}/grid",
        # Use the project's expected silver/, export/, and dlq/ prefixes.
        "output_root": args.output_root or base,
        "checkpoint": args.checkpoint_path or f"{base}/checkpoints/{STREAM_JOB_ID}",
    }


def create_spark(mode: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName("bronze_to_silver_incremental")
        .master("local[3]")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    builder = configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"],
    )
    if mode == "s3":
        # Use the same AWS settings as the original notebook; never needed in local mode.
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if spark.version != "3.5.3":
        print(f"WARNING: original notebook pinned Spark 3.5.3; detected Spark {spark.version}.", file=sys.stderr)
    return spark


def load_grid_meters(spark: SparkSession, grid_path: str) -> DataFrame:
    """Load daily meter-to-transformer snapshots, using the same date-based join as the batch job."""
    grid = (
        spark.read.option("header", True).option("encoding", "UTF-8")
        .option("pathGlobFilter", "meters_*.csv")
        .csv(grid_path)
        .withColumn("_source_file", F.input_file_name())
        .select(
            "meter_id",
            "transformer_id",
            F.to_date(
                F.regexp_extract("_source_file", r"meters_(\d{8})\.csv", 1),
                "yyyyMMdd",
            ).alias("grid_date"),
        )
        .filter(F.col("grid_date").isNotNull())
        .filter(~F.col("grid_date").cast("string").isin(*sorted(IGNORE_GRID_DAYS)))
    )
    duplicates = (
        grid.groupBy("meter_id", "grid_date").count()
        .filter(F.col("count") > 1).limit(1).count()
    )
    if duplicates:
        raise ValueError(
            "Grid contains duplicate (meter_id, grid_date) keys. Fix the daily grid files before streaming."
        )
    if grid.limit(1).count() == 0:
        raise ValueError(f"No usable meter grid rows found under {grid_path}")
    return F.broadcast(grid)


def has_rows(df: DataFrame) -> bool:
    return df.limit(1).count() > 0


def write_dlq_batch(df: DataFrame, output_root: str, dataset_name: str, batch_id: int) -> int:
    if not has_rows(df):
        return 0
    path = f"{output_root.rstrip('/')}/dlq/{dataset_name}/batch_id={batch_id:08d}"
    (df.write.mode("overwrite").parquet(path))
    return df.count()


def new_rows_not_in_silver(df: DataFrame, silver_path: str, keys: list[str]) -> DataFrame:
    """Identify first-seen business keys for the incremental Snowflake export."""
    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, silver_path):
        return df
    existing_keys = (
        spark.read.format("delta").load(silver_path)
        .select(*keys).dropDuplicates()
    )
    return df.join(existing_keys, on=keys, how="left_anti")


def merge_delta(df: DataFrame, silver_path: str, keys: list[str]) -> None:
    """Insert records that do not yet exist; repeated batches cannot duplicate business keys."""
    silver_df = df.drop("hour")
    if not has_rows(silver_df):
        return
    spark = silver_df.sparkSession
    if DeltaTable.isDeltaTable(spark, silver_path):
        target = DeltaTable.forPath(spark, silver_path)
        condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
        (
            target.alias("t")
            .merge(silver_df.alias("s"), condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            silver_df.write.format("delta").mode("overwrite")
            .partitionBy("date").save(silver_path)
        )


def export_new_batch(df: DataFrame, output_root: str, dataset_name: str, batch_id: int) -> int:
    """Write a deterministic per-microbatch folder; empty retries do not erase prior exports."""
    if not has_rows(df):
        return 0
    batch_path = f"{output_root.rstrip('/')}/export/{dataset_name}/batch_id={batch_id:08d}"
    (
        df.repartition("date", "hour")
        .write.mode("overwrite")
        .partitionBy("date", "hour")
        .parquet(batch_path)
    )
    return df.count()


def clean_meter_batch(batch_df: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Apply the original notebook's meter JSON parse, validation, dedup, and grid join."""
    raw = (
        batch_df.filter(F.col("topic") == "meter.readings")
        .withColumn("source", F.lit("kafka"))
    )

    meter_schema = StructType(
        [StructField(c, StringType(), True) for c in METER_COLUMNS]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    parsed = (
        raw.withColumn("p", F.from_json("raw_value", meter_schema, JSON_OPTIONS))
        .select("source", "partition", "offset", "kafka_timestamp", "raw_value", "p.*")
        .withColumn("ts_t", F.try_to_timestamp(F.col("ts")))
    )
    for column in ["kw", "kvar", "voltage", "kwh"]:
        parsed = parsed.withColumn(f"{column}_n", F.expr(f"try_cast(`{column}` as double)"))

    blank_id = F.col("meter_id").isNull() | (F.trim(F.col("meter_id")) == "")
    non_numeric = (
        F.col("kw_n").isNull() | F.col("kvar_n").isNull()
        | F.col("voltage_n").isNull() | F.col("kwh_n").isNull()
    )
    flagged = parsed.withColumn(
        "reject_reason",
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_json")
        .when(blank_id, "missing_meter_id")
        .when(~F.col("meter_id").rlike(r"^M-\d{6}$"), "bad_meter_id_format")
        .when(F.col("ts_t").isNull(), "missing_or_bad_ts")
        .when(non_numeric, "non_numeric_value")
        .when(F.col("kw_n") < 0, "negative_kw"),
    )
    dlq = flagged.filter(F.col("reject_reason").isNotNull()).select(
        "reject_reason", "source", "partition", "offset", "kafka_timestamp", "raw_value"
    )
    good = flagged.filter(F.col("reject_reason").isNull())

    # Same within-batch dedup key as the original: (meter_id, ts). Cross-batch
    # deduplication is handled by the Delta MERGE later.
    window = Window.partitionBy("meter_id", "ts_t").orderBy(
        "kafka_timestamp", "partition", "offset"
    )
    clean = (
        good.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1)
        .select(
            "meter_id", F.col("ts_t").alias("ts"),
            F.col("kw_n").alias("kw"), F.col("kvar_n").alias("kvar"),
            F.col("voltage_n").alias("voltage"), F.col("kwh_n").alias("kwh"),
        )
    )
    return clean, dlq


def clean_transformer_batch(batch_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Apply the original notebook's transformer JSON parse, validation, and dedup."""
    raw = (
        batch_df.filter(F.col("topic") == "transformer.readings")
        .withColumn("source", F.lit("kafka"))
    )
    transformer_schema = StructType(
        [StructField(c, StringType(), True) for c in TRANSFORMER_COLUMNS]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    parsed = (
        raw.withColumn("p", F.from_json("raw_value", transformer_schema, JSON_OPTIONS))
        .select("source", "partition", "offset", "kafka_timestamp", "raw_value", "p.*")
        .withColumn("ts_t", F.try_to_timestamp(F.col("ts")))
    )
    for column in ["kw", "voltage", "kwh"]:
        parsed = parsed.withColumn(f"{column}_n", F.expr(f"try_cast(`{column}` as double)"))

    blank_id = F.col("transformer_id").isNull() | (F.trim(F.col("transformer_id")) == "")
    non_numeric = F.col("kw_n").isNull() | F.col("voltage_n").isNull() | F.col("kwh_n").isNull()
    flagged = parsed.withColumn(
        "reject_reason",
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_json")
        .when(blank_id, "missing_transformer_id")
        .when(~F.col("transformer_id").rlike(r"^T-\d{4}$"), "bad_transformer_id_format")
        .when(F.col("ts_t").isNull(), "missing_or_bad_ts")
        .when(non_numeric, "non_numeric_value")
        .when(F.col("kw_n") < 0, "negative_kw"),
    )
    dlq = flagged.filter(F.col("reject_reason").isNotNull()).select(
        "reject_reason", "source", "partition", "offset", "kafka_timestamp", "raw_value"
    )
    good = flagged.filter(F.col("reject_reason").isNull())
    window = Window.partitionBy("transformer_id", "ts_t").orderBy(
        "kafka_timestamp", "partition", "offset"
    )
    clean = (
        good.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1)
        .select(
            "transformer_id", F.col("ts_t").alias("ts"),
            F.col("kw_n").alias("kw"), F.col("voltage_n").alias("voltage"),
            F.col("kwh_n").alias("kwh"),
        )
    )
    clean = clean.withColumn("date", F.to_date("ts")).withColumn("hour", F.hour("ts"))
    return clean, dlq


def process_batch_factory(spark: SparkSession, paths: dict[str, str], grid_meters: DataFrame):
    root = paths["output_root"].rstrip("/")
    meter_silver_path = f"{root}/silver/meter_readings"
    transformer_silver_path = f"{root}/silver/transformer_readings"

    def process_microbatch(batch_df: DataFrame, batch_id: int) -> None:
        if not has_rows(batch_df):
            print(f"Batch {batch_id}: no source rows.")
            return

        print(f"\n=== Processing micro-batch {batch_id} ===")
        # Transformations are ordinary DataFrame operations inside foreachBatch.
        meter_clean, meter_dlq = clean_meter_batch(batch_df)
        transformer_clean, transformer_dlq = clean_transformer_batch(batch_df)

        # Daily grid mapping is part of the original transformation: meter reading
        # event date must match the grid snapshot date.
        meter_joined = (
            meter_clean.withColumn("grid_date", F.to_date("ts"))
            .join(grid_meters, ["meter_id", "grid_date"], "left")
        )
        unmatched_grid = meter_joined.filter(F.col("transformer_id").isNull()).count()
        meter_final = (
            meter_joined.filter(F.col("transformer_id").isNotNull())
            .select("meter_id", "transformer_id", "ts", "kw", "kvar", "voltage", "kwh")
            .withColumn("date", F.to_date("ts"))
            .withColumn("hour", F.hour("ts"))
        ).persist(StorageLevel.MEMORY_AND_DISK)
        transformer_final = transformer_clean.persist(StorageLevel.MEMORY_AND_DISK)
        meter_dlq = meter_dlq.persist(StorageLevel.MEMORY_AND_DISK)
        transformer_dlq = transformer_dlq.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            meter_bad_count = write_dlq_batch(meter_dlq, root, "meter_readings", batch_id)
            transformer_bad_count = write_dlq_batch(transformer_dlq, root, "transformer_readings", batch_id)

            # Export only keys not yet in Silver. The export is written before the
            # Delta merge so a failed batch can safely retry and recreate its export.
            # The same checkpoint and batch ID make the output folder deterministic.
            new_meter = new_rows_not_in_silver(meter_final, meter_silver_path, ["meter_id", "ts"]).persist(StorageLevel.MEMORY_AND_DISK)
            new_transformer = new_rows_not_in_silver(
                transformer_final, transformer_silver_path, ["transformer_id", "ts"]
            ).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                meter_new_count = export_new_batch(new_meter, root, "meter_readings", batch_id)
                transformer_new_count = export_new_batch(new_transformer, root, "transformer_readings", batch_id)
                merge_delta(meter_final, meter_silver_path, ["meter_id", "ts"])
                merge_delta(transformer_final, transformer_silver_path, ["transformer_id", "ts"])
            finally:
                new_meter.unpersist()
                new_transformer.unpersist()

            print(f"Batch {batch_id}: meter valid+grid matched={meter_final.count():,}; new export={meter_new_count:,}; DLQ={meter_bad_count:,}")
            print(f"Batch {batch_id}: transformer valid={transformer_final.count():,}; new export={transformer_new_count:,}; DLQ={transformer_bad_count:,}")
            print(f"Batch {batch_id}: meter readings without matching grid snapshot={unmatched_grid:,}")
        finally:
            meter_final.unpersist()
            transformer_final.unpersist()
            meter_dlq.unpersist()
            transformer_dlq.unpersist()

    return process_microbatch


def main() -> int:
    args = parse_args()
    project_root = find_project_root()
    paths = build_paths(args, project_root)

    print("Run mode:", args.mode)
    print("Project root:", project_root)
    print("Bronze source:", paths["bronze"])
    print("Grid source:", paths["grid"])
    print("Output root:", paths["output_root"])
    print("Checkpoint:", paths["checkpoint"])
    print("Max files per micro-batch:", args.max_files_per_trigger)
    print("Ignored grid days:", sorted(IGNORE_GRID_DAYS))

    spark = create_spark(args.mode)
    try:
        bronze_stream = (
            spark.readStream.format("parquet")
            .schema(BRONZE_SCHEMA)
            .option("maxFilesPerTrigger", args.max_files_per_trigger)
            .load(paths["bronze"])
        )

        grid_meters = load_grid_meters(spark, paths["grid"])
        process_microbatch = process_batch_factory(spark, paths, grid_meters)

        writer = (
            bronze_stream.writeStream
            .foreachBatch(process_microbatch)
            .option("checkpointLocation", paths["checkpoint"])
        )

        if args.trigger_mode == "available-now":
            print("Trigger mode: AvailableNow — process backlog and exit")

            query = (
                writer
                .trigger(availableNow=True)
                .start()
            )

        else:
            print(
                f"Trigger mode: continuous micro-batches "
                f"every {args.trigger_interval}"
            )

            query = (
                writer
                .trigger(processingTime=args.trigger_interval)
                .start()
            )

        query.awaitTermination()

        print("\nStreaming run completed.")
        for name, keys in [
            ("meter_readings", ["meter_id", "ts"]),
            ("transformer_readings", ["transformer_id", "ts"]),
        ]:
            table_path = f"{paths['output_root'].rstrip('/')}/silver/{name}"
            if DeltaTable.isDeltaTable(spark, table_path):
                final_df = spark.read.format("delta").load(table_path)
                total = final_df.count()
                duplicate_keys = (
                    final_df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
                )
                print(f"Silver {name}: {total:,} rows; duplicate business keys present={bool(duplicate_keys)}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
