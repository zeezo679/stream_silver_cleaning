#!/usr/bin/env python3
"""Inspect local Bronze Parquet and cleaned Silver/Export data for SIC Graduation.

Run from the project root, for example:
    spark-submit work/testing.py

If automatic discovery chooses the wrong folders, specify paths explicitly:
    spark-submit work/testing.py \
      --bronze-path output/bronze \
      --cleaned-root work/out/streaming/bronze_to_silver_v1

The script is read-only: it does not write or change any data.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType


METER_ID_FIELDS = ["meter_id", "ts", "kw", "kvar", "voltage", "kwh"]
TRANSFORMER_ID_FIELDS = ["transformer_id", "ts", "kw", "voltage", "kwh"]
BRONZE_REQUIRED_COLUMNS = {"topic", "raw_value"}


def find_project_root() -> Path:
    """Find SIC_Graduation by locating generator.py; otherwise use a sensible fallback."""
    starts = [Path(__file__).resolve().parent, Path.cwd().resolve()]
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / "generator.py").is_file():
                return candidate
    script_dir = Path(__file__).resolve().parent
    if script_dir.name.lower() == "work":
        return script_dir.parent
    return Path.cwd().resolve()


def has_parquet(path: Path) -> bool:
    if not path.is_dir():
        return False
    try:
        return next(path.rglob("*.parquet"), None) is not None
    except OSError:
        return False


def is_delta_path(path: Path) -> bool:
    return path.is_dir() and (path / "_delta_log").is_dir()


def is_parquet_or_delta(path: Path) -> bool:
    return is_delta_path(path) or has_parquet(path)


def make_spark() -> SparkSession:
    """Create a local Spark session that can read both Parquet and Delta tables."""
    spark = (
        SparkSession.builder
        .appName("SIC_Local_Data_Inspector")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.3.2",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_table(spark: SparkSession, path: Path) -> DataFrame:
    """Read a Delta table when _delta_log exists, otherwise read Parquet."""
    if is_delta_path(path):
        print(f"Read format: Delta  | {path}")
        return spark.read.format("delta").load(str(path))
    if has_parquet(path):
        print(f"Read format: Parquet | {path}")
        return spark.read.parquet(str(path))
    raise FileNotFoundError(f"No Delta table or Parquet files found at: {path}")


def looks_like_bronze(spark: SparkSession, path: Path) -> bool:
    """Bronze in this project is Parquet with topic and raw_value columns."""
    if not has_parquet(path):
        return False
    try:
        columns = set(spark.read.parquet(str(path)).columns)
        return BRONZE_REQUIRED_COLUMNS.issubset(columns)
    except Exception:
        return False


def resolve_bronze(spark: SparkSession, root: Path, supplied: Optional[str]) -> Path:
    if supplied:
        path = Path(supplied).expanduser()
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        if not looks_like_bronze(spark, path):
            raise ValueError(
                f"The supplied Bronze path does not look like this project's Kafka-Bronze Parquet: {path}\n"
                "Expected columns include 'topic' and 'raw_value'."
            )
        return path

    preferred = [
        root / "lake" / "bronze",
        root / "output" / "bronze",
        root / "work" / "out" / "bronze",
        root / "work" / "bronze",
        root / "bronze",
        root / "output",
    ]
    seen: set[Path] = set()
    for path in preferred:
        path = path.resolve()
        if path not in seen and looks_like_bronze(spark, path):
            return path
        seen.add(path)

    # As a fallback, locate folders with Parquet files, skipping metadata and outputs.
    skip_names = {
        ".git", ".venv", "venv", "__pycache__", "_spark_metadata", "_delta_log",
        "checkpoints", "silver", "export", "dlq", "target", "site-packages",
    }
    try:
        for path in root.rglob("*"):
            if not path.is_dir() or path.name in skip_names:
                continue
            if path.name.startswith("."):
                continue
            if path in seen:
                continue
            if looks_like_bronze(spark, path):
                return path.resolve()
    except OSError:
        pass
    raise FileNotFoundError(
        "Could not locate a local Bronze Parquet folder with 'topic' and 'raw_value' columns.\n"
        "Pass its exact location with --bronze-path. Common paths include output/bronze or lake/bronze."
    )


def table_candidates(root: Path, table_name: str, layer: str) -> list[Path]:
    """Return likely local table paths in priority order."""
    bases = [
        root / "work" / "out" / "streaming" / "bronze_to_silver_v1",
        root / "work" / "out" / "streaming" / "bronze_to_silver_local_test",
        root / "work" / "out" / "streaming" / "local_smoke_test",
        root / "work" / "out" / "streaming" / "bronze_to_silver_smoke_test",
        root / "work" / "out",
        root / "out",
        root / "output",
        root / "work" / "output",
    ]
    candidates = [base / layer / table_name for base in bases]

    # Also find custom stream/run IDs below work/out/streaming, without scanning data files.
    stream_parent = root / "work" / "out" / "streaming"
    if stream_parent.is_dir():
        try:
            for run_dir in sorted(stream_parent.iterdir()):
                if run_dir.is_dir():
                    candidates.append(run_dir / layer / table_name)
        except OSError:
            pass

    # Prioritize Delta tables, then non-empty Parquet directories.
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve())
        if key not in seen:
            unique.append(candidate.resolve())
            seen.add(key)
    return unique


def resolve_data_table(
    root: Path,
    supplied_path: Optional[str],
    table_name: str,
    layer: str,
) -> Optional[Path]:
    if supplied_path:
        path = Path(supplied_path).expanduser()
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        return path if is_parquet_or_delta(path) else None
    for candidate in table_candidates(root, table_name, layer):
        if is_parquet_or_delta(candidate):
            return candidate
    return None


def section(title: str) -> None:
    print("\n" + "=" * 92)
    print(title)
    print("=" * 92)


def inspect_df(
    spark: SparkSession,
    df: DataFrame,
    name: str,
    sample_rows: int,
    do_count: bool,
) -> None:
    section(name)
    df.printSchema()
    print(f"Columns: {df.columns}")
    df.show(sample_rows, truncate=100, vertical=False)
    # Temporary views allow SQL inspection while this script's Spark session is running.
    safe_view = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name.lower())
    df.createOrReplaceTempView(safe_view)
    print(f"Temporary Spark SQL view: {safe_view}")
    if do_count:
        print(f"Total rows: {df.count():,}")


def inspect_bronze(spark: SparkSession, bronze_path: Path, sample_rows: int, do_count: bool) -> None:
    bronze = spark.read.parquet(str(bronze_path))
    inspect_df(spark, bronze, f"BRONZE_RAW ({bronze_path})", sample_rows, do_count)

    # Parse the embedded JSON into readable fields while retaining the Kafka metadata.
    section("BRONZE METER READINGS — JSON PARSED FOR DISPLAY")
    meter_schema = StructType([StructField(c, StringType(), True) for c in METER_ID_FIELDS])
    meters = (
        bronze.filter(F.col("topic") == "meter.readings")
        .withColumn("parsed", F.from_json("raw_value", meter_schema))
        .select("topic", "partition", "offset", "kafka_timestamp", "parsed.*")
    )
    meters.printSchema()
    meters.show(sample_rows, truncate=80, vertical=False)
    meters.createOrReplaceTempView("bronze_meter_readings")
    if do_count:
        print(f"Bronze meter rows: {meters.count():,}")

    section("BRONZE TRANSFORMER READINGS — JSON PARSED FOR DISPLAY")
    transformer_schema = StructType(
        [StructField(c, StringType(), True) for c in TRANSFORMER_ID_FIELDS]
    )
    transformers = (
        bronze.filter(F.col("topic") == "transformer.readings")
        .withColumn("parsed", F.from_json("raw_value", transformer_schema))
        .select("topic", "partition", "offset", "kafka_timestamp", "parsed.*")
    )
    transformers.printSchema()
    transformers.show(sample_rows, truncate=80, vertical=False)
    transformers.createOrReplaceTempView("bronze_transformer_readings")
    if do_count:
        print(f"Bronze transformer rows: {transformers.count():,}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-path", default=None, help="Exact local Bronze Parquet directory")
    parser.add_argument(
        "--cleaned-root", default=None,
        help="Root containing silver/ and/or export/ (e.g. work/out/streaming/bronze_to_silver_v1)",
    )
    parser.add_argument("--meter-silver", default=None, help="Exact meter Silver Delta or Parquet path")
    parser.add_argument("--transformer-silver", default=None, help="Exact transformer Silver Delta or Parquet path")
    parser.add_argument("--meter-export", default=None, help="Exact meter export Parquet path")
    parser.add_argument("--transformer-export", default=None, help="Exact transformer export Parquet path")
    parser.add_argument("--sample-rows", type=int, default=10, help="Rows shown per table (default: 10)")
    parser.add_argument("--count", action="store_true", help="Count all rows; can be slow for large data")
    args = parser.parse_args()
    if args.sample_rows < 1:
        parser.error("--sample-rows must be at least 1")

    root = find_project_root()
    print(f"Project root: {root}")
    print("Inspection mode: read-only; no source or output files are modified.")

    spark = make_spark()
    try:
        bronze_path = resolve_bronze(spark, root, args.bronze_path)
        print(f"Selected Bronze path: {bronze_path}")
        inspect_bronze(spark, bronze_path, args.sample_rows, args.count)

        # --cleaned-root can explicitly point to a run root containing silver/ and export/.
        cleaned_root = None
        if args.cleaned_root:
            cleaned_root = Path(args.cleaned_root).expanduser()
            if not cleaned_root.is_absolute():
                cleaned_root = (Path.cwd() / cleaned_root).resolve()

        def pick_cleaned(explicit: Optional[str], name: str, layer: str) -> Optional[Path]:
            if explicit:
                p = Path(explicit).expanduser()
                if not p.is_absolute():
                    p = (Path.cwd() / p).resolve()
                return p if is_parquet_or_delta(p) else None
            if cleaned_root is not None:
                candidate = cleaned_root / layer / name
                return candidate if is_parquet_or_delta(candidate) else None
            return resolve_data_table(root, None, name, layer)

        table_specs = [
            ("meter Silver", args.meter_silver, "meter_readings", "silver"),
            ("transformer Silver", args.transformer_silver, "transformer_readings", "silver"),
            ("meter Parquet export", args.meter_export, "meter_readings", "export"),
            ("transformer Parquet export", args.transformer_export, "transformer_readings", "export"),
        ]
        found = 0
        for label, explicit, table_name, layer in table_specs:
            path = pick_cleaned(explicit, table_name, layer)
            if path is None:
                print(f"\n[Not found] {label}. Use --cleaned-root or pass its exact path.")
                continue
            try:
                df = read_table(spark, path)
                inspect_df(spark, df, f"CLEANED {label.upper()} ({path})", args.sample_rows, args.count)
                found += 1
                if args.count and table_name in {"meter_readings", "transformer_readings"}:
                    # Show duplicate key count when the cleaned schema contains the expected keys.
                    keys = ["meter_id", "ts"] if table_name == "meter_readings" else ["transformer_id", "ts"]
                    if all(key in df.columns for key in keys):
                        duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
                        print(f"Duplicate business keys present: {bool(duplicates)}")
            except Exception as exc:
                print(f"[Could not inspect {label}] {path}: {exc}", file=sys.stderr)

        if found == 0:
            print(
                "\nNo cleaned Silver/Export datasets were found in the common local output paths.\n"
                "After you run the cleaner, provide --cleaned-root. For example:\n"
                "  spark-submit work/testing.py --cleaned-root work/out/streaming/bronze_to_silver_v1\n"
                "Or pass the individual --meter-silver / --transformer-silver paths."
            )
        print("\nDone. Temporary views were available during this Spark run; this script did not modify data.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
