#!/usr/bin/env python3
"""Append a small Parquet sample into a local Bronze directory every N seconds.

The target directory is the one watched by Structured Streaming, e.g.:
    SIC_Graduation/work/out/bronze_local_full

This script first creates a *separate seed snapshot* in work/out/bronze_replay_seed.
It then copies one seed Parquet shard at a time into the watched Bronze directory,
creating NEW immutable files. It deliberately replays duplicate business readings;
that is useful for verifying that the streaming checkpoint tracks files and Silver
Delta MERGE prevents duplicate meter_id+ts / transformer_id+ts records.

Run with spark-submit from the project root. Stop with Ctrl+C.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, functions as F

REQUIRED_COLUMNS = {
    "topic", "partition", "offset", "kafka_timestamp",
    "kafka_timestamp_type", "message_key", "raw_value",
}


def find_project_root() -> Path:
    starts = [Path(__file__).resolve().parent, Path.cwd().resolve()]
    for start in starts:
        for candidate in [start, *start.parents]:
            if (candidate / "generator.py").exists():
                return candidate
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent if script_dir.name == "work" else script_dir


def parquet_files(directory: Path) -> list[Path]:
    """Return visible Parquet data files, excluding Spark/Hadoop marker files."""
    files = [
        path for path in directory.rglob("*.parquet")
        if path.is_file()
        # The user's current files appear to have a leading dot before `part-`.
        # Include those when creating the replay seed; Spark may filter hidden
        # files when reading a directory, so read selected source files explicitly.
        and not path.name.startswith("_")
        and "_temporary" not in path.parts
    ]
    return sorted(files)


def parse_args() -> argparse.Namespace:
    root = find_project_root()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bronze-dir", default=str(root / "work" / "out" / "bronze_local_full"),
        help="Existing local Bronze dataset that the streaming job watches",
    )
    parser.add_argument(
        "--seed-dir", default=str(root / "work" / "out" / "bronze_replay_seed"),
        help="Separate directory containing the small sample used for replay",
    )
    parser.add_argument("--interval-seconds", type=int, default=120,
                        help="Seconds between new Parquet files (default: 120)")
    parser.add_argument("--sample-rows", type=int, default=20000,
                        help="Maximum source rows copied into the seed (default: 20000)")
    parser.add_argument("--seed-files", type=int, default=10,
                        help="Number of replay shards created from the sample")
    parser.add_argument("--seed-source-files", type=int, default=50,
                        help="Maximum existing Parquet files inspected when building the sample")
    parser.add_argument("--max-publishes", type=int, default=0,
                        help="Stop after this many publishes; 0 means run until Ctrl+C")
    parser.add_argument("--publish-immediately", action="store_true",
                        help="Publish the first file immediately instead of waiting one interval")
    parser.add_argument("--rebuild-seed", action="store_true",
                        help="Recreate the seed from the current Bronze directory")
    parser.add_argument("--master", default="local[*]",
                        help="Spark master (default: local[*])")
    return parser.parse_args()


def validate_paths(bronze_dir: Path, seed_dir: Path) -> None:
    if not bronze_dir.is_dir():
        raise FileNotFoundError(f"Bronze directory does not exist: {bronze_dir}")
    if bronze_dir.resolve() == seed_dir.resolve() or bronze_dir.resolve() in seed_dir.resolve().parents:
        raise ValueError("Seed directory must be outside the watched Bronze directory.")
    if seed_dir.resolve() in bronze_dir.resolve().parents:
        raise ValueError("Seed directory must not be a parent of the watched Bronze directory.")
    if not parquet_files(bronze_dir):
        raise FileNotFoundError(
            f"No visible *.parquet data files found under {bronze_dir}. "
            "Check the exact filenames with find before running this helper."
        )


def ensure_seed(spark: SparkSession, bronze_dir: Path, seed_dir: Path, args: argparse.Namespace) -> list[Path]:
    existing_seed_files = parquet_files(seed_dir) if seed_dir.exists() else []
    if args.rebuild_seed and seed_dir.exists():
        import shutil
        shutil.rmtree(seed_dir)
        existing_seed_files = []

    if not existing_seed_files:
        print(f"Creating replay seed from a sample of {bronze_dir}")
        # Use explicit source file paths instead of the directory path. This also
        # allows the seed builder to inspect `.part-*.parquet` files if present.
        # Only a bounded number of files are scanned to keep the helper quick.
        source_files = parquet_files(bronze_dir)[:args.seed_source_files]
        if not source_files:
            raise FileNotFoundError(f"No source Parquet files found under {bronze_dir}")
        source = spark.read.parquet(*[str(path) for path in source_files])
        missing = REQUIRED_COLUMNS - set(source.columns)
        if missing:
            raise ValueError(
                "Bronze schema does not match the expected Kafka-to-Parquet format. "
                f"Missing columns: {sorted(missing)}; found: {source.columns}"
            )

        meter_sample = (
            source.filter(F.col("topic") == "meter.readings")
            .limit(max(1, args.sample_rows // 2))
        )
        transformer_sample = (
            source.filter(F.col("topic") == "transformer.readings")
            .limit(max(1, args.sample_rows - args.sample_rows // 2))
        )
        sample = meter_sample.unionByName(transformer_sample)
        (sample.repartition(max(1, args.seed_files))
               .write.mode("overwrite").parquet(str(seed_dir)))
        print(f"Seed written to: {seed_dir}")

    seed_files = parquet_files(seed_dir)
    if not seed_files:
        raise RuntimeError(f"Seed directory has no Parquet shards: {seed_dir}")
    print(f"Replay seed has {len(seed_files)} Parquet shard(s).")
    return seed_files


def main() -> int:
    args = parse_args()
    if args.interval_seconds < 1:
        raise ValueError("--interval-seconds must be at least 1")
    if args.sample_rows < 2:
        raise ValueError("--sample-rows must be at least 2")
    if args.seed_files < 1:
        raise ValueError("--seed-files must be at least 1")
    if args.seed_source_files < 1:
        raise ValueError("--seed-source-files must be at least 1")
    if args.max_publishes < 0:
        raise ValueError("--max-publishes cannot be negative")

    bronze_dir = Path(args.bronze_dir).expanduser().resolve()
    seed_dir = Path(args.seed_dir).expanduser().resolve()
    validate_paths(bronze_dir, seed_dir)

    spark = (
        SparkSession.builder.appName("local_bronze_file_replayer")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        seed_files = ensure_seed(spark, bronze_dir, seed_dir, args)
        print("\n=== Local Bronze file replayer ===")
        print(f"Watched Bronze directory: {bronze_dir}")
        print(f"Replay seed directory:    {seed_dir}")
        print(f"Interval:                 {args.interval_seconds} seconds")
        print(f"Max publishes:            {args.max_publishes or 'unlimited'}")
        print("Each publish creates a new Parquet file; existing files are not modified.")
        print("The replayed records intentionally repeat business keys already in Bronze.")
        print("Start the streaming consumer in another terminal; stop this process with Ctrl+C.\n")

        publish_count = 0
        shard_index = 0
        if not args.publish_immediately:
            print(f"Waiting {args.interval_seconds}s before first publish so the stream can start...")
            time.sleep(args.interval_seconds)

        while args.max_publishes == 0 or publish_count < args.max_publishes:
            seed_file = seed_files[shard_index % len(seed_files)]
            shard = spark.read.parquet(str(seed_file)).coalesce(1)
            row_count = shard.count()
            if row_count == 0:
                print(f"Skipping empty seed shard: {seed_file.name}")
            else:
                # Append creates a new uniquely named part file under bronze_dir.
                # The stream consumes the new file; it never reads from the seed path.
                shard.write.mode("append").parquet(str(bronze_dir))
                publish_count += 1
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"publish #{publish_count}: {row_count:,} rows copied from "
                    f"{seed_file.name} into {bronze_dir}"
                )
                shard_index += 1

            if args.max_publishes and publish_count >= args.max_publishes:
                break
            print(f"Next publish in {args.interval_seconds}s. Press Ctrl+C to stop.")
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        print("\nPublisher stopped by user.")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
