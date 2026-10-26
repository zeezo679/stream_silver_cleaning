"""One place for every location. LAKE_ENV=local reads/writes /lake, LAKE_ENV=s3 uses the bucket."""
import os

ENV = os.getenv("LAKE_ENV", "local")
ROOT = "/lake" if ENV == "local" else f"s3a://{os.environ['S3_BUCKET']}"

BRONZE = f"{ROOT}/bronze"   # Delta, raw copy of the Kafka messages
SILVER = f"{ROOT}/silver"   # Delta, clean and deduplicated
DLQ    = f"{ROOT}/dlq"      # bad rows + reason
GRID   = f"{ROOT}/grid"     # daily CSVs: meters_YYYYMMDD.csv, transformers_YYYYMMDD.csv
EXPORT = f"{ROOT}/export"   # plain Parquet for Snowflake
TRUTH  = f"{ROOT}/truth"    # sim_truth answer key

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")