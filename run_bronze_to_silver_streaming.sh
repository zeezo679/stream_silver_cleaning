#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"

read -r SPARK_VERSION DELTA_VERSION SCALA_VERSION < <(
  "$PYTHON_BIN" - <<'PY'
import importlib.metadata
import pyspark

print(
    pyspark.__version__,
    importlib.metadata.version("delta-spark"),
    "2.13" if pyspark.__version__.split(".", 1)[0] == "4" else "2.12",
)
PY
)

if [[ "$SPARK_VERSION" == 4.* ]]; then
  DELTA_ARTIFACT="delta-spark_${SPARK_VERSION%.*}_$SCALA_VERSION"
else
  DELTA_ARTIFACT="delta-spark_$SCALA_VERSION"
fi

export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$PYTHON_BIN}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYTHON_BIN}"

exec "$SPARK_SUBMIT_BIN" \
  --packages "io.delta:${DELTA_ARTIFACT}:${DELTA_VERSION},org.apache.hadoop:hadoop-aws:3.3.4" \
  "$PROJECT_ROOT/work/bronze_to_silver_streaming.py" \
  "$@"
