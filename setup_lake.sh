#!/usr/bin/env bash
# Builds ./lake as a local copy of the bucket layout and seeds it from the generator output.
set -euo pipefail
mkdir -p lake/{bronze,silver,dlq,grid,export,truth}
cp output/grid/*.csv lake/grid/
cp output/sim_truth.jsonl lake/truth/
find lake -maxdepth 2 | sort