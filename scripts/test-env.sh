#!/usr/bin/env bash
# Source from the repo root: wires the local Homebrew Spark 4.1.2 into the project venv.
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/4.1.2/libexec
PY4J_ZIP="$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip)"
export PYTHONPATH="$SPARK_HOME/python/lib/pyspark.zip:$PY4J_ZIP:$(pwd)/src"
export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python"
export _ZO_DOCTOR=0
