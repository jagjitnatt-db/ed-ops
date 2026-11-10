#!/usr/bin/env bash
# Source from the repo root: wires a local Spark (4.1+) and Java 17 into the project venv.
# Uses JAVA_HOME / SPARK_HOME if already set, otherwise the Homebrew installs (brew install openjdk@17 apache-spark).
if [[ -z "${JAVA_HOME:-}" ]] && command -v brew >/dev/null; then export JAVA_HOME="$(brew --prefix openjdk@17)"; fi
if [[ -z "${SPARK_HOME:-}" ]] && command -v brew >/dev/null; then export SPARK_HOME="$(brew --prefix apache-spark)/libexec"; fi
: "${JAVA_HOME:?set JAVA_HOME to a Java 17 install}" "${SPARK_HOME:?set SPARK_HOME to a Spark 4.1+ install}"
PY4J_ZIP="$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip)"
export PYTHONPATH="$SPARK_HOME/python/lib/pyspark.zip:$PY4J_ZIP:$(pwd)/src"
export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python"
export _ZO_DOCTOR=0
# Project convention is naive UTC; PySpark converts timestamps via the OS time zone, so pin it (Databricks drivers run in UTC).
export TZ=UTC
