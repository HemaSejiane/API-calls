#!/bin/bash
set -e
set -x

export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export PYSPARK_PYTHON=python3.5

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 2G \
    --executor-memory 4G \
    pardot_read.py
