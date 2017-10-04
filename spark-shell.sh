#!/bin/bash

./bin/spark-shell --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/results.collection" --packages org.mongodb.spark:mongo-spark-connector_2.10:1.1.0