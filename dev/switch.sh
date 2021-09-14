#!/usr/bin/env bash

v=${1:-3.0}

SCALA_VERSION=2.12
SPARK_BIG_VERSION=3.0
if [[ "$v" == "2.4" ]]
then
  SCALA_VERSION=2.11
  SPARK_BIG_VERSION=2.4
fi

./dev/change-scala-version.sh "${SCALA_VERSION}"
python ./dev/python/convert_pom.py "${SPARK_BIG_VERSION}"