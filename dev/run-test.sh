#!/usr/bin/env bash
## example
## ./dev/run-test.sh

V=${1:-2.4}

OPTS="-Pscala-2.11 -Pspark-${V}.0 -Pstreamingpro-spark-${V}.0-adaptor"

if [ $V == "3.0" ]
then
   ./dev/change-scala-version.sh 2.12
   python ./dev/python/convert_pom.py
   OPTS="-Pscala-2.12 -Pspark-${V}.0 -Pstreamingpro-spark-${V}.0-adaptor"
else
  echo "Only accept 2.4|3.0"
  exit -1
fi

mvn clean install -DskipTests ${OPTS}
mvn test ${OPTS} -pl streamingpro-it