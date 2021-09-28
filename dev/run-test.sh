#!/usr/bin/env bash
## example
## ./dev/run-test.sh

V=${1:-3.0}
SKIP_INSTALL=${2:-NO}
MATCHES=${3:-.*}


if [ $V == "3.0" ];then
   ./dev/change-scala-version.sh 2.12
   python ./dev/python/convert_pom.py 3.0
elif [ $V == "2.4" ]; then
    ./dev/change-scala-version.sh 2.11
   python ./dev/python/convert_pom.py 2.4
else
  echo "Only accept 2.4|3.0"
  exit -1
fi

mvn clean

if [ ${SKIP_INSTALL} != "skipInstall" ];then
  mvn clean install -DskipTests
fi


mvn test -pl streamingpro-it "-DargLine=-Dmatches=${MATCHES}"