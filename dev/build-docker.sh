#!/usr/bin/env bash

SELF=$(cd $(dirname $0) && pwd)
cd $SELF

RELEASE=${RELEASE:-false}
MLSQL_VERSION=${MLSQL_VERSION:-1.6.0-SNAPSHOT}

if [[ "${RELEASE}" != "true" ]];then
   cd ..
   if [[ -z "${MLSQL_VERSION}" ]];then
    MLSQL_VERSION=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
   fi

   export MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION:-2.4}
   export SPARK_VERSION=${SPARK_VERSION:-2.4.5}
   export SCALA_VERSION=${SCALA_VERSION:-2.11}
   export MLSQL_DISTRIBUTIOIN_URL="streamingpro-mlsql-spark_${MLSQL_SPARK_VERSION}_2.11-${MLSQL_VERSION}.jar"
   export DISTRIBUTION=${MLSQL_SPARK_VERSION:-false}

   ./dev/package.sh
   cp streamingpro-mlsql/target/streamingpro-mlsql-spark_${MLSQL_SPARK_VERSION}_${SCALA_VERSION}-${MLSQL_VERSION}.jar ./dev/docker

   cd $SELF
   docker build --build-arg SCALA_VERSION=${SCALA_VERSION} --build-arg  MLSQL_VERSION=${MLSQL_VERSION} --build-arg SPARK_VERSION=${SPARK_VERSION} --build-arg MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION} -t mlsql:${MLSQL_SPARK_VERSION}-${MLSQL_VERSION} ./docker
else
   export MLSQL_VERSION=${MLSQL_VERSION:-1.1.7}
   export MLSQL_SPARK_VERSION=2.4
   export SPARK_VERSION=2.4.3
   docker build --build-arg  MLSQL_VERSION=${MLSQL_VERSION} --build-arg SPARK_VERSION=${SPARK_VERSION} --build-arg MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION} -t mlsql:${MLSQL_SPARK_VERSION}-${MLSQL_VERSION} ./docker

   export MLSQL_SPARK_VERSION=3.0
   export SPARK_VERSION=3.0.0-preview2
   docker build --build-arg  MLSQL_VERSION=${MLSQL_VERSION} --build-arg SPARK_VERSION=${SPARK_VERSION} --build-arg MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION} -t mlsql:${MLSQL_SPARK_VERSION}-${MLSQL_VERSION} ./docker
fi




