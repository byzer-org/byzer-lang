#!/usr/bin/env bash

SELF=$(cd $(dirname $0) && pwd)
cd $SELF

RELEASE=${RELEASE:-false}
MLSQL_VERSION=${MLSQL_VERSION:-1.3.0-SNAPSHOT}

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
   export SPARK_VERSION=${SPARK_VERSION:-2.4.3}
   export MLSQL_DISTRIBUTIOIN_URL="streamingpro-mlsql-spark_${MLSQL_SPARK_VERSION}-${MLSQL_VERSION}.jar"
   export DISTRIBUTION=${MLSQL_SPARK_VERSION:-false}

   ./dev/package.sh
   cp streamingpro-mlsql/target/streamingpro-mlsql-spark_${MLSQL_SPARK_VERSION}-${MLSQL_VERSION}.jar ./dev/docker

   cd $SELF
   docker build --build-arg  MLSQL_VERSION=${MLSQL_VERSION} --build-arg SPARK_VERSION=${SPARK_VERSION} --build-arg MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION} -t mlsql:${MLSQL_SPARK_VERSION}-${MLSQL_VERSION} ./docker
else
   export MLSQL_VERSION=${MLSQL_VERSION:-1.1.7}
   export MLSQL_SPARK_VERSION=2.4
   export SPARK_VERSION=2.4.3
   docker build --build-arg  MLSQL_VERSION=${MLSQL_VERSION} --build-arg SPARK_VERSION=${SPARK_VERSION} --build-arg MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION} -t mlsql:${MLSQL_SPARK_VERSION}-${MLSQL_VERSION} ./docker

   export MLSQL_SPARK_VERSION=2.3
   export SPARK_VERSION=2.3.2
   docker build --build-arg  MLSQL_VERSION=${MLSQL_VERSION} --build-arg SPARK_VERSION=${SPARK_VERSION} --build-arg MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION} -t mlsql:${MLSQL_SPARK_VERSION}-${MLSQL_VERSION} ./docker

   export MLSQL_SPARK_VERSION=2.2
   export SPARK_VERSION=2.2.2
   docker build --build-arg  MLSQL_VERSION=${MLSQL_VERSION} --build-arg SPARK_VERSION=${SPARK_VERSION} --build-arg MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION} -t mlsql:${MLSQL_SPARK_VERSION}-${MLSQL_VERSION} ./docker
fi




