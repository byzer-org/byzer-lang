#!/usr/bin/env bash

SELF=$(cd $(dirname $0) && pwd)
cd $SELF

cd ..

if [[ -z "${MLSQL_VERSION}" ]];then
MLSQL_VERSION=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
fi



export MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION:-2.3}
export SPARK_VERSION=${SPARK_VERSION:-2.3.2}
export MLSQL_DISTRIBUTIOIN_URL="streamingpro-mlsql-spark_${MLSQL_SPARK_VERSION}-${MLSQL_VERSION}.jar"
export DISTRIBUTION=${MLSQL_SPARK_VERSION:-false}

./dev/package.sh

cp streamingpro-mlsql/target/streamingpro-mlsql-spark_${MLSQL_SPARK_VERSION}-${MLSQL_VERSION}.jar ./dev/docker

cd $SELF
docker build -t mlsql:${MLSQL_SPARK_VERSION}-${MLSQL_VERSION} ./docker